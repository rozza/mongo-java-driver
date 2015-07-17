/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

import com.mongodb.client.GridFSBucket;
import com.mongodb.client.GridFSDownloadStream;
import com.mongodb.client.GridFSFindIterable;
import com.mongodb.client.GridFSUploadStream;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.GridFSDownloadByNameOptions;
import com.mongodb.client.model.GridFSUploadOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

final class GridFSBucketImpl implements GridFSBucket {
    private final MongoDatabase database;
    private final String bucketName;
    private final int chunkSizeBytes;
    private final CodecRegistry codecRegistry;
    private final WriteConcern writeConcern;
    private final ReadPreference readPreference;
    private final MongoCollection<BsonDocument> filesCollection;
    private final MongoCollection<BsonDocument> chunksCollection;
    private boolean checkedIndexes;

    GridFSBucketImpl(final MongoDatabase database) {
        this(database, "fs");
    }

    GridFSBucketImpl(final MongoDatabase database, final String bucketName) {
        this.database = notNull("database", database);
        this.bucketName = notNull("bucketName", bucketName);
        this.chunkSizeBytes = 255;
        this.codecRegistry = database.getCodecRegistry();
        this.writeConcern = database.getWriteConcern();
        this.readPreference = database.getReadPreference();
        this.filesCollection = getFilesCollection(BsonDocument.class);
        this.chunksCollection = getChunksCollection(BsonDocument.class);
    }

    GridFSBucketImpl(final MongoDatabase database, final String bucketName, final int chunkSizeBytes, final CodecRegistry codecRegistry,
                     final ReadPreference readPreference, final WriteConcern writeConcern,
                     final MongoCollection<BsonDocument> filesCollection, final MongoCollection<BsonDocument> chunksCollection,
                     final boolean checkedIndexes) {
        this.database = notNull("database", database);
        this.bucketName = notNull("bucketName", bucketName);
        this.chunkSizeBytes = chunkSizeBytes;
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.checkedIndexes = checkedIndexes;
        this.filesCollection =  notNull("filesCollection", filesCollection);
        this.chunksCollection = notNull("chunksCollection", chunksCollection);
    }

    @Override
    public String getBucketName() {
        return bucketName;
    }

    @Override
    public int getChunkSizeBytes() {
        return chunkSizeBytes;
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public GridFSBucket withBucketName(final String bucketName) {
        return new GridFSBucketImpl(database, bucketName, chunkSizeBytes, codecRegistry, readPreference, writeConcern, filesCollection,
                chunksCollection, false);
    }

    @Override
    public GridFSBucket withChunkSizeBytes(final int chunkSizeBytes) {
        return new GridFSBucketImpl(database, bucketName, chunkSizeBytes, codecRegistry, readPreference, writeConcern, filesCollection,
                chunksCollection, checkedIndexes);
    }

    @Override
    public GridFSBucket withCodecRegistry(final CodecRegistry codecRegistry) {
        return new GridFSBucketImpl(database, bucketName, chunkSizeBytes, codecRegistry, readPreference, writeConcern, filesCollection,
                chunksCollection, checkedIndexes);
    }

    @Override
    public GridFSBucket withReadPreference(final ReadPreference readPreference) {
        return new GridFSBucketImpl(database, bucketName, chunkSizeBytes, codecRegistry, readPreference, writeConcern, filesCollection,
                chunksCollection, checkedIndexes);
    }

    @Override
    public GridFSBucket withWriteConcern(final WriteConcern writeConcern) {
        return new GridFSBucketImpl(database, bucketName, chunkSizeBytes, codecRegistry, readPreference, writeConcern, filesCollection,
                chunksCollection, checkedIndexes);
    }

    @Override
    public GridFSUploadStream openUploadStream(final String filename) {
        return openUploadStream(filename, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadStream openUploadStream(final String filename, final GridFSUploadOptions options) {
        int chunkSize = options.getChunkSizeBytes() == null ? chunkSizeBytes : options.getChunkSizeBytes();
        CodecRegistry codecRegistryToUse = options.getCodecRegistry() == null ? codecRegistry : options.getCodecRegistry();
        Bson metadata = options.getMetadata() == null ? new BsonDocument() : options.getMetadata();
        BsonDocument metadataBsonDocument = metadata.toBsonDocument(BsonDocument.class, codecRegistryToUse);
        checkCreateIndex();
        return new GridFSUploadStreamImpl(filesCollection, chunksCollection, new ObjectId(), filename, chunkSize, metadataBsonDocument);
    }

    @Override
    public ObjectId uploadFromStream(final String filename, final InputStream source) {
        return uploadFromStream(filename, source, new GridFSUploadOptions());
    }

    @Override
    public ObjectId uploadFromStream(final String filename, final InputStream source, final GridFSUploadOptions options) {
        GridFSUploadStream uploadStream = openUploadStream(filename, options);
        int chunkSize = options.getChunkSizeBytes() == null ? chunkSizeBytes : options.getChunkSizeBytes();
        byte[] buffer = new byte[chunkSize];
        int len;
        try {
            while ((len = source.read(buffer)) != -1) {
                uploadStream.write(buffer, 0, len);
            }
            uploadStream.close();
        } catch (IOException e) {
            throw new MongoGridFSException("IO Exception when reading from the InputStream", e);
        }
        return uploadStream.getFileId();
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ObjectId id) {
        BsonDocument fileInfo = filesCollection.find(new BsonDocument("_id", new BsonObjectId(id))).first();
        if (fileInfo == null) {
            throw new MongoGridFSException(format("No file found with the ObjectId: %s", id));
        }
        return new GridFSDownloadStreamImpl(fileInfo, chunksCollection);
    }

    @Override
    public void downloadToStream(final ObjectId id, final OutputStream destination) {
        downloadToStream(openDownloadStream(id), destination);
    }

    @Override
    public GridFSDownloadStream openDownloadStreamByName(final String filename) {
        return openDownloadStreamByName(filename, new GridFSDownloadByNameOptions());
    }

    @Override
    public GridFSDownloadStream openDownloadStreamByName(final String filename, final GridFSDownloadByNameOptions options) {
        return new GridFSDownloadStreamImpl(getFileByName(filename, options), chunksCollection);
    }

    @Override
    public void downloadToStreamByName(final String filename, final OutputStream destination) {
        downloadToStreamByName(filename, destination, new GridFSDownloadByNameOptions());
    }

    @Override
    public void downloadToStreamByName(final String filename, final OutputStream destination, final GridFSDownloadByNameOptions options) {
        downloadToStream(openDownloadStreamByName(filename, options), destination);
    }

    @Override
    public GridFSFindIterable<Document> find() {
        return new GridFSFindIterableImpl<Document>(getFilesCollection().find());
    }

    @Override
    public <TResult> GridFSFindIterable<TResult> find(final Class<TResult> resultClass) {
        return new GridFSFindIterableImpl<TResult>(getFilesCollection(resultClass).find());
    }

    @Override
    public GridFSFindIterable<Document> find(final Bson filter) {
        return find().filter(filter);
    }

    @Override
    public <TResult> GridFSFindIterable<TResult> find(final Bson filter, final Class<TResult> resultClass) {
        return find(resultClass).filter(filter);
    }

    @Override
    public void delete(final ObjectId id) {
        DeleteResult result = filesCollection.deleteOne(new BsonDocument("_id", new BsonObjectId(id)));
        chunksCollection.deleteMany(new BsonDocument("files_id", new BsonObjectId(id)));

        if (result.wasAcknowledged() && result.getDeletedCount() == 0) {
            throw new MongoGridFSException(format("No file found with the ObjectId: %s", id));
        }
    }

    @Override
    public void rename(final ObjectId id, final String newFilename) {
        UpdateResult updateResult = filesCollection.updateOne(new BsonDocument("_id", new BsonObjectId(id)),
                new BsonDocument("$set", new BsonDocument("filename", new BsonString(newFilename))));

        if (updateResult.wasAcknowledged() && updateResult.getMatchedCount() == 0) {
            throw new MongoGridFSException(format("No file found with the ObjectId: %s", id));
        }
    }

    private MongoCollection<Document> getFilesCollection() {
        return getFilesCollection(Document.class);
    }

    private <TResult> MongoCollection<TResult> getFilesCollection(final Class<TResult> clazz) {
        return database.getCollection(bucketName + ".files", clazz)
                .withCodecRegistry(codecRegistry)
                .withReadPreference(readPreference)
                .withWriteConcern(writeConcern);
    }

    private <TResult> MongoCollection<TResult> getChunksCollection(final Class<TResult> clazz) {
        return database.getCollection(bucketName + ".chunks", clazz)
                .withCodecRegistry(codecRegistry)
                .withReadPreference(readPreference)
                .withWriteConcern(writeConcern);
    }

    private void checkCreateIndex() {
        if (!checkedIndexes) {
            if (filesCollection.withReadPreference(primary()).find()
                    .projection(new BsonDocument("_id", new BsonInt32(1))).first() == null) {
                filesCollection.createIndex(Indexes.ascending("filename", "uploadDate"));
                chunksCollection.createIndex(Indexes.ascending("files_id", "n"), new IndexOptions().unique(true));
            }
            checkedIndexes = true;
        }
    }

    private BsonDocument getFileByName(final String filename, final GridFSDownloadByNameOptions options) {
        int revision = options.getRevision();
        int skip;
        BsonInt32 sort;
        if (revision >= 0) {
            skip = revision;
            sort = new BsonInt32(1);
        } else {
            skip = (-revision) - 1;
            sort = new BsonInt32(-1);
        }

        BsonDocument fileInfo = filesCollection.find(new BsonDocument("filename", new BsonString(filename)))
                .skip(skip).sort(new BsonDocument("uploadDate", sort)).first();
        if (fileInfo == null) {
            throw new MongoGridFSException(format("No file found with the filename: %s and revision: %s", filename, revision));
        }
        return fileInfo;
    }

    private void downloadToStream(final GridFSDownloadStream stream, final OutputStream destination) {
        byte[] buffer = new byte[stream.getFileInformation().getInt32("chunkSize").getValue()];
        int len;
        try {
            while ((len = stream.read(buffer)) != -1) {
                destination.write(buffer, 0, len);
            }
            stream.close();
        } catch (IOException e) {
            throw new MongoGridFSException("IO Exception when reading from the OutputStream", e);
        }
    }
}
