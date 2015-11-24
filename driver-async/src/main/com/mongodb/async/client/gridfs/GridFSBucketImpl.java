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

package com.mongodb.async.client.gridfs;

import com.mongodb.MongoGridFSException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.client.gridfs.model.GridFSDownloadByNameOptions;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static java.lang.String.format;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@SuppressWarnings("deprecation")
final class GridFSBucketImpl implements GridFSBucket {
    private final MongoDatabase database;
    private final String bucketName;
    private final int chunkSizeBytes;
    private final WriteConcern writeConcern;
    private final ReadConcern readConcern;
    private final ReadPreference readPreference;
    private final MongoCollection<Document> filesCollection;
    private final MongoCollection<Document> chunksCollection;
    private final CodecRegistry codecRegistry;
    private volatile boolean checkedIndexes;
    private final Object checkedIndexLock = new Object();

    GridFSBucketImpl(final MongoDatabase database) {
        this(database, "fs");
    }

    GridFSBucketImpl(final MongoDatabase database, final String bucketName) {
        this.database = notNull("database", database);
        this.bucketName = notNull("bucketName", bucketName);
        this.chunkSizeBytes = 255 * 1024;
        this.writeConcern = database.getWriteConcern();
        this.readConcern = database.getReadConcern();
        this.readPreference = database.getReadPreference();
        this.codecRegistry = getCodecRegistry();
        this.filesCollection = getFilesCollection();
        this.chunksCollection = getChunksCollection();
    }

    GridFSBucketImpl(final MongoDatabase database, final String bucketName, final int chunkSizeBytes, final CodecRegistry codecRegistry,
                     final ReadPreference readPreference, final WriteConcern writeConcern, final ReadConcern readConcern,
                     final MongoCollection<Document> filesCollection, final MongoCollection<Document> chunksCollection,
                     final boolean checkedIndexes) {
        this.database = notNull("database", database);
        this.bucketName = notNull("bucketName", bucketName);
        this.chunkSizeBytes = chunkSizeBytes;
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.readConcern = notNull("readConcern", readConcern);
        this.checkedIndexes = checkedIndexes;
        this.filesCollection = notNull("filesCollection", filesCollection);
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
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    @Override
    public GridFSBucket withChunkSizeBytes(final int chunkSizeBytes) {
        return new GridFSBucketImpl(database, bucketName, chunkSizeBytes, codecRegistry, readPreference, writeConcern, readConcern,
                filesCollection, chunksCollection, checkedIndexes);
    }

    @Override
    public GridFSBucket withReadPreference(final ReadPreference readPreference) {
        return new GridFSBucketImpl(database, bucketName, chunkSizeBytes, codecRegistry, readPreference, writeConcern, readConcern,
                filesCollection, chunksCollection, checkedIndexes);
    }

    @Override
    public GridFSBucket withWriteConcern(final WriteConcern writeConcern) {
        return new GridFSBucketImpl(database, bucketName, chunkSizeBytes, codecRegistry, readPreference, writeConcern, readConcern,
                filesCollection, chunksCollection, checkedIndexes);
    }

    @Override
    public GridFSBucket withReadConcern(final ReadConcern readConcern) {
        return new GridFSBucketImpl(database, bucketName, chunkSizeBytes, codecRegistry, readPreference, writeConcern, readConcern,
                filesCollection, chunksCollection, checkedIndexes);
    }

    @Override
    public GridFSUploadStream openUploadStream(final String filename) {
        return openUploadStream(filename, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadStream openUploadStream(final String filename, final GridFSUploadOptions options) {
        int chunkSize = options.getChunkSizeBytes() == null ? chunkSizeBytes : options.getChunkSizeBytes();
        checkCreateIndex();
        return new GridFSUploadStreamImpl(filesCollection, chunksCollection, new ObjectId(), filename, chunkSize, options.getMetadata());
    }

    @Override
    public void uploadFromStream(final String filename, final AsyncInputStream source, final SingleResultCallback<ObjectId> callback) {
        uploadFromStream(filename, source, new GridFSUploadOptions(), callback);
    }

    @Override
    public void uploadFromStream(final String filename, final AsyncInputStream source, final GridFSUploadOptions options,
                                 final SingleResultCallback<ObjectId> callback) {
        int chunkSize = options.getChunkSizeBytes() == null ? chunkSizeBytes : options.getChunkSizeBytes();
        readAndWriteInputStream(source, openUploadStream(filename, options), ByteBuffer.allocate(chunkSize), callback);
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ObjectId id) {
        return new GridFSDownloadStreamImpl(find().filter(new Document("_id", id)), chunksCollection);
    }

    @Override
    public void downloadToStream(final ObjectId id, final AsyncOutputStream destination, final SingleResultCallback<Long> callback) {
        downloadToAsyncOutputStream(openDownloadStream(id), destination, callback);
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final BsonValue id) {
        return new GridFSDownloadStreamImpl(find().filter(new Document("_id", id)), chunksCollection);
    }

    @Override
    public void downloadToStream(final BsonValue id, final AsyncOutputStream destination, final SingleResultCallback<Long> callback) {
        downloadToAsyncOutputStream(openDownloadStream(id), destination, callback);
    }

    @Override
    public GridFSDownloadStream openDownloadStreamByName(final String filename) {
        return openDownloadStreamByName(filename, new GridFSDownloadByNameOptions());
    }

    @Override
    public GridFSDownloadStream openDownloadStreamByName(final String filename, final GridFSDownloadByNameOptions options) {
        return new GridFSDownloadStreamImpl(findFileByName(filename, options), chunksCollection);
    }

    @Override
    public void downloadToStreamByName(final String filename, final AsyncOutputStream destination,
                                       final SingleResultCallback<Long> callback) {
        downloadToStreamByName(filename, destination, new GridFSDownloadByNameOptions(), callback);
    }

    @Override
    public void downloadToStreamByName(final String filename, final AsyncOutputStream destination,
                                       final GridFSDownloadByNameOptions options, final SingleResultCallback<Long> callback) {
        downloadToAsyncOutputStream(openDownloadStreamByName(filename, options), destination, callback);
    }

    @Override
    public GridFSFindIterable find() {
        return new GridFSFindIterableImpl(filesCollection.find());
    }

    @Override
    public GridFSFindIterable find(final Bson filter) {
        return new GridFSFindIterableImpl(filesCollection.find(filter));
    }

    @Override
    public void delete(final ObjectId id, final SingleResultCallback<Void> callback) {
        filesCollection.deleteOne(new BsonDocument("_id", new BsonObjectId(id)), new SingleResultCallback<DeleteResult>() {
            @Override
            public void onResult(final DeleteResult filesResult, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    chunksCollection.deleteMany(new BsonDocument("files_id", new BsonObjectId(id)),
                            new SingleResultCallback<DeleteResult>() {
                                @Override
                                public void onResult(final DeleteResult chunksResult, final Throwable t) {
                                    if (filesResult.wasAcknowledged() && filesResult.getDeletedCount() == 0) {
                                        callback.onResult(null,
                                                new MongoGridFSException(format("No file found with the ObjectId: %s", id)));
                                    } else {
                                        callback.onResult(null, t);
                                    }
                                }
                            });
                }
            }
        });
    }

    @Override
    public void rename(final ObjectId id, final String newFilename, final SingleResultCallback<Void> callback) {
        filesCollection.updateOne(new Document("_id", id), new Document("$set", new Document("filename", newFilename)),
                new SingleResultCallback<UpdateResult>() {

                    @Override
                    public void onResult(final UpdateResult result, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
                            callback.onResult(null, new MongoGridFSException(format("No file found with the ObjectId: %s", id)));
                        } else {
                            callback.onResult(null, null);
                        }
                    }
                });
    }

    @Override
    public void drop(final SingleResultCallback<Void> callback) {
        filesCollection.drop(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    chunksCollection.drop(callback);
                }
            }
        });
    }

    private GridFSFindIterable findFileByName(final String filename, final GridFSDownloadByNameOptions options) {
        int revision = options.getRevision();
        int skip;
        int sort;
        if (revision >= 0) {
            skip = revision;
            sort = 1;
        } else {
            skip = (-revision) - 1;
            sort = -1;
        }

        return new GridFSFindIterableImpl(filesCollection.find(new Document("filename", filename)).skip(skip)
                .sort(new Document("uploadDate", sort)));
    }

    private CodecRegistry getCodecRegistry() {
        return fromRegistries(database.getCodecRegistry(), MongoClients.getDefaultCodecRegistry());
    }

    private MongoCollection<Document> getFilesCollection() {
        return database.getCollection(bucketName + ".files")
                .withCodecRegistry(codecRegistry)
                .withReadPreference(readPreference)
                .withWriteConcern(writeConcern);
    }

    private MongoCollection<Document> getChunksCollection() {
        return database.getCollection(bucketName + ".chunks")
                .withCodecRegistry(MongoClients.getDefaultCodecRegistry())
                .withReadPreference(readPreference)
                .withWriteConcern(writeConcern);
    }

    private void downloadToAsyncOutputStream(final GridFSDownloadStream downloadStream, final AsyncOutputStream destination,
                                             final SingleResultCallback<Long> callback) {
        final SingleResultCallback<Long> errorHandlingCallback = errorHandlingCallback(callback);
        downloadStream.getGridFSFile(new SingleResultCallback<GridFSFile>() {
            @Override
            public void onResult(final GridFSFile result, final Throwable t) {
                if (t != null) {
                    errorHandlingCallback.onResult(null, t);
                } else {
                    readAndWriteOutputStream(destination, downloadStream, ByteBuffer.allocate(result.getChunkSize()), 0,
                            errorHandlingCallback);
                }
            }
        });
    }

    private void readAndWriteInputStream(final AsyncInputStream source, final GridFSUploadStream uploadStream, final ByteBuffer buffer,
                                         final SingleResultCallback<ObjectId> callback) {
        buffer.clear();
        source.read(buffer, new SingleResultCallback<Integer>() {
            @Override
            public void onResult(final Integer result, final Throwable t) {
                if (t != null) {
                    if (t instanceof IOException) {
                        uploadStream.abort(new SingleResultCallback<Void>() {
                            @Override
                            public void onResult(final Void result, final Throwable abortException) {
                                if (abortException != null) {
                                    callback.onResult(null, abortException);
                                } else {
                                    callback.onResult(null, new MongoGridFSException("IOException when reading from the InputStream", t));
                                }
                            }
                        });
                    } else {
                        callback.onResult(null, t);
                    }
                } else if (result > 0) {
                    buffer.flip();
                    uploadStream.write(buffer, new SingleResultCallback<Integer>() {
                        @Override
                        public void onResult(final Integer result, final Throwable t) {
                            if (t != null) {
                                callback.onResult(null, t);
                            } else {
                                readAndWriteInputStream(source, uploadStream, buffer, callback);
                            }
                        }
                    });
                } else {
                    uploadStream.close(new SingleResultCallback<Void>() {
                        @Override
                        public void onResult(final Void result, final Throwable t) {
                            if (t != null) {
                                callback.onResult(null, t);
                            } else {
                                callback.onResult(uploadStream.getFileId(), null);
                            }
                        }
                    });
                }
            }
        });
    }

    private void readAndWriteOutputStream(final AsyncOutputStream destination, final GridFSDownloadStream downloadStream,
                                          final ByteBuffer buffer, final long amountRead, final SingleResultCallback<Long> callback) {
        buffer.clear();
        downloadStream.read(buffer, new SingleResultCallback<Integer>() {
            @Override
            public void onResult(final Integer result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else if (result > 0) {
                    buffer.flip();
                    destination.write(buffer, new SingleResultCallback<Integer>() {
                        @Override
                        public void onResult(final Integer result, final Throwable t) {
                            if (t != null) {
                                callback.onResult(null, t);
                            } else {
                                readAndWriteOutputStream(destination, downloadStream, buffer, amountRead + result, callback);
                            }
                        }
                    });
                } else {
                    callback.onResult(amountRead, null);
                }
            }
        });
    }

    private void checkCreateIndex() {
        boolean checkIndexes = false;
        synchronized (checkedIndexLock) {
            checkIndexes = !checkedIndexes;
            checkedIndexes = true;
        }
        if (checkIndexes) {
            new Runnable() {
                @Override
                public void run() {
                    filesCollection.withReadPreference(primary()).find().projection(new Document("_id", 1)).first(
                            new SingleResultCallback<Document>() {
                                @Override
                                public void onResult(final Document result, final Throwable t) {
                                    if (result == null & t == null) {
                                        checkFilesIndex();
                                        checkChunksIndex();
                                    }
                                }
                            });
                }
            }.run();
        }
    }

    private void hasIndex(final MongoCollection<Document> collection, final Document index, final SingleResultCallback<Boolean> callback) {
        collection.listIndexes().into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> indexes, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    boolean hasIndex = false;
                    for (Document indexDoc : indexes) {
                        if (indexDoc.get("key", Document.class).equals(index)) {
                            hasIndex = true;
                            break;
                        }
                    }
                    callback.onResult(hasIndex, null);
                }
            }
        });
    }

    private void checkFilesIndex() {
        final Document filesIndex = new Document("filename", 1).append("uploadDate", 1);
        hasIndex(filesCollection.withReadPreference(primary()), filesIndex, new SingleResultCallback<Boolean>() {
            @Override
            public void onResult(final Boolean result, final Throwable t) {
                if (!result && t == null) {
                    filesCollection.createIndex(filesIndex, NOOP_CALLBACK);
                }
            }
        });
    }

    private void checkChunksIndex() {
        final Document chunksIndex = new Document("files_id", 1).append("n", 1);
        hasIndex(chunksCollection.withReadPreference(primary()), chunksIndex, new SingleResultCallback<Boolean>() {
            @Override
            public void onResult(final Boolean result, final Throwable t) {
                if (!result && t == null) {
                    chunksCollection.createIndex(chunksIndex, new IndexOptions().unique(true), NOOP_CALLBACK);
                }
            }
        });
    }

    private static final SingleResultCallback<String> NOOP_CALLBACK = new SingleResultCallback<String>() {
        @Override
        public void onResult(final String result, final Throwable t) {
            // Noop
        }
    };

}
