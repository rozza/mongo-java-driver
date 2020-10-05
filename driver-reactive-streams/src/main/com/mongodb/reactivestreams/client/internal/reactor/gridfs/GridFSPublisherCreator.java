/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.reactivestreams.client.internal.reactor.gridfs;

import com.mongodb.MongoGridFSException;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.gridfs.GridFSFindPublisher;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

public class GridFSPublisherCreator {

    public static GridFSUploadPublisherImpl createGridFSUploadPublisher(
            final int chunkSizeBytes, final MongoCollection<GridFSFile> filesCollection, final MongoCollection<Document> chunksCollection,
            @Nullable final ClientSession clientSession, final BsonValue id, final String filename, final GridFSUploadOptions options,
            final Publisher<ByteBuffer> source) {
        notNull("filesCollection", filesCollection);
        notNull("chunksCollection", chunksCollection);
        notNull("id", id);
        notNull("filename", filename);
        notNull("options", options);
        int chunkSize = options.getChunkSizeBytes() == null ? chunkSizeBytes : options.getChunkSizeBytes();
        return new GridFSUploadPublisherImpl(clientSession, filesCollection, chunksCollection, id, filename, chunkSize,
                options.getMetadata(), source);
    }

    public static GridFSDownloadPublisherImpl createGridFSDownloadPublisher(
            final MongoCollection<Document> chunksCollection,
            @Nullable final ClientSession clientSession,
            final GridFSFindPublisher publisher) {
        notNull("chunksCollection", chunksCollection);
        notNull("publisher", publisher);
        return new GridFSDownloadPublisherImpl(clientSession, Mono.from(publisher), chunksCollection);
    }

    public static GridFSFindPublisher createGridFSFindPublisher(
            final MongoCollection<GridFSFile> filesCollection,
            @Nullable final ClientSession clientSession,
            @Nullable final Bson filter) {
        notNull("filesCollection", filesCollection);
        return new GridFSFindPublisherImpl(createFindPublisher(filesCollection, clientSession, filter));
    }

    public static GridFSFindPublisher createGridFSFindPublisher(
            final MongoCollection<GridFSFile> filesCollection,
            @Nullable final ClientSession clientSession,
            final String filename,
            final GridFSDownloadOptions options) {
        notNull("filesCollection", filesCollection);
        notNull("filename", filename);
        notNull("options", options);

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

        return createGridFSFindPublisher(filesCollection, clientSession, new Document("filename", filename)).skip(skip)
                .sort(new Document("uploadDate", sort));
    }

    public static FindPublisher<GridFSFile> createFindPublisher(
            final MongoCollection<GridFSFile> filesCollection,
            @Nullable final ClientSession clientSession,
            @Nullable final Bson filter) {
        notNull("filesCollection", filesCollection);
        FindPublisher<GridFSFile> publisher;
        if (clientSession == null)  {
            publisher = filesCollection.find();
        } else {
            publisher = filesCollection.find(clientSession);
        }

        if (filter != null) {
            publisher = publisher.filter(filter);
        }
        return publisher;
    }

    public static Publisher<Void> createDeletePublisher(final MongoCollection<GridFSFile> filesCollection,
                                                        final MongoCollection<Document> chunksCollection,
                                                        @Nullable final ClientSession clientSession,
                                                        final BsonValue id) {
        notNull("filesCollection", filesCollection);
        notNull("chunksCollection", chunksCollection);
        notNull("id", id);
        BsonDocument filter = new BsonDocument("_id", id);
        Publisher<DeleteResult> publisher;
        if (clientSession == null)  {
            publisher = filesCollection.deleteOne(filter);
        } else {
            publisher = filesCollection.deleteOne(clientSession, filter);
        }
        return Mono.from(publisher)
                .flatMap(deleteResult -> {
                    if (deleteResult.wasAcknowledged() && deleteResult.getDeletedCount() == 0) {
                        throw new MongoGridFSException(format("No file found with the ObjectId: %s", id));
                    }
                    return Mono.from(chunksCollection.deleteMany(new BsonDocument("files_id", id)));
                })
                .then();
    }

    public static Publisher<Void> createRenamePublisher(final MongoCollection<GridFSFile> filesCollection,
                                                        @Nullable final ClientSession clientSession,
                                                        final BsonValue id,
                                                        final String newFilename) {
        notNull("filesCollection", filesCollection);
        notNull("id", id);
        notNull("newFilename", newFilename);
        BsonDocument filter = new BsonDocument("_id", id);
        BsonDocument update = new BsonDocument("$set",
                new BsonDocument("filename", new BsonString(newFilename)));
        Publisher<UpdateResult> publisher;
        if (clientSession == null)  {
            publisher = filesCollection.updateOne(filter, update);
        } else {
            publisher = filesCollection.updateOne(clientSession, filter, update);
        }

        return Mono.from(publisher).map(updateResult -> {
            if (updateResult.wasAcknowledged() && updateResult.getModifiedCount() == 0) {
                throw new MongoGridFSException(format("No file found with the ObjectId: %s", id));
            }
            return null;
        });
    }

    public static Publisher<Void> createDropPublisher(final MongoCollection<GridFSFile> filesCollection,
                                                      final MongoCollection<Document> chunksCollection,
                                                      @Nullable final ClientSession clientSession) {
        Publisher<Void> filesDropPublisher;
        if (clientSession == null)  {
            filesDropPublisher = filesCollection.drop();
        } else {
            filesDropPublisher = filesCollection.drop(clientSession);
        }

        Publisher<Void> chunksDropPublisher;
        if (clientSession == null)  {
            chunksDropPublisher = chunksCollection.drop();
        } else {
            chunksDropPublisher = chunksCollection.drop(clientSession);
        }

        return Mono.from(filesDropPublisher).then(Mono.from(chunksDropPublisher));
    }

    private GridFSPublisherCreator() {
    }
}
