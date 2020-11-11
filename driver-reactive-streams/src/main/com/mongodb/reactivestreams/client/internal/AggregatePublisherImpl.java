/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.client.OperationExecutor;
import com.mongodb.internal.async.client.WriteOperationThenCursorReadOperation;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.FindOptions;
import com.mongodb.internal.operation.AsyncOperations;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.internal.operation.AsyncWriteOperation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.sinkToCallback;

final class AggregatePublisherImpl<D, T> extends BatchCursorPublisherImpl<T> implements AggregatePublisher<T> {
    private final AsyncOperations<D> operations;
    private final MongoNamespace namespace;
    private final Class<D> documentClass;
    private final Class<T> resultClass;
    private final CodecRegistry codecRegistry;
    private final List<? extends Bson> pipeline;
    private final AggregationLevel aggregationLevel;
    private Boolean allowDiskUse;
    private long maxTimeMS;
    private long maxAwaitTimeMS;
    private Boolean bypassDocumentValidation;
    private Collation collation;
    private String comment;
    private Bson hint;

    AggregatePublisherImpl(@Nullable final ClientSession clientSession, final MongoNamespace namespace,
                           final Class<D> documentClass, final Class<T> resultClass, final CodecRegistry codecRegistry,
                           final ReadPreference readPreference, final ReadConcern readConcern, final WriteConcern writeConcern,
                           final OperationExecutor executor, final List<? extends Bson> pipeline,
                           final AggregationLevel aggregationLevel, final boolean retryReads) {
        super(clientSession, executor, readConcern, readPreference, retryReads);
        this.operations = new AsyncOperations<>(namespace, documentClass, readPreference, codecRegistry, readConcern, writeConcern,
                                                false, retryReads);
        this.namespace = notNull("namespace", namespace);
        this.documentClass = notNull("documentClass", documentClass);
        this.resultClass = notNull("resultClass", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.pipeline = notNull("pipeline", pipeline);
        this.aggregationLevel = notNull("aggregationLevel", aggregationLevel);
    }

    @Override
    public AggregatePublisher<T> allowDiskUse(@Nullable final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    @Override
    public AggregatePublisher<T> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public AggregatePublisher<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public AggregatePublisher<T> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxAwaitTimeMS = TimeUnit.MILLISECONDS.convert(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public AggregatePublisher<T> bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public AggregatePublisher<T> collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public AggregatePublisher<T> comment(@Nullable final String comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public AggregatePublisher<T> hint(@Nullable final Bson hint) {
        this.hint = hint;
        return this;
    }

    @Override
    public Publisher<Void> toCollection() {
        BsonDocument lastPipelineStage = getLastPipelineStage();
        if (lastPipelineStage == null || !lastPipelineStage.containsKey("$out") && !lastPipelineStage.containsKey("$merge")) {
            throw new IllegalStateException("The last stage of the aggregation pipeline must be $out or $merge");
        }

        return Mono.create(sink -> getExecutor().execute(operations.aggregateToCollection(pipeline, maxTimeMS, allowDiskUse,
                                                                                          bypassDocumentValidation, collation, hint,
                                                                                          comment, aggregationLevel), getReadConcern(),
                                                         getClientSession() != null ? getClientSession().getWrapped() : null,
                                                         sinkToCallback(sink)));
    }

    @Override
    AsyncReadOperation<AsyncBatchCursor<T>> asAsyncReadOperation() {
        MongoNamespace outNamespace = getOutNamespace();

        if (outNamespace != null) {
            AsyncWriteOperation<Void> aggregateToCollectionOperation =
                    operations.aggregateToCollection(pipeline, maxTimeMS, allowDiskUse, bypassDocumentValidation, collation, hint, comment,
                                                     aggregationLevel);

            FindOptions findOptions = new FindOptions().collation(collation);
            Integer batchSize = getBatchSize();
            if (batchSize != null) {
                findOptions.batchSize(batchSize);
            }
            AsyncReadOperation<AsyncBatchCursor<T>> findOperation =
                    operations.find(outNamespace, new BsonDocument(), resultClass, findOptions);

            return new WriteOperationThenCursorReadOperation<>(aggregateToCollectionOperation, findOperation);
        } else {
            return operations.aggregate(pipeline, resultClass, maxTimeMS, maxAwaitTimeMS, getBatchSize(), collation,
                                        hint, comment, allowDiskUse, aggregationLevel);
        }
    }

    @Nullable
    private BsonDocument getLastPipelineStage() {
        if (pipeline.isEmpty()) {
            return null;
        } else {
            Bson lastStage = notNull("last pipeline stage", pipeline.get(pipeline.size() - 1));
            return lastStage.toBsonDocument(documentClass, codecRegistry);
        }
    }

    @Nullable
    private MongoNamespace getOutNamespace() {
        BsonDocument lastPipelineStage = getLastPipelineStage();
        if (lastPipelineStage == null) {
            return null;
        }
        if (lastPipelineStage.containsKey("$out")) {
            if (lastPipelineStage.get("$out").isString()) {
                return new MongoNamespace(namespace.getDatabaseName(), lastPipelineStage.getString("$out").getValue());
            } else if (lastPipelineStage.get("$out").isDocument()) {
                BsonDocument outDocument = lastPipelineStage.getDocument("$out");
                if (!outDocument.containsKey("db") || !outDocument.containsKey("coll")) {
                    throw new IllegalStateException("Cannot return a cursor when the value for $out stage is not a namespace document");
                }
                return new MongoNamespace(outDocument.getString("db").getValue(), outDocument.getString("coll").getValue());
            } else {
                throw new IllegalStateException("Cannot return a cursor when the value for $out stage "
                                                        + "is not a string or namespace document");
            }
        } else if (lastPipelineStage.containsKey("$merge")) {
            BsonDocument mergeDocument = lastPipelineStage.getDocument("$merge");
            if (mergeDocument.isDocument("into")) {
                BsonDocument intoDocument = mergeDocument.getDocument("into");
                return new MongoNamespace(intoDocument.getString("db", new BsonString(namespace.getDatabaseName())).getValue(),
                                          intoDocument.getString("coll").getValue());
            } else if (mergeDocument.isString("into")) {
                return new MongoNamespace(namespace.getDatabaseName(), mergeDocument.getString("into").getValue());
            }
        }

        return null;
    }
}
