/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.async.client;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.FindOptions;
import com.mongodb.internal.operation.AsyncOperations;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.internal.operation.AsyncWriteOperation;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;


class AsyncAggregateIterableImpl<TDocument, TResult> extends AsyncMongoIterableImpl<TResult> implements AsyncAggregateIterable<TResult> {
    private final AsyncOperations<TDocument> operations;
    private final MongoNamespace namespace;
    private final Class<TDocument> documentClass;
    private final Class<TResult> resultClass;
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
    private BsonDocument lastPipelineStage;

    AsyncAggregateIterableImpl(@Nullable final AsyncClientSession clientSession, final String databaseName,
                               final Class<TDocument> documentClass, final Class<TResult> resultClass, final CodecRegistry codecRegistry,
                               final ReadPreference readPreference, final ReadConcern readConcern, final WriteConcern writeConcern,
                               final OperationExecutor executor, final List<? extends Bson> pipeline,
                               final AggregationLevel aggregationLevel, final boolean retryReads) {
        this(clientSession, new MongoNamespace(databaseName, "ignored"), documentClass, resultClass, codecRegistry, readPreference,
                readConcern, writeConcern, executor, pipeline, aggregationLevel, retryReads);
    }

    AsyncAggregateIterableImpl(@Nullable final AsyncClientSession clientSession, final MongoNamespace namespace,
                               final Class<TDocument> documentClass, final Class<TResult> resultClass, final CodecRegistry codecRegistry,
                               final ReadPreference readPreference, final ReadConcern readConcern, final WriteConcern writeConcern,
                               final OperationExecutor executor, final List<? extends Bson> pipeline,
                               final AggregationLevel aggregationLevel, final boolean retryReads) {
        super(clientSession, executor, readConcern, readPreference, retryReads);
        this.operations = new AsyncOperations<TDocument>(namespace, documentClass, readPreference, codecRegistry, readConcern, writeConcern,
                false, retryReads);
        this.namespace = notNull("namespace", namespace);
        this.documentClass = notNull("documentClass", documentClass);
        this.resultClass = notNull("resultClass", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.pipeline = notNull("pipeline", pipeline);
        this.aggregationLevel = notNull("aggregationLevel", aggregationLevel);
    }

    @Override
    public void toCollection(final SingleResultCallback<Void> callback) {
        if (!getLastPipelineStage().containsKey("$out") && !getLastPipelineStage().containsKey("$merge")) {
            throw new IllegalStateException("The last stage of the aggregation pipeline must be $out or $merge");
        }

        getExecutor().execute(operations.aggregateToCollection(pipeline, maxTimeMS, allowDiskUse, bypassDocumentValidation, collation, hint,
                comment, aggregationLevel), getReadConcern(), getClientSession(), callback);
    }

    @Override
    public AsyncAggregateIterable<TResult> allowDiskUse(@Nullable final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    @Override
    public AsyncAggregateIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public AsyncAggregateIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public AsyncAggregateIterable<TResult> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxAwaitTimeMS = TimeUnit.MILLISECONDS.convert(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public AsyncAggregateIterable<TResult> bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public AsyncAggregateIterable<TResult> collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public AsyncAggregateIterable<TResult> comment(@Nullable final String comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public AsyncAggregateIterable<TResult> hint(@Nullable final Bson hint) {
        this.hint = hint;
        return this;
    }

    @Override
    AsyncReadOperation<AsyncBatchCursor<TResult>> asAsyncReadOperation() {
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
            AsyncReadOperation<AsyncBatchCursor<TResult>> findOperation =
                    operations.find(outNamespace, new BsonDocument(), resultClass, findOptions);

            return new WriteOperationThenCursorReadOperation<TResult>(aggregateToCollectionOperation, findOperation);
        } else {
            return operations.aggregate(pipeline, resultClass, maxTimeMS, maxAwaitTimeMS, getBatchSize(), collation,
                    hint, comment, allowDiskUse, aggregationLevel);
        }

    }

    private BsonDocument getLastPipelineStage() {
        if (lastPipelineStage == null) {
            if (pipeline.isEmpty()) {
                lastPipelineStage = new BsonDocument();
            } else {
                Bson lastStage = notNull("last pipeline stage", pipeline.get(pipeline.size() - 1));
                lastPipelineStage = lastStage.toBsonDocument(documentClass, codecRegistry);
            }
        }
        return lastPipelineStage;
    }

    @Nullable
    private MongoNamespace getOutNamespace() {
        BsonDocument lastStageDocument = getLastPipelineStage();

        if (lastStageDocument.containsKey("$out")) {
            if (!lastStageDocument.get("$out").isString()) {
                throw new IllegalStateException("Cannot return a cursor when the value for $out stage is not a string");
            }
            return new MongoNamespace(namespace.getDatabaseName(), lastStageDocument.getString("$out").getValue());
        } else if (lastStageDocument.containsKey("$merge")) {
            BsonDocument mergeDocument = lastStageDocument.getDocument("$merge");
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
