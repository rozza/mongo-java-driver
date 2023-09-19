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

package com.mongodb.internal.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.connection.QueryResult;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.Decoder;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.CommandReadTransformerAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.cursorDocumentToQueryResult;
import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand;
import static com.mongodb.internal.operation.SyncOperationHelper.CommandReadTransformer;
import static com.mongodb.internal.operation.SyncOperationHelper.executeRetryableRead;

class AggregateOperationImpl<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private static final String RESULT = "result";
    private static final String CURSOR = "cursor";
    private static final String FIRST_BATCH = "firstBatch";
    private static final List<String> FIELD_NAMES_WITH_RESULT = Arrays.asList(RESULT, FIRST_BATCH);

    private final TimeoutContext clientSideOperationTimeout;
    private final MongoNamespace namespace;
    private final List<BsonDocument> pipeline;
    private final Decoder<T> decoder;
    private final AggregateTarget aggregateTarget;
    private final PipelineCreator pipelineCreator;

    private boolean retryReads;
    private Boolean allowDiskUse;
    private Integer batchSize;
    private Collation collation;
    private BsonValue comment;
    private BsonValue hint;
    private BsonDocument variables;

    AggregateOperationImpl(final TimeoutContext clientSideOperationTimeout, final MongoNamespace namespace,
            final List<BsonDocument> pipeline, final Decoder<T> decoder, final AggregationLevel aggregationLevel) {
        this(clientSideOperationTimeout, namespace, pipeline, decoder,
                defaultAggregateTarget(notNull("aggregationLevel", aggregationLevel),
                        notNull("namespace", namespace).getCollectionName()),
                defaultPipelineCreator(pipeline));
    }

    AggregateOperationImpl(final TimeoutContext clientSideOperationTimeout, final MongoNamespace namespace,
            final List<BsonDocument> pipeline, final Decoder<T> decoder, final AggregateTarget aggregateTarget,
            final PipelineCreator pipelineCreator) {
        this.clientSideOperationTimeout = notNull("clientSideOperationTimeout", clientSideOperationTimeout);
        this.namespace = notNull("namespace", namespace);
        this.pipeline = notNull("pipeline", pipeline);
        this.decoder = notNull("decoder", decoder);
        this.aggregateTarget = notNull("aggregateTarget", aggregateTarget);
        this.pipelineCreator = notNull("pipelineCreator", pipelineCreator);
    }

    MongoNamespace getNamespace() {
        return namespace;
    }

    List<BsonDocument> getPipeline() {
        return pipeline;
    }

    Decoder<T> getDecoder() {
        return decoder;
    }

    Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    AggregateOperationImpl<T> allowDiskUse(@Nullable final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    Integer getBatchSize() {
        return batchSize;
    }

    AggregateOperationImpl<T> batchSize(@Nullable final Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    Collation getCollation() {
        return collation;
    }

    AggregateOperationImpl<T> collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Nullable
    BsonValue getComment() {
        return comment;
    }

    AggregateOperationImpl<T> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    AggregateOperationImpl<T> let(@Nullable final BsonDocument variables) {
        this.variables = variables;
        return this;
    }

    AggregateOperationImpl<T> retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    boolean getRetryReads() {
        return retryReads;
    }

    @Nullable
    BsonValue getHint() {
        return hint;
    }

    public TimeoutContext getClientSideOperationTimeout() {
        return clientSideOperationTimeout;
    }

    AggregateOperationImpl<T> hint(@Nullable final BsonValue hint) {
        isTrueArgument("BsonString or BsonDocument", hint == null || hint.isDocument() || hint.isString());
        this.hint = hint;
        return this;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return executeRetryableRead(clientSideOperationTimeout, binding, namespace.getDatabaseName(),
                getCommandCreator(binding.getSessionContext()), CommandResultDocumentCodec.create(decoder, FIELD_NAMES_WITH_RESULT),
                transformer(), retryReads);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        SingleResultCallback<AsyncBatchCursor<T>> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
       executeRetryableReadAsync(clientSideOperationTimeout, binding, namespace.getDatabaseName(),
               getCommandCreator(binding.getSessionContext()), CommandResultDocumentCodec.create(this.decoder, FIELD_NAMES_WITH_RESULT),
               asyncTransformer(), retryReads,
               errHandlingCallback);
    }

    private CommandCreator getCommandCreator(final SessionContext sessionContext) {
        return (clientSideOperationTimeout, serverDescription, connectionDescription) ->
                getCommand(clientSideOperationTimeout, sessionContext, connectionDescription.getMaxWireVersion());
    }

    BsonDocument getCommand(final TimeoutContext clientSideOperationTimeout, final SessionContext sessionContext,
            final int maxWireVersion) {
        BsonDocument commandDocument = new BsonDocument("aggregate", aggregateTarget.create());

        appendReadConcernToCommand(sessionContext, maxWireVersion, commandDocument);
        commandDocument.put("pipeline", pipelineCreator.create());
        long maxTimeMS = clientSideOperationTimeout.getMaxTimeMS();
        if (maxTimeMS > 0) {
            commandDocument.put("maxTimeMS", new BsonInt64(maxTimeMS));
        }
        BsonDocument cursor = new BsonDocument();
        if (batchSize != null) {
            cursor.put("batchSize", new BsonInt32(batchSize));
        }
        commandDocument.put(CURSOR, cursor);
        if (allowDiskUse != null) {
            commandDocument.put("allowDiskUse", BsonBoolean.valueOf(allowDiskUse));
        }
        if (collation != null) {
            commandDocument.put("collation", collation.asDocument());
        }
        if (comment != null) {
            commandDocument.put("comment", comment);
        }
        if (hint != null) {
            commandDocument.put("hint", hint);
        }
        if (variables != null) {
            commandDocument.put("let", variables);
        }

        return commandDocument;
    }

    private QueryResult<T> createQueryResult(final BsonDocument result, final ConnectionDescription description) {
        assertNotNull(result);
        return cursorDocumentToQueryResult(result.getDocument(CURSOR), description.getServerAddress());
    }

    private CommandReadTransformer<BsonDocument, QueryBatchCursor<T>> transformer() {
        return (result, source, connection) -> {
            QueryResult<T> queryResult = createQueryResult(result, connection.getDescription());
            long maxAwaitTimeMS = clientSideOperationTimeout.getMaxAwaitTimeMS();
            return new QueryBatchCursor<>(queryResult, 0, batchSize != null ? batchSize : 0, maxAwaitTimeMS, decoder, comment,
                    source, connection, result);
        };
    }

    private CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>> asyncTransformer() {
        return (result, source, connection) -> {
            QueryResult<T> queryResult = createQueryResult(result, connection.getDescription());
            long maxAwaitTimeMS = clientSideOperationTimeout.getMaxAwaitTimeMS();
            return new AsyncQueryBatchCursor<>(queryResult, 0, batchSize != null ? batchSize : 0, maxAwaitTimeMS, decoder,
                    comment, source, connection, result);
        };
    }

    interface AggregateTarget {
        BsonValue create();
    }

    interface PipelineCreator {
        BsonArray create();
    }

    private static AggregateTarget defaultAggregateTarget(final AggregationLevel aggregationLevel, final String collectionName) {
        return () -> {
            if (aggregationLevel == AggregationLevel.DATABASE) {
                return new BsonInt32(1);
            } else {
                return new BsonString(collectionName);
            }
        };
    }

    private static PipelineCreator defaultPipelineCreator(final List<BsonDocument> pipeline) {
        return () -> new BsonArray(pipeline);
    }
}
