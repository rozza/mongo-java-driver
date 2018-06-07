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

package com.mongodb.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.changestream.ChangeStreamLevel;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.QueryResult;
import com.mongodb.session.SessionContext;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.Decoder;
import org.bson.codecs.RawBsonDocumentCodec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionFourDotZero;
import static com.mongodb.operation.CommandOperationHelper.CommandTransformer;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.CallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.cursorDocumentToQueryResult;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.validateReadConcernAndCollation;
import static com.mongodb.operation.OperationHelper.withConnection;
import static com.mongodb.operation.OperationReadConcernHelper.appendReadConcernToCommand;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * An operation that executes an {@code $changeStream} aggregation.
 *
 * @param <T> the operations result type.
 * @mongodb.driver.manual aggregation/ Aggregation
 * @mongodb.server.release 2.6
 * @since 3.6
 */
public class ChangeStreamOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private static final String RESULT = "result";
    private static final String CURSOR = "cursor";
    private static final String FIRST_BATCH = "firstBatch";
    private static final List<String> FIELD_NAMES_WITH_RESULT = Arrays.asList(RESULT, FIRST_BATCH);
    private static final RawBsonDocumentCodec RAW_BSON_DOCUMENT_CODEC = new RawBsonDocumentCodec();
    private final MongoNamespace namespace;
    private final FullDocument fullDocument;
    private final List<BsonDocument> pipeline;
    private final Decoder<T> decoder;
    private final ChangeStreamLevel changeStreamLevel;

    private BsonDocument resumeToken;
    private Integer batchSize;
    private Collation collation;
    private long maxAwaitTimeMS;
    private BsonTimestamp startAtOperationTime;

    /**
     * Construct a new instance.
     *
     * @param namespace    the database and collection namespace for the operation.
     * @param fullDocument the fullDocument value
     * @param pipeline     the aggregation pipeline.
     * @param decoder      the decoder for the result documents.
     */
    public ChangeStreamOperation(final MongoNamespace namespace, final FullDocument fullDocument, final List<BsonDocument> pipeline,
                                 final Decoder<T> decoder) {
        this(namespace, fullDocument, pipeline, decoder, ChangeStreamLevel.COLLECTION);
    }

    /**
     * Construct a new instance.
     *
     * @param namespace         the database and collection namespace for the operation.
     * @param fullDocument      the fullDocument value
     * @param pipeline          the aggregation pipeline.
     * @param decoder           the decoder for the result documents.
     * @param changeStreamLevel the level at which the change stream is observing
     *
     * @since 3.8
     */
    public ChangeStreamOperation(final MongoNamespace namespace, final FullDocument fullDocument, final List<BsonDocument> pipeline,
                                 final Decoder<T> decoder, final ChangeStreamLevel changeStreamLevel) {
        this.namespace = notNull("namespace", namespace);
        this.fullDocument = notNull("fullDocument", fullDocument);
        this.pipeline = notNull("pipeline", pipeline);
        this.decoder = notNull("decoder", decoder);
        this.changeStreamLevel = notNull("changeStreamLevel", changeStreamLevel);
    }

    /**
     * @return the namespace for this operation
     */
    public MongoNamespace getNamespace() {
        return namespace;
    }

    /**
     * @return the decoder for this operation
     */
    public Decoder<T> getDecoder() {
        return decoder;
    }

    /**
     * Returns the fullDocument value, in 3.6
     *
     * @return the fullDocument value
     */
    public FullDocument getFullDocument() {
        return fullDocument;
    }

    /**
     * Returns the logical starting point for the new change stream.
     *
     * <p>A null value represents the server default.</p>
     *
     * @return the resumeAfter
     */
    public BsonDocument getResumeToken() {
        return resumeToken;
    }

    /**
     * Sets the logical starting point for the new change stream.
     *
     * @param resumeToken the resumeToken
     * @return this
     */
    public ChangeStreamOperation<T> resumeAfter(final BsonDocument resumeToken) {
        this.resumeToken = resumeToken;
        return this;
    }

    /**
     * Gets the aggregation pipeline.
     *
     * @return the pipeline
     * @mongodb.driver.manual core/aggregation-introduction/#aggregation-pipelines Aggregation Pipeline
     */
    public List<BsonDocument> getPipeline() {
        return pipeline;
    }

    /**
     * Gets the number of documents to return per batch.  Default to 0, which indicates that the server chooses an appropriate batch size.
     *
     * @return the batch size, which may be null
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public Integer getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public ChangeStreamOperation<T> batchSize(final Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * The maximum amount of time for the server to wait on new documents to satisfy a tailable cursor
     * query. This only applies to a TAILABLE_AWAIT cursor. When the cursor is not a TAILABLE_AWAIT cursor,
     * this option is ignored.
     *
     * A zero value will be ignored.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum await execution time in the given time unit
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public long getMaxAwaitTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxAwaitTimeMS, MILLISECONDS);
    }

    /**
     * Sets the maximum await execution time on the server for this operation.
     *
     * @param maxAwaitTime the max await time.  A value less than one will be ignored, and indicates that the driver should respect the
     *                     server's default value
     * @param timeUnit     the time unit, which may not be null
     * @return this
     */
    public ChangeStreamOperation<T> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        isTrueArgument("maxAwaitTime >= 0", maxAwaitTime >= 0);
        this.maxAwaitTimeMS = MILLISECONDS.convert(maxAwaitTime, timeUnit);
        return this;
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     */
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     *
     * @param collation the collation options to use
     * @return this
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     */
    public ChangeStreamOperation<T> collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * The change stream will only provides changes that occurred after the specified timestamp.
     *
     * <p>Any command run against the server will return an operation time that can be used here.</p>
     * <p>The default value is an operation time obtained from the server before the change stream was created.</p>
     *
     * @param startAtOperationTime the start at operation time
     * @return this
     * @since 4.0
     * @mongodb.server.release 4.0
     * @mongodb.driver.manual reference/method/db.runCommand/
     */
    public ChangeStreamOperation<T> startAtOperationTime(final BsonTimestamp startAtOperationTime) {
        this.startAtOperationTime = startAtOperationTime;
        return this;
    }

    /**
     * Returns the start at operation time
     *
     * @return the start at operation time
     * @since 4.0
     * @mongodb.server.release 4.0
     */
    public BsonTimestamp getStartAtOperationTime() {
        return startAtOperationTime;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnectionAndSource<BatchCursor<T>>() {
            @Override
            public BatchCursor<T> call(final ConnectionSource source, final Connection connection) {
                validateReadConcernAndCollation(connection, binding.getSessionContext().getReadConcern(), collation);
                return new ChangeStreamBatchCursor<T>(ChangeStreamOperation.this,
                        executeWrappedCommandProtocol(binding, namespace.getDatabaseName(),
                                getCommand(connection.getDescription(), binding.getSessionContext()),
                                CommandResultDocumentCodec.create(RAW_BSON_DOCUMENT_CODEC, FIELD_NAMES_WITH_RESULT), connection,
                                transformer(source, connection)), binding);
            }
        });
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        withConnection(binding, new AsyncCallableWithConnectionAndSource() {
            @Override
            public void call(final AsyncConnectionSource source, final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<AsyncBatchCursor<T>> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    final SingleResultCallback<AsyncBatchCursor<T>> wrappedCallback = releasingCallback(errHandlingCallback, source,
                            connection);
                    validateReadConcernAndCollation(source, connection, binding.getSessionContext().getReadConcern(), collation,
                            new AsyncCallableWithConnectionAndSource() {
                                @Override
                                public void call(final AsyncConnectionSource source, final AsyncConnection connection, final Throwable t) {
                                    if (t != null) {
                                        wrappedCallback.onResult(null, t);
                                    } else {
                                        executeWrappedCommandProtocolAsync(binding, namespace.getDatabaseName(),
                                                getCommand(connection.getDescription(), binding.getSessionContext()),
                                                CommandResultDocumentCodec.create(RAW_BSON_DOCUMENT_CODEC, FIELD_NAMES_WITH_RESULT),
                                                connection, asyncTransformer(source, connection),
                                                new SingleResultCallback<AsyncBatchCursor<RawBsonDocument>>() {
                                                    @Override
                                                    public void onResult(final AsyncBatchCursor<RawBsonDocument> result,
                                                                         final Throwable t) {
                                                        if (t != null) {
                                                            wrappedCallback.onResult(null, t);
                                                        } else {
                                                            wrappedCallback.onResult(
                                                                    new AsyncChangeStreamBatchCursor<T>(ChangeStreamOperation.this,
                                                                            result, binding), null);
                                                        }
                                                    }
                                                });
                                    }
                                }
                            });
                }
            }
        });
    }

    private BsonDocument getCommand(final ConnectionDescription description, final SessionContext sessionContext) {
        BsonValue aggregate = changeStreamLevel == ChangeStreamLevel.COLLECTION
                ? new BsonString(namespace.getCollectionName()) : new BsonInt32(1);
        BsonDocument commandDocument = new BsonDocument("aggregate", aggregate);
        appendReadConcernToCommand(sessionContext, commandDocument);

        List<BsonDocument> changeStreamPipeline = new ArrayList<BsonDocument>();
        BsonDocument changeStream = new BsonDocument("fullDocument", new BsonString(fullDocument.getValue()));

        if (changeStreamLevel == ChangeStreamLevel.CLIENT) {
            changeStream.append("allChangesForCluster", BsonBoolean.TRUE);
        }

        if (resumeToken != null) {
            changeStream.append("resumeAfter", resumeToken);
        }

        BsonTimestamp startAtTime = startAtOperationTime;
        if (startAtTime == null && resumeToken == null && serverIsAtLeastVersionFourDotZero(description)) {
            if (sessionContext.getOperationTime() != null
                    && sessionContext.getOperationTime().compareTo(description.getOperationTime()) > 0) {
                startAtTime = sessionContext.getOperationTime();
            } else {
                startAtTime = description.getOperationTime();
            }
        }

        if (startAtTime != null) {
            changeStream.append("startAtOperationTime", startAtTime);
        }

        changeStreamPipeline.add(new BsonDocument("$changeStream", changeStream));
        changeStreamPipeline.addAll(pipeline);
        commandDocument.put("pipeline", new BsonArray(changeStreamPipeline));
        BsonDocument cursor = new BsonDocument();
        if (batchSize != null) {
            cursor.put("batchSize", new BsonInt32(batchSize));
        }
        commandDocument.put(CURSOR, cursor);
        if (collation != null) {
            commandDocument.put("collation", collation.asDocument());
        }
        return commandDocument;
    }

    private CommandTransformer<BsonDocument, BatchCursor<RawBsonDocument>> transformer(final ConnectionSource source,
                                                                                       final Connection connection) {
        return new CommandTransformer<BsonDocument, BatchCursor<RawBsonDocument>>() {
            @Override
            public BatchCursor<RawBsonDocument> apply(final BsonDocument result, final ServerAddress serverAddress) {
                QueryResult<RawBsonDocument> queryResult = cursorDocumentToQueryResult(result.getDocument(CURSOR), serverAddress);
                return new QueryBatchCursor<RawBsonDocument>(queryResult, 0, batchSize != null ? batchSize : 0, maxAwaitTimeMS,
                        RAW_BSON_DOCUMENT_CODEC, source, connection);
            }
        };
    }

    private CommandTransformer<BsonDocument, AsyncBatchCursor<RawBsonDocument>> asyncTransformer(final AsyncConnectionSource source,
                                                                                                 final AsyncConnection connection) {
        return new CommandTransformer<BsonDocument, AsyncBatchCursor<RawBsonDocument>>() {
            @Override
            public AsyncBatchCursor<RawBsonDocument> apply(final BsonDocument result, final ServerAddress serverAddress) {
                QueryResult<RawBsonDocument> queryResult = cursorDocumentToQueryResult(result.getDocument(CURSOR), serverAddress);
                return new AsyncQueryBatchCursor<RawBsonDocument>(queryResult, 0, batchSize != null ? batchSize : 0, maxAwaitTimeMS,
                        RAW_BSON_DOCUMENT_CODEC, source, connection);
            }
        };
    }

}
