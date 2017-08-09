/*
 * Copyright 2017 MongoDB, Inc.
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

package com.mongodb.operation;

import com.mongodb.MongoChangeStreamException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoServerException;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.QueryResult;
import com.mongodb.operation.CommandOperationHelper.CommandTransformer;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.bson.codecs.Codec;
import org.bson.codecs.RawBsonDocumentCodec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.CallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.cursorDocumentToQueryResult;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.validateChangeStreams;
import static com.mongodb.operation.OperationHelper.withConnection;

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
    private final String fullDocument;
    private final List<BsonDocument> pipeline;
    private final Codec<T> codec;
    private final boolean resumable;

    private BsonDocument resumeToken;
    private Integer batchSize;
    private Collation collation;
    private long maxAwaitTimeMS;
    private long maxTimeMS;
    private ReadConcern readConcern = ReadConcern.DEFAULT;

    /**
     * Construct a new instance.
     *
     * @param namespace    the database and collection namespace for the operation.
     * @param fullDocument the fullDocument value
     * @param pipeline     the aggregation pipeline.
     * @param codec        the codec for the result documents.
     */
    public ChangeStreamOperation(final MongoNamespace namespace, final String fullDocument, final List<BsonDocument> pipeline,
                                 final Codec<T> codec) {
        this.namespace = notNull("namespace", namespace);
        this.fullDocument = notNull("fullDocument", fullDocument);
        this.pipeline = notNull("pipeline", pipeline);
        this.codec = notNull("codec", codec);
        this.resumable = true;
    }

    ChangeStreamOperation(final ChangeStreamOperation<T> from, final BsonDocument resumeToken) {
        this.namespace = from.namespace;
        this.fullDocument = from.fullDocument;
        this.pipeline = from.pipeline;
        this.codec = from.codec;
        this.batchSize = from.batchSize;
        this.collation = from.collation;
        this.maxAwaitTimeMS = from.maxAwaitTimeMS;
        this.maxTimeMS = from.maxTimeMS;
        this.readConcern = from.readConcern;
        this.resumeToken = resumeToken;
        this.resumable = false;
    }

    /**
     * @return the namespace for this operation
     */
    public MongoNamespace getNamespace() {
        return namespace;
    }

    /**
     * @return the codec for this operation
     */
    public Codec<T> getCodec() {
        return codec;
    }

    /**
     * Returns the fullDocument value, in 3.6 the supported values are: {@code "none"}, {@code "lookup"}.
     * <p>
     * <p>
     * Defaults to {@code "none"}.  When set to {@code "lookup"}, the change stream will include both a delta describing the
     * changes to the document, as well as a copy of the entire document that was changed from some time after the change occurred.
     * </p>
     *
     * @return the fullDocument value
     */
    public String getFullDocument() {
        return fullDocument;
    }

    /**
     * Returns the logical starting point for the new change stream.
     * <p>
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
     * @return true if this is a resumable operation
     */
    public boolean isResumable() {
        return resumable;
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
     * <p>
     * A zero value will be ignored.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum await execution time in the given time unit
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public long getMaxAwaitTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxAwaitTimeMS, TimeUnit.MILLISECONDS);
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
        this.maxAwaitTimeMS = TimeUnit.MILLISECONDS.convert(maxAwaitTime, timeUnit);
        return this;
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public ChangeStreamOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Gets the read concern
     *
     * @return the read concern
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    /**
     * Sets the read concern
     *
     * @param readConcern the read concern
     * @return this
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    public ChangeStreamOperation<T> readConcern(final ReadConcern readConcern) {
        this.readConcern = notNull("readConcern", readConcern);
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
     * <p>
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

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        if (isResumable()) {
            try {
                return getCursor(binding);
            } catch (Exception e) {
                if (e instanceof IllegalArgumentException) {
                    throw (IllegalArgumentException) e;
                } else if (e instanceof MongoServerException
                        && !(e instanceof MongoNotPrimaryException || e instanceof MongoQueryException)) {
                    throw (MongoServerException) e;
                } else if (e instanceof MongoChangeStreamException) {
                    throw (MongoChangeStreamException) e;
                }
                return getCursor(binding);
            }
        } else {
            return getCursor(binding);
        }
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        getCursor(binding, new SingleResultCallback<AsyncBatchCursor<T>>() {
            @Override
            public void onResult(final AsyncBatchCursor<T> result, final Throwable t) {
                if (!resumable) {
                    callback.onResult(result, t);
                } else if (t == null) {
                    callback.onResult(result, null);
                } else if (t instanceof IllegalArgumentException
                        || t instanceof MongoChangeStreamException
                        || t instanceof MongoServerException
                        && !(t instanceof MongoNotPrimaryException || t instanceof MongoQueryException)) {
                    callback.onResult(result, t);
                } else {
                    getCursor(binding, callback);
                }
            }
        });
    }

    private BatchCursor<T> getCursor(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnectionAndSource<BatchCursor<T>>() {
            @Override
            public BatchCursor<T> call(final ConnectionSource source, final Connection connection) {
                validateChangeStreams(connection);
                return executeWrappedCommandProtocol(binding, namespace.getDatabaseName(), getCommand(),
                        CommandResultDocumentCodec.create(RAW_BSON_DOCUMENT_CODEC, FIELD_NAMES_WITH_RESULT),
                        connection, transformer(binding, source, connection));
            }
        });
    }

    private void getCursor(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        withConnection(binding, new AsyncCallableWithConnectionAndSource() {
            @Override
            public void call(final AsyncConnectionSource source, final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<AsyncBatchCursor<T>> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    final SingleResultCallback<AsyncBatchCursor<T>> wrappedCallback =
                            releasingCallback(errHandlingCallback, source, connection);
                    validateChangeStreams(source, connection,
                            new AsyncCallableWithConnectionAndSource() {
                                @Override
                                public void call(final AsyncConnectionSource source, final AsyncConnection connection, final Throwable t) {
                                    if (t != null) {
                                        wrappedCallback.onResult(null, t);
                                    } else {
                                        executeWrappedCommandProtocolAsync(binding, namespace.getDatabaseName(),
                                                getCommand(), CommandResultDocumentCodec.create(RAW_BSON_DOCUMENT_CODEC,
                                                        FIELD_NAMES_WITH_RESULT),
                                                connection, asyncTransformer(binding, source, connection), wrappedCallback);
                                    }
                                }
                            });
                }
            }
        });
    }

    private BsonDocument getCommand() {
        BsonDocument commandDocument = new BsonDocument("aggregate", new BsonString(namespace.getCollectionName()));
        List<BsonDocument> changeStreamPipeline = new ArrayList<BsonDocument>();

        BsonDocument changeStream = new BsonDocument("fullDocument", new BsonString(fullDocument));
        if (resumeToken != null) {
            changeStream.append("resumeAfter", resumeToken);
        }
        changeStreamPipeline.add(new BsonDocument("$changeStream", changeStream));
        changeStreamPipeline.addAll(pipeline);

        commandDocument.put("pipeline", new BsonArray(changeStreamPipeline));

        if (maxTimeMS > 0) {
            commandDocument.put("maxTimeMS", new BsonInt64(maxTimeMS));
        }

        BsonDocument cursor = new BsonDocument();
        if (batchSize != null) {
            cursor.put("batchSize", new BsonInt32(batchSize));
        }
        commandDocument.put(CURSOR, cursor);

        if (!readConcern.isServerDefault()) {
            commandDocument.put("readConcern", readConcern.asDocument());
        }
        if (collation != null) {
            commandDocument.put("collation", collation.asDocument());
        }

        return commandDocument;
    }

    private CommandTransformer<BsonDocument, BatchCursor<T>> transformer(final ReadBinding binding,
                                                                         final ConnectionSource source,
                                                                         final Connection connection) {
        final ChangeStreamOperation<T> changeStreamOperation = this;
        return new CommandTransformer<BsonDocument, BatchCursor<T>>() {
            @Override
            public BatchCursor<T> apply(final BsonDocument result, final ServerAddress serverAddress) {
                return new ChangeStreamBatchCursor<T>(changeStreamOperation, binding,
                        new QueryBatchCursor<RawBsonDocument>(getQueryResult(result, serverAddress), 0, batchSize != null ? batchSize : 0,
                                getMaxTimeForCursor(), RAW_BSON_DOCUMENT_CODEC, source, connection));
            }
        };
    }

    private CommandTransformer<BsonDocument, AsyncBatchCursor<T>> asyncTransformer(final AsyncReadBinding binding,
                                                                                   final AsyncConnectionSource source,
                                                                                   final AsyncConnection connection) {
        final ChangeStreamOperation<T> changeStreamOperation = this;
        return new CommandTransformer<BsonDocument, AsyncBatchCursor<T>>() {
            @Override
            public AsyncBatchCursor<T> apply(final BsonDocument result, final ServerAddress serverAddress) {
                return new AsyncChangeStreamBatchCursor<T>(changeStreamOperation, binding,
                        new AsyncQueryBatchCursor<RawBsonDocument>(getQueryResult(result, serverAddress), 0,
                                batchSize != null ? batchSize : 0, getMaxTimeForCursor(), RAW_BSON_DOCUMENT_CODEC, source, connection));
            }
        };
    }

    QueryResult<RawBsonDocument> getQueryResult(final BsonDocument result, final ServerAddress serverAddress) {
        List<RawBsonDocument> firstBatch = BsonDocumentWrapperHelper.<RawBsonDocument>toList(result.getDocument(CURSOR), FIRST_BATCH);
        if (!firstBatch.isEmpty() && !firstBatch.get(0).containsKey("_id")) {
            throw new MongoChangeStreamException("Cannot provide resume functionality when the resume token is missing.");
        }
        BsonDocument cursorDocument = result.getDocument(CURSOR);
        cursorDocument.put(FIRST_BATCH, new BsonArrayWrapper<RawBsonDocument>(firstBatch));
        return cursorDocumentToQueryResult(cursorDocument, serverAddress);
    }

    private long getMaxTimeForCursor() {
        return maxAwaitTimeMS > 0 ? maxAwaitTimeMS : 0;
    }

}
