/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

import com.mongodb.CursorType;
import com.mongodb.ExplainVerbosity;
import com.mongodb.Function;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoQueryException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.AsyncSingleConnectionReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.binding.SingleConnectionReadBinding;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.QueryResult;
import com.mongodb.operation.OperationHelper.CallableWithConnectionAndSource;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.checkValidReadConcern;
import static com.mongodb.operation.OperationHelper.cursorDocumentToQueryResult;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.serverIsAtLeastVersionThreeDotTwo;
import static com.mongodb.operation.OperationHelper.serverIsAtLeastVersionThreeDotZero;
import static com.mongodb.operation.OperationHelper.withConnection;

/**
 * An operation that queries a collection using the provided criteria.
 *
 * @param <T> the operations result type.
 * @since 3.0
 */
public class FindOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private static final String FIRST_BATCH = "firstBatch";

    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    private BsonDocument filter;
    private int batchSize;
    private int limit;
    private BsonDocument modifiers;
    private BsonDocument projection;
    private long maxTimeMS;
    private int skip;
    private BsonDocument sort;
    private CursorType cursorType = CursorType.NonTailable;
    private boolean slaveOk;
    private boolean oplogReplay;
    private boolean noCursorTimeout;
    private boolean partial;
    private ReadConcern readConcern = ReadConcern.DEFAULT;

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param decoder the decoder for the result documents.
     */
    public FindOperation(final MongoNamespace namespace, final Decoder<T> decoder) {
        this.namespace = notNull("namespace", namespace);
        this.decoder = notNull("decoder", decoder);
    }

    /**
     * Gets the namespace.
     *
     * @return the namespace
     */
    public MongoNamespace getNamespace() {
        return namespace;
    }

    /**
     * Gets the decoder used to decode the result documents.
     *
     * @return the decoder
     */
    public Decoder<T> getDecoder() {
        return decoder;
    }

    /**
     * Gets the query filter.
     *
     * @return the query filter
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public BsonDocument getFilter() {
        return filter;
    }

    /**
     * Sets the query filter to apply to the query.
     *
     * @param filter the filter, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public FindOperation<T> filter(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Gets the number of documents to return per batch.  Default to 0, which indicates that the server chooses an appropriate batch size.
     *
     * @return the batch size, which may be null
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public FindOperation<T> batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Gets the limit to apply.  The default is null.
     *
     * @return the limit
     * @mongodb.driver.manual reference/method/cursor.limit/#cursor.limit Limit
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Sets the limit to apply.
     *
     * @param limit the limit, which may be null
     * @return this
     * @mongodb.driver.manual reference/method/cursor.limit/#cursor.limit Limit
     */
    public FindOperation<T> limit(final int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Gets the query modifiers to apply to this operation.  The default is not to apply any modifiers.
     *
     * @return the query modifiers, which may be null
     * @mongodb.driver.manual reference/operator/query-modifier/ Query Modifiers
     */
    public BsonDocument getModifiers() {
        return modifiers;
    }

    /**
     * Sets the query modifiers to apply to this operation.
     *
     * @param modifiers the query modifiers to apply, which may be null.
     * @return this
     * @mongodb.driver.manual reference/operator/query-modifier/ Query Modifiers
     */
    public FindOperation<T> modifiers(final BsonDocument modifiers) {
        this.modifiers = modifiers;
        return this;
    }

    /**
     * Gets a document describing the fields to return for all matching documents.
     *
     * @return the project document, which may be null
     * @mongodb.driver.manual reference/method/db.collection.find/ Projection
     */
    public BsonDocument getProjection() {
        return projection;
    }

    /**
     * Sets a document describing the fields to return for all matching documents.
     *
     * @param projection the project document, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Projection
     */
    public FindOperation<T> projection(final BsonDocument projection) {
        this.projection = projection;
        return this;
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
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
     */
    public FindOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Gets the number of documents to skip.  The default is 0.
     *
     * @return the number of documents to skip, which may be null
     * @mongodb.driver.manual reference/method/cursor.skip/#cursor.skip Skip
     */
    public int getSkip() {
        return skip;
    }

    /**
     * Sets the number of documents to skip.
     *
     * @param skip the number of documents to skip
     * @return this
     * @mongodb.driver.manual reference/method/cursor.skip/#cursor.skip Skip
     */
    public FindOperation<T> skip(final int skip) {
        this.skip = skip;
        return this;
    }

    /**
     * Gets the sort criteria to apply to the query. The default is null, which means that the documents will be returned in an undefined
     * order.
     *
     * @return a document describing the sort criteria
     * @mongodb.driver.manual reference/method/cursor.sort/ Sort
     */
    public BsonDocument getSort() {
        return sort;
    }

    /**
     * Sets the sort criteria to apply to the query.
     *
     * @param sort the sort criteria, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/cursor.sort/ Sort
     */
    public FindOperation<T> sort(final BsonDocument sort) {
        this.sort = sort;
        return this;
    }

    /**
     * Get the cursor type.
     *
     * @return the cursor type
     */
    public CursorType getCursorType() {
        return cursorType;
    }

    /**
     * Sets the cursor type.
     *
     * @param cursorType the cursor type
     * @return this
     */
    public FindOperation<T> cursorType(final CursorType cursorType) {
        this.cursorType = notNull("cursorType", cursorType);
        return this;
    }

    /**
     * Returns true if set to allowed to query non-primary replica set members.
     *
     * @return true if set to allowed to query non-primary replica set members.
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isSlaveOk() {
        return slaveOk;
    }

    /**
     * Sets if allowed to query non-primary replica set members.
     *
     * @param slaveOk true if allowed to query non-primary replica set members.
     * @return this
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public FindOperation<T> slaveOk(final boolean slaveOk) {
        this.slaveOk = slaveOk;
        return this;
    }

    /**
     * Internal replication use only.  Driver users should ordinarily not use this.
     *
     * @return oplogReplay
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isOplogReplay() {
        return oplogReplay;
    }

    /**
     * Internal replication use only.  Driver users should ordinarily not use this.
     *
     * @param oplogReplay the oplogReplay value
     * @return this
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public FindOperation<T> oplogReplay(final boolean oplogReplay) {
        this.oplogReplay = oplogReplay;
        return this;
    }

    /**
     * Returns true if cursor timeout has been turned off.
     *
     * <p>The server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use.</p>
     *
     * @return if cursor timeout has been turned off
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isNoCursorTimeout() {
        return noCursorTimeout;
    }

    /**
     * Sets if the cursor timeout should be turned off.
     *
     * @param noCursorTimeout true if the cursor timeout should be turned off.
     * @return this
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public FindOperation<T> noCursorTimeout(final boolean noCursorTimeout) {
        this.noCursorTimeout = noCursorTimeout;
        return this;
    }

    /**
     * Returns true if can get partial results from a mongos if some shards are down.
     *
     * @return if can get partial results from a mongos if some shards are down
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isPartial() {
        return partial;
    }

    /**
     * Sets if partial results from a mongos if some shards are down are allowed
     *
     * @param partial allow partial results from a mongos if some shards are down
     * @return this
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public FindOperation<T> partial(final boolean partial) {
        this.partial = partial;
        return this;
    }

    /**
     * Gets the read concern
     *
     * @return the read concern
     * @since 3.2
     */
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    /**
     * Sets the read concern
     * @param readConcern the read concern
     * @return this
     * @since 3.2
     */
    public FindOperation<T> readConcern(final ReadConcern readConcern) {
        this.readConcern = notNull("readConcern", readConcern);
        return this;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnectionAndSource<BatchCursor<T>>() {
            @Override
            public BatchCursor<T> call(final ConnectionSource source, final Connection connection) {
                if (serverIsAtLeastVersionThreeDotTwo(connection.getDescription())) {
                    try {
                        return executeWrappedCommandProtocol(binding, namespace.getDatabaseName(), asCommandDocument(),
                                                             CommandResultDocumentCodec.create(decoder, FIRST_BATCH),
                                                             connection, transformer(source, connection));
                    } catch (MongoCommandException e) {
                        throw new MongoQueryException(e.getServerAddress(), e.getErrorCode(), e.getErrorMessage());
                    }
                } else {
                    checkValidReadConcern(connection, readConcern);
                    QueryResult<T> queryResult = connection.query(namespace,
                                                                  asDocument(connection.getDescription(), binding.getReadPreference()),
                                                                  projection,
                                                                  skip,
                                                                  limit,
                                                                  batchSize,
                                                                  isSlaveOk() || binding.getReadPreference().isSlaveOk(),
                                                                  isTailableCursor(),
                                                                  isAwaitData(),
                                                                  isNoCursorTimeout(),
                                                                  isPartial(),
                                                                  isOplogReplay(),
                                                                  decoder);
                    return new QueryBatchCursor<T>(queryResult, limit, batchSize, decoder, source, connection);
                }
            }
        });
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        withConnection(binding, new AsyncCallableWithConnectionAndSource() {
            @Override
            public void call(final AsyncConnectionSource source, final AsyncConnection connection, final Throwable t) {
                if (t != null) {
                    errorHandlingCallback(callback).onResult(null, t);
                } else {
                    if (serverIsAtLeastVersionThreeDotTwo(connection.getDescription())) {
                        executeWrappedCommandProtocolAsync(binding, namespace.getDatabaseName(), asCommandDocument(),
                                                           CommandResultDocumentCodec.create(decoder, FIRST_BATCH),
                                                           connection, asyncTransformer(source, connection),
                                                           releasingCallback(exceptionTransformingCallback(errorHandlingCallback(callback)),
                                                                             source, connection));
                    } else {
                        final SingleResultCallback<AsyncBatchCursor<T>> wrappedCallback = releasingCallback(errorHandlingCallback(callback),
                                                                                                            source, connection);
                        checkValidReadConcern(source, connection, readConcern, new AsyncCallableWithConnectionAndSource() {
                            @Override
                            public void call(final AsyncConnectionSource source, final AsyncConnection connection, final Throwable t) {
                                if (t != null) {
                                    wrappedCallback.onResult(null, t);
                                } else {
                                    connection.queryAsync(namespace, asDocument(connection.getDescription(), binding.getReadPreference()),
                                            projection, skip, limit, batchSize, isSlaveOk() || binding.getReadPreference().isSlaveOk(),
                                            isTailableCursor(), isAwaitData(), isNoCursorTimeout(), isPartial(), isOplogReplay(), decoder,
                                            new SingleResultCallback<QueryResult<T>>() {
                                                @Override
                                                public void onResult(final QueryResult<T> result, final Throwable t) {
                                                    if (t != null) {
                                                        wrappedCallback.onResult(null, t);
                                                    } else {
                                                        wrappedCallback.onResult(new AsyncQueryBatchCursor<T>(result, limit, batchSize,
                                                                        decoder, source, connection), null);
                                                    }
                                                }
                                            });
                                }
                            }
                        });
                    }
                }
            }
        });
    }

    private static <T> SingleResultCallback<T> exceptionTransformingCallback(final SingleResultCallback<T> callback) {
        return new SingleResultCallback<T>() {
            @Override
            public void onResult(final T result, final Throwable t) {
                if (t != null) {
                    if (t instanceof MongoCommandException) {
                        MongoCommandException commandException = (MongoCommandException) t;
                        callback.onResult(result, new MongoQueryException(commandException.getServerAddress(),
                                                                          commandException.getErrorCode(),
                                                                          commandException.getErrorMessage()));
                    } else {
                        callback.onResult(result, t);
                    }
                } else {
                    callback.onResult(result, null);
                }
            }
        };
    }


    /**
     * Gets an operation whose execution explains this operation.
     *
     * @param explainVerbosity the explain verbosity
     * @return a read operation that when executed will explain this operation
     */
    public ReadOperation<BsonDocument> asExplainableOperation(final ExplainVerbosity explainVerbosity) {
        notNull("explainVerbosity", explainVerbosity);
        return new ReadOperation<BsonDocument>() {
            @Override
            public BsonDocument execute(final ReadBinding binding) {
                return withConnection(binding, new CallableWithConnectionAndSource<BsonDocument>() {
                    @Override
                    public BsonDocument call(final ConnectionSource connectionSource, final Connection connection) {
                        ReadBinding singleConnectionBinding = new SingleConnectionReadBinding(binding.getReadPreference(),
                                                                                              connectionSource.getServerDescription(),
                                                                                              connection);
                        try {
                            if (serverIsAtLeastVersionThreeDotZero(connection.getDescription())) {
                                try {
                                    return new CommandReadOperation<BsonDocument>(getNamespace().getDatabaseName(),
                                                                                  new BsonDocument("explain", asCommandDocument()),
                                                                                  new BsonDocumentCodec()).execute(singleConnectionBinding);
                                } catch (MongoCommandException e) {
                                    throw new MongoQueryException(e.getServerAddress(), e.getErrorCode(), e.getErrorMessage());
                                }
                            } else {
                                BatchCursor<BsonDocument> cursor = createExplainableQueryOperation().execute(singleConnectionBinding);
                                try {
                                    return cursor.next().iterator().next();
                                } finally {
                                    cursor.close();
                                }
                            }
                        } finally {
                            singleConnectionBinding.release();
                        }
                    }
                });
            }
        };
    }

    /**
     * Gets an operation whose execution explains this operation.
     *
     * @param explainVerbosity the explain verbosity
     * @return a read operation that when executed will explain this operation
     */
    public AsyncReadOperation<BsonDocument> asExplainableOperationAsync(final ExplainVerbosity explainVerbosity) {
        notNull("explainVerbosity", explainVerbosity);
        return new AsyncReadOperation<BsonDocument>() {
            @Override
            public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<BsonDocument> callback) {
                withConnection(binding, new AsyncCallableWithConnectionAndSource() {
                    @Override
                    public void call(final AsyncConnectionSource connectionSource, final AsyncConnection connection, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            AsyncReadBinding singleConnectionReadBinding =
                            new AsyncSingleConnectionReadBinding(binding.getReadPreference(), connectionSource.getServerDescription(),
                                                                 connection);

                            if (serverIsAtLeastVersionThreeDotTwo(connection.getDescription())) {
                                new CommandReadOperation<BsonDocument>(namespace.getDatabaseName(),
                                                                       new BsonDocument("explain", asCommandDocument()),
                                                                       new BsonDocumentCodec())
                                .executeAsync(singleConnectionReadBinding,
                                              releasingCallback(exceptionTransformingCallback(errorHandlingCallback(callback)),
                                                                singleConnectionReadBinding, connectionSource, connection));
                            } else {
                                createExplainableQueryOperation()
                                .executeAsync(singleConnectionReadBinding,
                                              releasingCallback(errorHandlingCallback(new ExplainResultCallback(callback)),
                                                                singleConnectionReadBinding, connectionSource, connection));
                            }
                        }
                    }
                });
            }
        };
    }

    private FindOperation<BsonDocument> createExplainableQueryOperation() {
        FindOperation<BsonDocument> explainFindOperation = new FindOperation<BsonDocument>(namespace, new BsonDocumentCodec());

        BsonDocument explainModifiers = new BsonDocument();
        if (modifiers != null) {
            explainModifiers.putAll(modifiers);
        }
        explainModifiers.append("$explain", BsonBoolean.TRUE);

        return explainFindOperation.filter(filter)
               .projection(projection)
               .sort(sort)
               .skip(skip)
               .limit(Math.abs(limit) * -1)
               .modifiers(explainModifiers);

    }

    private BsonDocument asDocument(final ConnectionDescription connectionDescription, final ReadPreference readPreference) {
        BsonDocument document = new BsonDocument();

        if (modifiers != null) {
            document.putAll(modifiers);
        }

        if (sort != null) {
            document.put("$orderby", sort);
        }

        if (maxTimeMS > 0) {
            document.put("$maxTimeMS", new BsonInt64(maxTimeMS));
        }

        if (connectionDescription.getServerType() == SHARD_ROUTER && !readPreference.equals(primary())) {
            document.put("$readPreference", readPreference.toDocument());
        }

        if (document.isEmpty()) {
            document = filter != null ? filter : new BsonDocument();
        } else if (filter != null) {
            document.put("$query", filter);
        } else if (!document.containsKey("$query")) {
            document.put("$query", new BsonDocument());
        }

        return document;
    }

    private static final Map<String, String> META_OPERATOR_TO_COMMAND_FIELD_MAP = new HashMap<String, String>();

    static {
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$query", "filter");
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$orderby", "sort");
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$hint", "hint");
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$comment", "comment");
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$maxScan", "maxScan");
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$maxTimeMS", "maxTimeMS");
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$max", "max");
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$min", "min");
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$returnKey", "returnKey");
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$showDiskLoc", "showRecordId");
        META_OPERATOR_TO_COMMAND_FIELD_MAP.put("$snapshot", "snapshot");
    }

    private BsonDocument asCommandDocument() {
        BsonDocument document = new BsonDocument("find", new BsonString(namespace.getCollectionName()));

        if (modifiers != null) {
            for (Map.Entry<String, BsonValue> cur : modifiers.entrySet()) {
                String commandFieldName = META_OPERATOR_TO_COMMAND_FIELD_MAP.get(cur.getKey());
                if (commandFieldName != null) {
                    document.append(commandFieldName, cur.getValue());
                }
            }
        }

        if (filter != null) {
            document.put("filter", filter);
        }

        if (sort != null) {
            document.put("sort", sort);
        }

        if (projection != null) {
            document.put("projection", projection);
        }

        if (skip > 0) {
            document.put("skip", new BsonInt32(skip));
        }

        if (limit != 0) {
            document.put("limit", new BsonInt32(Math.abs(limit)));
        }

        if (batchSize != 0) {
            document.put("batchSize", new BsonInt32(Math.abs(batchSize)));
        }

        if (limit < 0 || batchSize < 0) {
            document.put("singleBatch", BsonBoolean.TRUE);
        }

        if (maxTimeMS > 0) {
            document.put("maxTimeMS", new BsonInt64(maxTimeMS));
        }

        if (isTailableCursor()) {
            document.put("tailable", BsonBoolean.TRUE);
        }

        if (isAwaitData()) {
            document.put("awaitData", BsonBoolean.TRUE);
        }

        if (oplogReplay) {
            document.put("oplogReplay", BsonBoolean.TRUE);
        }

        if (noCursorTimeout) {
            document.put("noCursorTimeout", BsonBoolean.TRUE);
        }

        if (partial) {
            document.put("allowPartialResults", BsonBoolean.TRUE);
        }

        if (readConcern.getValue() != null) {
            document.put("readConcern", new BsonDocument("level", new BsonString(readConcern.getValue())));
        }

        return document;
    }

    private boolean isTailableCursor() {
        return cursorType.isTailable();
    }

    private boolean isAwaitData() {
        return cursorType == CursorType.TailableAwait;
    }

    private Function<BsonDocument, BatchCursor<T>> transformer(final ConnectionSource source, final Connection connection) {
        return new Function<BsonDocument, BatchCursor<T>>() {
            @Override
            public BatchCursor<T> apply(final BsonDocument result) {
                QueryResult<T> queryResult = cursorDocumentToQueryResult(result.getDocument("cursor"),
                                                                         connection.getDescription().getServerAddress());
                return new QueryBatchCursor<T>(queryResult, 0, batchSize, decoder, source);
            }
        };
    }

    private Function<BsonDocument, AsyncBatchCursor<T>> asyncTransformer(final AsyncConnectionSource source,
                                                                         final AsyncConnection connection) {
        return new Function<BsonDocument, AsyncBatchCursor<T>>() {
            @Override
            public AsyncBatchCursor<T> apply(final BsonDocument result) {
                QueryResult<T> queryResult = cursorDocumentToQueryResult(result.getDocument("cursor"),
                                                                         connection.getDescription().getServerAddress());
                return new AsyncQueryBatchCursor<T>(queryResult, 0, batchSize, decoder, source, connection);
            }
        };
    }

    private static class ExplainResultCallback implements SingleResultCallback<AsyncBatchCursor<BsonDocument>> {
        private final SingleResultCallback<BsonDocument> callback;

        public ExplainResultCallback(final SingleResultCallback<BsonDocument> callback) {
            this.callback = callback;
        }

        @Override
        public void onResult(final AsyncBatchCursor<BsonDocument> cursor, final Throwable t) {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                cursor.next(new SingleResultCallback<List<BsonDocument>>() {
                    @Override
                    public void onResult(final List<BsonDocument> result, final Throwable t) {
                        cursor.close();
                        if (t != null) {
                            callback.onResult(null, t);
                        } else if (result == null || result.size() == 0) {
                            callback.onResult(null, new MongoInternalException("Expected explain result"));
                        } else {
                            callback.onResult(result.get(0), null);
                        }
                    }
                });
            }
        }
    }
}
