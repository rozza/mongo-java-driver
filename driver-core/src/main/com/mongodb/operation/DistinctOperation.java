/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.Function;
import com.mongodb.MongoNamespace;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.connection.Connection;
import com.mongodb.connection.QueryResult;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.withConnection;

/**
 * Finds the distinct values for a specified field across a single collection.
 *
 * <p>When possible, the distinct command uses an index to find documents and return values.</p>
 *
 * @mongodb.driver.manual reference/command/distinct Distinct Command
 * @since 3.0
 */
public class DistinctOperation implements AsyncReadOperation<AsyncBatchCursor<BsonValue>>, ReadOperation<BatchCursor<BsonValue>> {
    private static final String VALUES = "values";
    private static final Decoder<BsonDocument> DECODER = new BsonDocumentCodec();

    private final MongoNamespace namespace;
    private final String fieldName;
    private BsonDocument filter;
    private long maxTimeMS;

    /**
     * Construct an instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param fieldName the name of the field to return distinct values.
     */
    public DistinctOperation(final MongoNamespace namespace, final String fieldName) {
        this.namespace = notNull("namespace", namespace);
        this.fieldName = notNull("fieldName", fieldName);
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
     * @param filter the query filter, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public DistinctOperation filter(final BsonDocument filter) {
        this.filter = filter;
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
    public DistinctOperation maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public BatchCursor<BsonValue> execute(final ReadBinding binding) {
        return withConnection(binding, new OperationHelper.CallableWithConnectionAndSource<BatchCursor<BsonValue>>() {
            @Override
            public BatchCursor<BsonValue> call(final ConnectionSource source, final Connection connection) {
                return executeWrappedCommandProtocol(namespace.getDatabaseName(), getCommand(), DECODER,
                        connection, binding.getReadPreference(), transformer(source, connection));
            }
        });
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<BsonValue>> callback) {
        withConnection(binding, new OperationHelper.AsyncCallableWithConnectionAndSource() {
            @Override
            public void call(final AsyncConnectionSource source, final Connection connection, final Throwable t) {
                if (t != null) {
                    errorHandlingCallback(callback).onResult(null, t);
                } else {
                    executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), getCommand(), DECODER,
                            connection, binding.getReadPreference(), asyncTransformer(source, connection),
                            releasingCallback(errorHandlingCallback(callback), source, connection));
                }
            }
        });
    }

    private QueryResult<BsonValue> createQueryResult(final BsonDocument result, final Connection connection) {
        return new QueryResult<BsonValue>(namespace, result.getArray(VALUES).getValues(), 0L,
                connection.getDescription().getServerAddress());
    }

    private Function<BsonDocument, BatchCursor<BsonValue>> transformer(final ConnectionSource source, final Connection connection) {
        return new Function<BsonDocument, BatchCursor<BsonValue>>() {
            @Override
            public BatchCursor<BsonValue> apply(final BsonDocument result) {
                QueryResult<BsonValue> queryResult = createQueryResult(result, connection);
                return new QueryBatchCursor<BsonValue>(queryResult, 0, 0, null, source);
            }
        };
    }

    private Function<BsonDocument, AsyncBatchCursor<BsonValue>> asyncTransformer(final AsyncConnectionSource source, final Connection
            connection) {
        return new Function<BsonDocument, AsyncBatchCursor<BsonValue>>() {
            @Override
            public AsyncBatchCursor<BsonValue> apply(final BsonDocument result) {
                QueryResult<BsonValue> queryResult = createQueryResult(result, connection);
                return new AsyncQueryBatchCursor<BsonValue>(queryResult, 0, 0, null, source);
            }
        };
    }

    private BsonDocument getCommand() {
        BsonDocument cmd = new BsonDocument("distinct", new BsonString(namespace.getCollectionName()));
        cmd.put("key", new BsonString(fieldName));
        putIfNotNull(cmd, "query", filter);
        putIfNotZero(cmd, "maxTimeMS", maxTimeMS);

        return cmd;
    }
}
