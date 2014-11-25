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

import com.mongodb.MongoNamespace;
import com.mongodb.ServerCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.connection.Connection;
import com.mongodb.connection.QueryResult;
import org.bson.codecs.Decoder;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.async.ErrorHandlingResultCallback.wrapCallback;
import static com.mongodb.operation.CursorHelper.getNumberToReturn;
import static java.util.Arrays.asList;

class AsyncQueryBatchCursor<T> implements AsyncBatchCursor<T> {
    private final MongoNamespace namespace;
    private final int limit;
    private final Decoder<T> decoder;
    private final AsyncConnectionSource connectionSource;
    private volatile QueryResult<T> firstBatch;
    private volatile int batchSize;
    private volatile ServerCursor cursor;
    private volatile int count;
    private volatile boolean closed;

    AsyncQueryBatchCursor(final MongoNamespace namespace, final QueryResult<T> firstBatch, final int limit, final int batchSize,
                          final Decoder<T> decoder, final AsyncConnectionSource connectionSource) {
        this.namespace = namespace;
        this.firstBatch = firstBatch;
        this.limit = limit;
        this.batchSize = batchSize;
        this.decoder = decoder;
        this.cursor = firstBatch.getCursor();
        this.connectionSource = notNull("connectionSource", connectionSource).retain();
        this.count += firstBatch.getResults().size();
        if (limitReached()) {
            killCursor();
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            killCursor();
        }
    }

    @Override
    public void next(final SingleResultCallback<List<T>> callback) {
        isTrue("open", !closed);
        // may be empty for a tailable cursor
        if (firstBatch  != null && !firstBatch.getResults().isEmpty()) {
            List<T> results = firstBatch.getResults();
            firstBatch = null;
            callback.onResult(results, null);
        } else {
            if (cursor == null) {
                close();
                callback.onResult(null, null);
            } else {
                getMore(callback);
            }
        }
    }

    @Override
    public void setBatchSize(final int batchSize) {
        isTrue("open", !closed);
        this.batchSize = batchSize;
    }

    @Override
    public int getBatchSize() {
        isTrue("open", !closed);
        return batchSize;
    }

    private boolean limitReached() {
        return limit != 0 && count >= limit;
    }

    private void getMore(final SingleResultCallback<List<T>> callback) {
        connectionSource.getConnection(new SingleResultCallback<Connection>() {
            @Override
            public void onResult(final Connection connection, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    connection.getMoreAsync(namespace, cursor.getId(), getNumberToReturn(limit, batchSize, count),
                                            decoder, new QueryResultSingleResultCallback(connection, callback));
                }
            }
        });
    }

    private void killCursor() {
        if (cursor != null) {
            final ServerCursor localCursor = cursor;
            cursor = null;
            connectionSource.getConnection(new SingleResultCallback<Connection>() {
                @Override
                public void onResult(final Connection connection, final Throwable connectionException) {
                    connection.killCursorAsync(asList(localCursor.getId()), new SingleResultCallback<Void>() {
                                  @Override
                                  public void onResult(final Void result, final Throwable t) {
                                      connection.release();
                                      connectionSource.release();
                                  }
                              });
                }
            });
        } else {
            connectionSource.release();
        }
    }

    private class QueryResultSingleResultCallback implements SingleResultCallback<QueryResult<T>> {
        private final Connection connection;
        private final SingleResultCallback<List<T>> callback;

        public QueryResultSingleResultCallback(final Connection connection, final SingleResultCallback<List<T>> callback) {
            this.connection = connection;
            this.callback = wrapCallback(callback);
        }

        @Override
        public void onResult(final QueryResult<T> result, final Throwable t) {
            if (t != null) {
                connection.release();
                close();
                callback.onResult(null, t);
            } else if (result.getResults().isEmpty() && result.getCursor() != null) {
                connection.getMoreAsync(namespace, cursor.getId(), getNumberToReturn(limit, batchSize, count),
                                        decoder, this);
            } else {
                cursor = result.getCursor();
                count += result.getResults().size();
                if (limitReached()) {
                    killCursor();
                }
                connection.release();
                if (result.getResults().isEmpty()) {
                    callback.onResult(null, null);
                } else {
                    callback.onResult(result.getResults(), null);
                }
            }
        }
    }
}
