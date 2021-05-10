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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.ClientSideOperationTimeoutFactory;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.QueryResult;
import com.mongodb.internal.operation.AsyncCommandOperationHelper.CommandReadTransformerAsync;
import com.mongodb.internal.operation.SyncCommandOperationHelper.CommandReadTransformer;
import com.mongodb.internal.operation.SyncOperationHelper.CallableWithSource;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncCommandOperationHelper.executeCommandAsyncWithConnection;
import static com.mongodb.internal.operation.AsyncOperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.internal.operation.AsyncOperationHelper.createEmptyAsyncBatchCursor;
import static com.mongodb.internal.operation.AsyncOperationHelper.cursorDocumentToAsyncBatchCursor;
import static com.mongodb.internal.operation.AsyncOperationHelper.releasingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.withAsyncReadConnection;
import static com.mongodb.internal.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.internal.operation.CommandOperationHelper.rethrowIfNotNamespaceError;
import static com.mongodb.internal.operation.CursorHelper.getCursorDocumentFromBatchSize;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotZero;
import static com.mongodb.internal.operation.SyncCommandOperationHelper.executeCommandWithConnection;
import static com.mongodb.internal.operation.SyncOperationHelper.createEmptyBatchCursor;
import static com.mongodb.internal.operation.SyncOperationHelper.cursorDocumentToBatchCursor;
import static com.mongodb.internal.operation.SyncOperationHelper.withReadConnectionSource;

/**
 * An operation that lists the indexes that have been created on a collection.  For flexibility, the type of each document returned is
 * generic.
 *
 * @param <T> the operations result type.
 * @since 3.0
 * @mongodb.driver.manual reference/command/listIndexes/ List indexes
 */
public class ListIndexesOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory;
    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    private boolean retryReads;
    private int batchSize;

    /**
     * Construct a new instance.
     *
     * @param clientSideOperationTimeoutFactory the client side operation timeout factory
     * @param namespace the database and collection namespace for the operation.
     * @param decoder   the decoder for the result documents.
     */
    public ListIndexesOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory, final MongoNamespace namespace,
                                final Decoder<T> decoder) {
        this.clientSideOperationTimeoutFactory = notNull("clientSideOperationTimeoutFactory", clientSideOperationTimeoutFactory);
        this.namespace = notNull("namespace", namespace);
        this.decoder = notNull("decoder", decoder);
    }

    /**
     * Gets the number of documents to return per batch.
     *
     * @return the batch size
     * @mongodb.server.release 3.0
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
     * @mongodb.server.release 3.0
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public ListIndexesOperation<T> batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Enables retryable reads if a read fails due to a network error.
     *
     * @param retryReads true if reads should be retried
     * @return this
     * @since 3.11
     */
    public ListIndexesOperation<T> retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    /**
     * Gets the value for retryable reads. The default is true.
     *
     * @return the retryable reads value
     * @since 3.11
     */
    public boolean getRetryReads() {
        return retryReads;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return withReadConnectionSource(clientSideOperationTimeoutFactory.create(), binding, new CallableWithSource<BatchCursor<T>>() {
            @Override
            public BatchCursor<T> call(final ClientSideOperationTimeout clientSideOperationTimeout, final ConnectionSource source) {
                Connection connection = source.getConnection();
                if (serverIsAtLeastVersionThreeDotZero(connection.getDescription())) {
                    try {
                        return executeCommandWithConnection(clientSideOperationTimeout, binding, source, namespace.getDatabaseName(),
                                getCommandCreator(), createCommandDecoder(), transformer(), retryReads, connection);
                    } catch (MongoCommandException e) {
                        return rethrowIfNotNamespaceError(e, createEmptyBatchCursor(clientSideOperationTimeout, namespace, decoder,
                                                                                    source.getServerDescription().getAddress(), batchSize));
                    }
                } else {
                    try {
                        return new QueryBatchCursor<T>(clientSideOperationTimeout, connection.query(getIndexNamespace(),
                                asQueryDocument(clientSideOperationTimeout, connection.getDescription(), binding.getReadPreference()),
                                null, 0, 0, batchSize, binding.getReadPreference().isSlaveOk(), false, false, false, false, false,
                                decoder), 0, batchSize, decoder, source);
                    } finally {
                        connection.release();
                    }
                }
            }
        });
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        withAsyncReadConnection(clientSideOperationTimeoutFactory.create(), binding, new AsyncCallableWithConnectionAndSource() {
            @Override
            public void call(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncConnectionSource source,
                             final AsyncConnection connection,
                             final Throwable t) {
                final SingleResultCallback<AsyncBatchCursor<T>> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    if (serverIsAtLeastVersionThreeDotZero(connection.getDescription())) {
                        executeCommandAsyncWithConnection(clientSideOperationTimeout, binding, source, namespace.getDatabaseName(),
                                getCommandCreator(), createCommandDecoder(), asyncTransformer(), retryReads, connection,
                                new SingleResultCallback<AsyncBatchCursor<T>>() {
                                    @Override
                                    public void onResult(final AsyncBatchCursor<T> result,
                                                         final Throwable t) {
                                        if (t != null && !isNamespaceError(t)) {
                                            errHandlingCallback.onResult(null, t);
                                        } else {
                                            errHandlingCallback.onResult(result != null ? result
                                                    : emptyAsyncCursor(clientSideOperationTimeout, source), null);
                                        }
                                    }
                                });
                    } else {
                        final SingleResultCallback<AsyncBatchCursor<T>> wrappedCallback = releasingCallback(errHandlingCallback,
                                source, connection);
                        connection.queryAsync(getIndexNamespace(),
                                asQueryDocument(clientSideOperationTimeout, connection.getDescription(), binding.getReadPreference()),
                                null, 0, 0, batchSize, binding.getReadPreference().isSlaveOk(), false, false, false, false, false, decoder,
                                new SingleResultCallback<QueryResult<T>>() {
                                    @Override
                                    public void onResult(final QueryResult<T> result, final Throwable t) {
                                        if (t != null) {
                                            wrappedCallback.onResult(null, t);
                                        } else {
                                            wrappedCallback.onResult(new AsyncQueryBatchCursor<T>(clientSideOperationTimeout, result, 0,
                                                            batchSize, decoder, source, connection), null);
                                        }
                                    }
                                });
                    }
                }
            }
        });
    }

    private AsyncBatchCursor<T> emptyAsyncCursor(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                 final AsyncConnectionSource source) {
        return createEmptyAsyncBatchCursor(clientSideOperationTimeout, namespace, source.getServerDescription().getAddress());
    }

    private BsonDocument asQueryDocument(final ClientSideOperationTimeout clientSideOperationTimeout,
                                         final ConnectionDescription connectionDescription, final ReadPreference readPreference) {
        BsonDocument document = new BsonDocument("$query", new BsonDocument("ns", new BsonString(namespace.getFullName())));
        putIfNotZero(document, "$maxTimeMS", clientSideOperationTimeout.getMaxTimeMS());
        if (connectionDescription.getServerType() == SHARD_ROUTER && !readPreference.equals(primary())) {
            document.put("$readPreference", readPreference.toDocument());
        }
        return document;
    }

    private MongoNamespace getIndexNamespace() {
        return new MongoNamespace(namespace.getDatabaseName(), "system.indexes");
    }

    private CommandCreator getCommandCreator() {
        return new CommandCreator() {
            @Override
            public BsonDocument create(final ClientSideOperationTimeout clientSideOperationTimeout,
                                       final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
                return getCommand(clientSideOperationTimeout);
            }
        };
    }

    private BsonDocument getCommand(final ClientSideOperationTimeout clientSideOperationTimeout) {
        BsonDocument command = new BsonDocument("listIndexes", new BsonString(namespace.getCollectionName()))
                .append("cursor", getCursorDocumentFromBatchSize(batchSize == 0 ? null : batchSize));
        long maxTimeMS = clientSideOperationTimeout.getMaxTimeMS();
        if (maxTimeMS > 0) {
            command.put("maxTimeMS", new BsonInt64(maxTimeMS));
        }
        return command;
    }

    private CommandReadTransformer<BsonDocument, BatchCursor<T>> transformer() {
        return new CommandReadTransformer<BsonDocument, BatchCursor<T>>() {
            @Override
            public BatchCursor<T> apply(
                    final ClientSideOperationTimeout clientSideOperationTimeout, final ConnectionSource source,
                    final Connection connection, final BsonDocument result) {
                return cursorDocumentToBatchCursor(clientSideOperationTimeout, result.getDocument("cursor"), decoder, source, batchSize);
            }
        };
    }

    private CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>> asyncTransformer() {
        return new CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>>() {
            @Override
            public AsyncBatchCursor<T> apply(
                    final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncConnectionSource source,
                    final AsyncConnection connection, final BsonDocument result) {
                return cursorDocumentToAsyncBatchCursor(clientSideOperationTimeout, result.getDocument("cursor"), decoder, source,
                        connection, batchSize);
            }
        };
    }

    private Codec<BsonDocument> createCommandDecoder() {
        return CommandResultDocumentCodec.create(decoder, "firstBatch");
    }
}
