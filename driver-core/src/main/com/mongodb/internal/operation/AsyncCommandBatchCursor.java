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
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoSocketException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.async.AsyncAggregateResponseBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.AsyncCallbackSupplier;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.operation.AsyncOperationHelper.AsyncCallableConnectionWithCallback;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.FIRST_BATCH;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CLOSED_AS_CURSOR;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CONCURRENT_OPERATION;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.NEXT_BATCH;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.NO_OP_FIELD_NAME_VALIDATOR;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.getKillCursorsCommand;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.getCommandCursorResult;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.getMoreCommandDocument;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.translateCommandException;
import static java.util.Collections.emptyList;

class AsyncCommandBatchCursor<T> implements AsyncAggregateResponseBatchCursor<T> {

    private final MongoNamespace namespace;
    private final int limit;
    private final long maxTimeMS;
    private final Decoder<T> decoder;
    @Nullable
    private final BsonValue comment;
    private final int maxWireVersion;
    private final boolean firstBatchEmpty;
    private final ResourceManager resourceManager;
    private final CommandCursorResult<T> commandCursorResult;
    private final AtomicInteger count = new AtomicInteger();
    private final AtomicBoolean processedInitial = new AtomicBoolean();
    private int batchSize;

    AsyncCommandBatchCursor(
            final BsonDocument commandCursorDocument,
            final int limit, final int batchSize, final long maxTimeMS,
            final Decoder<T> decoder,
            @Nullable final BsonValue comment,
            final AsyncConnectionSource connectionSource,
            final AsyncConnection connection) {
        ConnectionDescription connectionDescription = connection.getDescription();
        this.commandCursorResult = toCommandCursorResult(connectionDescription.getServerAddress(), FIRST_BATCH, commandCursorDocument);
        this.namespace = commandCursorResult.getNamespace();
        this.limit = limit;
        this.batchSize = batchSize;
        this.maxTimeMS = maxTimeMS;
        this.decoder = notNull("decoder", decoder);
        this.comment = comment;
        this.maxWireVersion = connectionDescription.getMaxWireVersion();
        this.firstBatchEmpty = commandCursorResult.getResults().isEmpty();

        AsyncConnection connectionToPin = null;
        boolean releaseServerAndResources = false;
        if (limitReached()) {
            releaseServerAndResources = true;
        } else if (connectionDescription.getServerType() == ServerType.LOAD_BALANCER) {
            connectionToPin = connection;
        }

        resourceManager = new ResourceManager(namespace, connectionSource, connectionToPin, commandCursorResult.getServerCursor());
        if (releaseServerAndResources) {
            resourceManager.releaseServerAndClientResources(connection);
        }
    }

    @Override
    public void next(final SingleResultCallback<List<T>> callback) {
        if (isClosed()) {
            callback.onResult(null, new MongoException(MESSAGE_IF_CLOSED_AS_CURSOR));
            return;
        }



        resourceManager.execute((AsyncCallbackSupplier<List<T>>) funcCallback -> {
            ServerCursor localServerCursor = resourceManager.getServerCursor();
            boolean serverCursorIsNull = localServerCursor == null;
            List<T> batchResults = emptyList();
            if (!processedInitial.getAndSet(true) && !firstBatchEmpty) {
                batchResults = commandCursorResult.getResults();
            }

            if (serverCursorIsNull || !batchResults.isEmpty()) {
                if (serverCursorIsNull) {
                    close();
                }
                funcCallback.onResult(batchResults, null);
            } else {
                getMore(localServerCursor, funcCallback);
            }
        }).get((r, t) -> {
            if (limitReached()) {
                close();
            }
            callback.onResult(r, t);
        });
    }

    @Override
    public boolean isClosed() {
        return !resourceManager.operable();
    }

    @Override
    public void setBatchSize(final int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public void close() {
        resourceManager.close();
    }

    @Nullable
    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    ServerCursor getServerCursor() {
        if (!resourceManager.operable()) {
            return null;
        }
        return resourceManager.getServerCursor();
    }

    @Override
    public BsonDocument getPostBatchResumeToken() {
        return commandCursorResult.getPostBatchResumeToken();
    }

    @Override
    public BsonTimestamp getOperationTime() {
        return commandCursorResult.getOperationTime();
    }

    @Override
    public boolean isFirstBatchEmpty() {
        return firstBatchEmpty;
    }

    @Override
    public int getMaxWireVersion() {
        return maxWireVersion;
    }

    private void getMore(final ServerCursor cursor, final SingleResultCallback<List<T>> callback) {
        resourceManager.executeWithConnection((connection, wrappedCallback) ->
                        assertNotNull(connection).commandAsync(namespace.getDatabaseName(),
                                getMoreCommandDocument(cursor.getId(), connection.getDescription(), namespace,
                                        limit, batchSize, count.get(), maxTimeMS, comment),
                                NO_OP_FIELD_NAME_VALIDATOR, ReadPreference.primary(),
                                CommandResultDocumentCodec.create(decoder, NEXT_BATCH),
                                assertNotNull(resourceManager.getConnectionSource()).getOperationContext(),
                                (commandResult, t) -> {
                                    if (t != null) {
                                        Throwable translatedException =
                                                t instanceof MongoCommandException
                                                        ? translateCommandException((MongoCommandException) t, cursor)
                                                        : t;
                                        wrappedCallback.onResult(null, translatedException);
                                        return;
                                    }
                                    CommandCursorResult<T> commandCursorResult = toCommandCursorResult(
                                            connection.getDescription().getServerAddress(), NEXT_BATCH, assertNotNull(commandResult));
                                    resourceManager.setServerCursor(commandCursorResult.getServerCursor());

                                    if (!resourceManager.operable()) {
                                        // The cursor was closed
                                        resourceManager.releaseServerAndClientResources(connection);
                                        wrappedCallback.onResult(emptyList(), null);
                                        return;
                                    }

                                    List<T> nextBatch = commandCursorResult.getResults();
                                    if (nextBatch.isEmpty() && commandCursorResult.getServerCursor() != null) {
                                        getMore(commandCursorResult.getServerCursor(), wrappedCallback);
                                    } else {
                                        wrappedCallback.onResult(nextBatch, null);
                                    }
                                }), callback);
    }

    private CommandCursorResult<T> toCommandCursorResult(final ServerAddress serverAddress, final String fieldNameContainingBatch,
            final BsonDocument commandCursorDocument) {
        CommandCursorResult<T> commandCursorResult = getCommandCursorResult(serverAddress, fieldNameContainingBatch, commandCursorDocument);
        this.count.addAndGet(commandCursorResult.getResults().size());
        return commandCursorResult;
    }

    private boolean limitReached() {
        return Math.abs(limit) != 0 && count.get() >= Math.abs(limit);
    }

    @ThreadSafe
    private static final class ResourceManager extends CursorResourceManager<AsyncConnectionSource, AsyncConnection> {

        ResourceManager(
                final MongoNamespace namespace,
                final AsyncConnectionSource connectionSource,
                @Nullable final AsyncConnection connectionToPin,
                @Nullable final ServerCursor serverCursor) {
            super(new ReentrantLock(), namespace, connectionSource, connectionToPin, serverCursor);
        }

        /**
         * Thread-safe.
         * Executes {@code operation} within the {@link #tryStartOperation()}/{@link #endOperation()} bounds.
         */
        <R> AsyncCallbackSupplier<R> execute(final AsyncCallbackSupplier<R> callbackSupplier) {
            return callback -> {
                if (!tryStartOperation()) {
                    callback.onResult(null, new IllegalStateException(MESSAGE_IF_CONCURRENT_OPERATION));
                } else {
                    callbackSupplier.whenComplete(this::endOperation).get(callback);
                }
            };
        }

        @Override
        void markAsPinned(final AsyncConnection connectionToPin, final Connection.PinningMode pinningMode) {
            connectionToPin.markAsPinned(pinningMode);
        }

        @Override
        void doClose() {
            if (isSkipReleasingServerResourcesOnClose()) {
                unsetServerCursor();
            }

            if (getServerCursor() != null) {
                // Don't handle corrupted connections
                getConnection((connection, t) -> {
                    if (connection != null) {
                        releaseServerAndClientResources(connection, (r, t1) -> connection.release());
                    } else {
                        unsetServerCursor();
                        releaseClientResources();
                    }
                });
            } else {
                releaseClientResources();
            }
        }

        <R> void executeWithConnection(final AsyncCallableConnectionWithCallback<R> callable, final SingleResultCallback<R> callback) {
            getConnection((connection, t) -> {
                if (t != null) {
                    callback.onResult(null, t);
                    return;
                }
                callable.call(connection, (result, t1) -> {
                    if (t1 instanceof MongoSocketException) {
                        onCorruptedConnection(connection, (MongoSocketException) t1);
                    }
                    assertNotNull(connection).release();
                    callback.onResult(result, t1);
                });
            });
        }

        private void getConnection(final SingleResultCallback<AsyncConnection> callback) {
            assertTrue(getState() != State.IDLE);
            AsyncConnection pinnedConnection = getPinnedConnection();
            if (pinnedConnection != null) {
                callback.onResult(assertNotNull(pinnedConnection).retain(), null);
            } else {
                assertNotNull(getConnectionSource()).getConnection(callback);
            }
        }

        private void releaseServerAndClientResources(final AsyncConnection connection) {
            releaseServerAndClientResources(connection, (r, t) -> { /* Do nothing */ });
        }

        private void releaseServerAndClientResources(final AsyncConnection connection, final SingleResultCallback<Void> callback) {
            AsyncCallbackSupplier<Void> callbackSupplier = funcCallback -> {
                ServerCursor localServerCursor = getServerCursor();
                if (localServerCursor != null) {
                    killServerCursor(getNamespace(), localServerCursor, connection, funcCallback);
                }
            };
            callbackSupplier.whenComplete(() -> {
                unsetServerCursor();
                releaseClientResources();
            }).get(callback);
        }

        private void killServerCursor(final MongoNamespace namespace, final ServerCursor localServerCursor,
                final AsyncConnection localConnection, final SingleResultCallback<Void> callback) {
            OperationContext operationContext = assertNotNull(getConnectionSource()).getOperationContext();
            localConnection.commandAsync(namespace.getDatabaseName(), getKillCursorsCommand(namespace, localServerCursor),
                    NO_OP_FIELD_NAME_VALIDATOR, ReadPreference.primary(), new BsonDocumentCodec(),
                    operationContext, (r, t) -> callback.onResult(null, null));
        }
    }
}
