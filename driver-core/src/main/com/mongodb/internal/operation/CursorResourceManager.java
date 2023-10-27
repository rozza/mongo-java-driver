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

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoSocketException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerCursor;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.fail;
import static com.mongodb.internal.Locks.withLock;
import static java.util.Collections.singletonList;

/**
 * This class maintains all resources that must be released in {@link CommandBatchCursor#close()}.
 * It also implements a {@linkplain #doClose() deferred close action} such that it is totally ordered with other operations of
 * {@link CommandBatchCursor} (methods {@link #tryStartOperation()}/{@link #endOperation()} must be used properly to enforce the order)
 * despite the method {@link CommandBatchCursor#close()} being called concurrently with those operations.
 * This total order induces the happens-before order.
 * <p>
 * The deferred close action does not violate externally observable idempotence of {@link CommandBatchCursor#close()},
 * because {@link CommandBatchCursor#close()} is allowed to release resources "eventually".
 * <p>
 * Only methods explicitly documented as thread-safe are thread-safe,
 * others are not and rely on the total order mentioned above.
 */
@ThreadSafe
final class CursorResourceManager {

    private static final FieldNameValidator NO_OP_FIELD_NAME_VALIDATOR = new NoOpFieldNameValidator();

    private final MongoNamespace namespace;
    private final Lock lock;
    private volatile State state;
    @Nullable
    private volatile ConnectionSource connectionSource;
    @Nullable
    private volatile Connection pinnedConnection;
    @Nullable
    private volatile ServerCursor serverCursor;
    private volatile boolean skipReleasingServerResourcesOnClose;

    CursorResourceManager(final MongoNamespace namespace, @Nullable final ConnectionSource connectionSource,
            @Nullable final Connection connectionToPin, @Nullable final ServerCursor serverCursor) {
        this.namespace = namespace;
        lock = new StampedLock().asWriteLock();
        state = State.IDLE;
        if (serverCursor != null) {
            this.connectionSource = (assertNotNull(connectionSource)).retain();
            if (connectionToPin != null) {
                this.pinnedConnection = connectionToPin.retain();
                connectionToPin.markAsPinned(Connection.PinningMode.CURSOR);
            }
        }
        skipReleasingServerResourcesOnClose = false;
        this.serverCursor = serverCursor;
    }

    @Nullable
    public ConnectionSource getConnectionSource() {
        return connectionSource;
    }

    /**
     * Thread-safe.
     */
    boolean operable() {
        return state.operable();
    }

    /**
     * Thread-safe.
     * Executes {@code operation} within the {@link #tryStartOperation()}/{@link #endOperation()} bounds.
     *
     * @throws IllegalStateException If {@linkplain CommandBatchCursor#close() closed}.
     */
    @Nullable
    <R> R execute(final String exceptionMessageIfClosed, final Supplier<R> operation) throws IllegalStateException {
        if (!tryStartOperation()) {
            throw new IllegalStateException(exceptionMessageIfClosed);
        }
        try {
            return operation.get();
        } finally {
            endOperation();
        }
    }

    /**
     * Thread-safe.
     * Returns {@code true} iff started an operation.
     * If {@linkplain #operable() closed}, then returns false, otherwise completes abruptly.
     *
     * @throws IllegalStateException Iff another operation is in progress.
     */
    private boolean tryStartOperation() throws IllegalStateException {
        return withLock(lock, () -> {
            State localState = state;
            if (!localState.operable()) {
                return false;
            } else if (localState == State.IDLE) {
                state = State.OPERATION_IN_PROGRESS;
                return true;
            } else if (localState == State.OPERATION_IN_PROGRESS) {
                throw new IllegalStateException("Another operation is currently in progress, concurrent operations are not supported");
            } else {
                throw fail(state.toString());
            }
        });
    }

    /**
     * Thread-safe.
     */
    private void endOperation() {
        boolean doClose = withLock(lock, () -> {
            State localState = state;
            if (localState == State.OPERATION_IN_PROGRESS) {
                state = State.IDLE;
                return false;
            } else if (localState == State.CLOSE_PENDING) {
                state = State.CLOSED;
                return true;
            } else {
                throw fail(localState.toString());
            }
        });
        if (doClose) {
            doClose();
        }
    }

    /**
     * Thread-safe.
     */
    void close() {
        boolean doClose = withLock(lock, () -> {
            State localState = state;
            if (localState == State.OPERATION_IN_PROGRESS) {
                state = State.CLOSE_PENDING;
                return false;
            } else if (localState != State.CLOSED) {
                state = State.CLOSED;
                return true;
            }
            return false;
        });
        if (doClose) {
            doClose();
        }
    }

    /**
     * This method is never executed concurrently with either itself or other operations
     * demarcated by {@link #tryStartOperation()}/{@link #endOperation()}.
     */
    private void doClose() {
        try {
            if (skipReleasingServerResourcesOnClose) {
                serverCursor = null;
            } else if (serverCursor != null) {
                Connection connection = connection();
                try {
                    releaseServerResources(connection);
                } finally {
                    connection.release();
                }
            }
        } catch (MongoException e) {
            // ignore exceptions when releasing server resources
        } finally {
            // guarantee that regardless of exceptions, `serverCursor` is null and client resources are released
            serverCursor = null;
            releaseClientResources();
        }
    }

    void onCorruptedConnection(final Connection corruptedConnection) {
        assertTrue(state.inProgress());
        // if `pinnedConnection` is corrupted, then we cannot kill `serverCursor` via such a connection
        Connection localPinnedConnection = pinnedConnection;
        if (localPinnedConnection != null) {
            assertTrue(corruptedConnection == localPinnedConnection);
            skipReleasingServerResourcesOnClose = true;
        }
    }

    void executeWithConnection(final Consumer<Connection> action) {
        Connection connection = connection();
        try {
            action.accept(connection);
        } catch (MongoSocketException e) {
            try {
                onCorruptedConnection(connection);
            } catch (Exception suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        } finally {
            connection.release();
        }
    }

    private Connection connection() {
        assertTrue(state != State.IDLE);
        if (pinnedConnection == null) {
            return assertNotNull(connectionSource).getConnection();
        } else {
            return assertNotNull(pinnedConnection).retain();
        }
    }

    /**
     * Thread-safe.
     */
    @Nullable
    ServerCursor serverCursor() {
        return serverCursor;
    }

    void setServerCursor(@Nullable final ServerCursor serverCursor) {
        assertTrue(state.inProgress());
        assertNotNull(this.serverCursor);
        // without `connectionSource` we will not be able to kill `serverCursor` later
        assertNotNull(connectionSource);
        this.serverCursor = serverCursor;
        if (serverCursor == null) {
            releaseClientResources();
        }
    }


    void releaseServerAndClientResources(final Connection connection) {
        try {
            releaseServerResources(assertNotNull(connection));
        } finally {
            releaseClientResources();
        }
    }

    private void releaseServerResources(final Connection connection) {
        try {
            ServerCursor localServerCursor = serverCursor;
            if (localServerCursor != null) {
                killServerCursor(namespace, localServerCursor, assertNotNull(connection));
            }
        } finally {
            serverCursor = null;
        }
    }

    private void killServerCursor(final MongoNamespace namespace, final ServerCursor serverCursor, final Connection connection) {
        connection.command(namespace.getDatabaseName(), asKillCursorsCommandDocument(namespace, serverCursor),
                NO_OP_FIELD_NAME_VALIDATOR, ReadPreference.primary(), new BsonDocumentCodec(),
                assertNotNull(connectionSource).getOperationContext());
    }

    private BsonDocument asKillCursorsCommandDocument(final MongoNamespace namespace, final ServerCursor serverCursor) {
        return new BsonDocument("killCursors", new BsonString(namespace.getCollectionName()))
                .append("cursors", new BsonArray(singletonList(new BsonInt64(serverCursor.getId()))));
    }

    private void releaseClientResources() {
        assertNull(serverCursor);
        ConnectionSource localConnectionSource = connectionSource;
        if (localConnectionSource != null) {
            localConnectionSource.release();
            connectionSource = null;
        }
        Connection localPinnedConnection = pinnedConnection;
        if (localPinnedConnection != null) {
            localPinnedConnection.release();
            pinnedConnection = null;
        }
    }

    private enum State {
        IDLE(true, false),
        OPERATION_IN_PROGRESS(true, true),
        /**
         * Implies {@link #OPERATION_IN_PROGRESS}.
         */
        CLOSE_PENDING(false, true),
        CLOSED(false, false);

        private final boolean operable;
        private final boolean inProgress;

        State(final boolean operable, final boolean inProgress) {
            this.operable = operable;
            this.inProgress = inProgress;
        }

        boolean operable() {
            return operable;
        }

        boolean inProgress() {
            return inProgress;
        }
    }
}
