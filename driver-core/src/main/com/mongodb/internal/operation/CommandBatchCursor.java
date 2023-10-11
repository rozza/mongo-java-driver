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
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.FIRST_BATCH;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CLOSED_AS_CURSOR;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CLOSED_AS_ITERATOR;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.NEXT_BATCH;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.NO_OP_FIELD_NAME_VALIDATOR;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.getMoreCommandDocument;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.translateCommandException;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static java.lang.String.format;

class CommandBatchCursor<T> implements AggregateResponseBatchCursor<T> {

    private final MongoNamespace namespace;
    private final int limit;
    private final long maxTimeMS;
    private final Decoder<T> decoder;
    @Nullable
    private final BsonValue comment;
    private final int maxWireVersion;
    private final boolean firstBatchEmpty;
    private final ResourceManager resourceManager;

    private int batchSize;
    private CommandCursorResult<T> commandCursorResult;
    private int count = 0;
    @Nullable
    private List<T> nextBatch;

    CommandBatchCursor(
            final BsonDocument commandCursorDocument,
            final int limit, final int batchSize, final long maxTimeMS,
            final Decoder<T> decoder,
            @Nullable final BsonValue comment,
            final ConnectionSource connectionSource,
            final Connection connection) {
        ConnectionDescription connectionDescription = connection.getDescription();
        this.commandCursorResult = initFromCommandCursorDocument(connectionDescription.getServerAddress(), FIRST_BATCH,
                commandCursorDocument);
        this.namespace = commandCursorResult.getNamespace();
        this.limit = limit;
        this.batchSize = batchSize;
        this.maxTimeMS = maxTimeMS;
        this.decoder = notNull("decoder", decoder);
        this.comment = comment;
        this.maxWireVersion = connectionDescription.getMaxWireVersion();
        this.firstBatchEmpty = commandCursorResult.getResults().isEmpty();

        Connection connectionToPin = null;
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
    public boolean hasNext() {
        return assertNotNull(resourceManager.execute(MESSAGE_IF_CLOSED_AS_CURSOR, this::doHasNext));
    }

    private boolean doHasNext() {
        if (nextBatch != null) {
            return true;
        }

        if (limitReached()) {
            return false;
        }

        while (resourceManager.getServerCursor() != null) {
            getMore();
            if (!resourceManager.operable()) {
                throw new IllegalStateException(MESSAGE_IF_CLOSED_AS_CURSOR);
            }
            if (nextBatch != null) {
                return true;
            }
        }

        return false;
    }

    @Override
    public List<T> next() {
        return assertNotNull(resourceManager.execute(MESSAGE_IF_CLOSED_AS_ITERATOR, this::doNext));
    }

    @Override
    public int available() {
        return !resourceManager.operable() || nextBatch == null ? 0 : nextBatch.size();
    }

    @Nullable
    private List<T> doNext() {
        if (!doHasNext()) {
            throw new NoSuchElementException();
        }

        List<T> retVal = nextBatch;
        nextBatch = null;
        return retVal;
    }

    @VisibleForTesting(otherwise = PRIVATE)
    boolean isClosed() {
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
    public void remove() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void close() {
        resourceManager.close();
    }

    @Nullable
    @Override
    public List<T> tryNext() {
        return resourceManager.execute(MESSAGE_IF_CLOSED_AS_CURSOR, () -> {
            if (!tryHasNext()) {
                return null;
            }
            return doNext();
        });
    }

    private boolean tryHasNext() {
        if (nextBatch != null) {
            return true;
        }

        if (limitReached()) {
            return false;
        }

        if (resourceManager.getServerCursor() != null) {
            getMore();
        }

        return nextBatch != null;
    }

    @Override
    @Nullable
    public ServerCursor getServerCursor() {
        if (!resourceManager.operable()) {
            throw new IllegalStateException(MESSAGE_IF_CLOSED_AS_ITERATOR);
        }
        return resourceManager.getServerCursor();
    }

    @Override
    public ServerAddress getServerAddress() {
        if (!resourceManager.operable()) {
            throw new IllegalStateException(MESSAGE_IF_CLOSED_AS_ITERATOR);
        }

        return commandCursorResult.getServerAddress();
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

    private void getMore() {
        ServerCursor serverCursor = assertNotNull(resourceManager.getServerCursor());
        resourceManager.executeWithConnection(connection -> {
            ServerCursor nextServerCursor;
            try {
                this.commandCursorResult = initFromCommandCursorDocument(connection.getDescription().getServerAddress(), NEXT_BATCH,
                        assertNotNull(
                            connection.command(namespace.getDatabaseName(),
                                 getMoreCommandDocument(serverCursor.getId(), connection.getDescription(), namespace,
                                     limit, batchSize, count, maxTimeMS, comment),
                                 NO_OP_FIELD_NAME_VALIDATOR,
                                 ReadPreference.primary(),
                                 CommandResultDocumentCodec.create(decoder, NEXT_BATCH),
                                 assertNotNull(resourceManager.getConnectionSource()).getOperationContext())));
                nextServerCursor = commandCursorResult.getServerCursor();
            } catch (MongoCommandException e) {
                throw translateCommandException(e, serverCursor);
            }
            resourceManager.setServerCursor(nextServerCursor);
            if (limitReached() || !resourceManager.operable()) {
                resourceManager.releaseServerAndClientResources(connection);
            }
        });
    }

    private CommandCursorResult<T> initFromCommandCursorDocument(final ServerAddress serverAddress, final String fieldNameContainingBatch,
            final BsonDocument commandCursorDocument) {
        CommandCursorResult<T> commandCursorResult = new CommandCursorResult<>(serverAddress, fieldNameContainingBatch,
                commandCursorDocument);
        this.nextBatch = commandCursorResult.getResults().isEmpty() ? null : commandCursorResult.getResults();
        this.count += commandCursorResult.getResults().size();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Received batch of %d documents with cursorId %d from server %s", commandCursorResult.getResults().size(),
                    commandCursorResult.getCursorId(), commandCursorResult.getServerAddress()));
        }
        return commandCursorResult;
    }

    private boolean limitReached() {
        return Math.abs(limit) != 0 && count >= Math.abs(limit);
    }

    @ThreadSafe
    private static final class ResourceManager extends CursorResourceManager<ConnectionSource, Connection> {

        ResourceManager(
                final MongoNamespace namespace,
                final ConnectionSource connectionSource,
                @Nullable final Connection connectionToPin,
                @Nullable final ServerCursor serverCursor) {
            super(new StampedLock().asWriteLock(), namespace, connectionSource, connectionToPin, serverCursor);
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

        @Override
        void markAsPinned(final Connection connectionToPin, final Connection.PinningMode pinningMode) {
            connectionToPin.markAsPinned(pinningMode);
        }

        @Override
        void doClose() {
            if (isSkipReleasingServerResourcesOnClose()) {
                unsetServerCursor();
            }
            try {
                if (getServerCursor() != null) {
                    // Don't handle corrupted connections
                    Connection connection = getConnection();
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
                unsetServerCursor();
                releaseClientResources();
            }
        }

        void executeWithConnection(final Consumer<Connection> action) {
            Connection connection = getConnection();
            try {
                action.accept(connection);
            } catch (MongoSocketException e) {
                onCorruptedConnection(connection, e);
                throw e;
            } finally {
                connection.release();
            }
        }

        private Connection getConnection() {
            assertTrue(getState() != State.IDLE);
            Connection pinnedConnection = getPinnedConnection();
            if (pinnedConnection == null) {
                return assertNotNull(getConnectionSource()).getConnection();
            } else {
                return pinnedConnection.retain();
            }
        }

        private void releaseServerAndClientResources(final Connection connection) {
            try {
                releaseServerResources(assertNotNull(connection));
            } finally {
                releaseClientResources();
            }
        }

        private void releaseServerResources(final Connection connection) {
            try {
                ServerCursor localServerCursor = getServerCursor();
                if (localServerCursor != null) {
                    killServerCursor(getNamespace(), localServerCursor, assertNotNull(connection));
                }
            } finally {
                unsetServerCursor();
            }
        }

        private void killServerCursor(final MongoNamespace namespace, final ServerCursor localServerCursor,
                final Connection localConnection) {
            OperationContext operationContext = assertNotNull(getConnectionSource()).getOperationContext();
            localConnection.command(namespace.getDatabaseName(), getKillCursorsCommand(namespace, localServerCursor),
                    NO_OP_FIELD_NAME_VALIDATOR, ReadPreference.primary(), new BsonDocumentCodec(),
                    operationContext);
        }
    }
}
