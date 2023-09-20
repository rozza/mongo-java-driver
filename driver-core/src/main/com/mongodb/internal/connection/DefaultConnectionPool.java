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

package com.mongodb.internal.connection;

import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoServerUnavailableException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.RequestContext;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.event.ConnectionCheckOutFailedEvent;
import com.mongodb.event.ConnectionCheckOutFailedEvent.Reason;
import com.mongodb.event.ConnectionCheckOutStartedEvent;
import com.mongodb.event.ConnectionCheckedInEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionClosedEvent;
import com.mongodb.event.ConnectionCreatedEvent;
import com.mongodb.event.ConnectionPoolClearedEvent;
import com.mongodb.event.ConnectionPoolClosedEvent;
import com.mongodb.event.ConnectionPoolCreatedEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ConnectionPoolReadyEvent;
import com.mongodb.event.ConnectionReadyEvent;
import com.mongodb.internal.time.TimePoint;
import com.mongodb.internal.time.Timeout;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.connection.SdamServerDescriptionManager.SdamIssue;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.event.EventReasonMessageResolver;
import com.mongodb.internal.inject.OptionalProvider;
import com.mongodb.internal.logging.LogMessage;
import com.mongodb.internal.logging.StructuredLogger;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.thread.DaemonThreadFactory;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import org.bson.ByteBuf;
import org.bson.codecs.Decoder;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.fail;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.event.ConnectionClosedEvent.Reason.ERROR;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.connection.ConcurrentPool.INFINITE_SIZE;
import static com.mongodb.internal.connection.ConcurrentPool.lockInterruptibly;
import static com.mongodb.internal.connection.ConcurrentPool.lockUnfair;
import static com.mongodb.internal.connection.ConcurrentPool.sizeToString;
import static com.mongodb.internal.event.EventListenerHelper.getConnectionPoolListener;
import static com.mongodb.internal.logging.LogMessage.Component.CONNECTION;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.DRIVER_CONNECTION_ID;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.ERROR_DESCRIPTION;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.MAX_CONNECTING;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.MAX_IDLE_TIME_MS;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.MAX_POOL_SIZE;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.MIN_POOL_SIZE;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.REASON_DESCRIPTION;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.SERVER_HOST;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.SERVER_PORT;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.SERVICE_ID;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.WAIT_QUEUE_TIMEOUT_MS;
import static com.mongodb.internal.logging.LogMessage.Level.DEBUG;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@SuppressWarnings("deprecation")
@ThreadSafe
final class DefaultConnectionPool implements ConnectionPool {
    private static final Logger LOGGER = Loggers.getLogger("connection");
    private static final StructuredLogger STRUCTURED_LOGGER = new StructuredLogger("connection");
    private final ConcurrentPool<UsageTrackingInternalConnection> pool;
    private final ConnectionPoolSettings settings;
    private final BackgroundMaintenanceManager backgroundMaintenance;
    private final AsyncWorkManager asyncWorkManager;
    private final ConnectionPoolListener connectionPoolListener;
    private final ServerId serverId;
    private final PinnedStatsManager pinnedStatsManager = new PinnedStatsManager();
    private final ServiceStateManager serviceStateManager = new ServiceStateManager();
    private final ConnectionGenerationSupplier connectionGenerationSupplier;
    private final OpenConcurrencyLimiter openConcurrencyLimiter;
    private final StateAndGeneration stateAndGeneration;
    private final OptionalProvider<SdamServerDescriptionManager> sdamProvider;

    @VisibleForTesting(otherwise = PRIVATE)
    DefaultConnectionPool(final ServerId serverId, final InternalConnectionFactory internalConnectionFactory,
            final ConnectionPoolSettings settings, final OptionalProvider<SdamServerDescriptionManager> sdamProvider) {
        this(serverId, internalConnectionFactory, settings, InternalConnectionPoolSettings.builder().build(), sdamProvider);
    }

    /**
     * @param sdamProvider For handling exceptions via the
     *                     <a href="https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst">
     *                     SDAM</a> machinery as specified
     *                     <a href="https://github.com/mongodb/specifications/blob/master/source/connection-monitoring-and-pooling/connection-monitoring-and-pooling.rst#populating-the-pool-with-a-connection-internal-implementation">
     *                     here</a>.
     *                     Must provide an {@linkplain Optional#isPresent() empty} {@link Optional} if created in load-balanced mode,
     *                     otherwise must provide a non-empty {@link Optional}.
     */
    DefaultConnectionPool(final ServerId serverId, final InternalConnectionFactory internalConnectionFactory,
            final ConnectionPoolSettings settings, final InternalConnectionPoolSettings internalSettings,
            final OptionalProvider<SdamServerDescriptionManager> sdamProvider) {
        this.serverId = notNull("serverId", serverId);
        this.settings = notNull("settings", settings);
        UsageTrackingInternalConnectionItemFactory connectionItemFactory =
                new UsageTrackingInternalConnectionItemFactory(internalConnectionFactory);
        pool = new ConcurrentPool<>(maxSize(settings), connectionItemFactory, format("The server at %s is no longer available",
                serverId.getAddress()));
        this.sdamProvider = assertNotNull(sdamProvider);
        this.connectionPoolListener = getConnectionPoolListener(settings);
        backgroundMaintenance = new BackgroundMaintenanceManager();
        connectionPoolCreated(connectionPoolListener, serverId, settings);
        openConcurrencyLimiter = new OpenConcurrencyLimiter(settings.getMaxConnecting());
        asyncWorkManager = new AsyncWorkManager(internalSettings.isPrestartAsyncWorkManager());
        stateAndGeneration = new StateAndGeneration();
        connectionGenerationSupplier = new ConnectionGenerationSupplier() {
            @Override
            public int getGeneration() {
                return stateAndGeneration.generation();
            }

            @Override
            public int getGeneration(@NonNull final ObjectId serviceId) {
                return serviceStateManager.getGeneration(serviceId);
            }
        };
    }

    @Override
    public InternalConnection get(final OperationIdContext operationIdContext) {
        return get(operationIdContext, settings.getMaxWaitTime(MILLISECONDS), MILLISECONDS);
    }

    @Override
    public InternalConnection get(final OperationIdContext operationIdContext, final long timeoutValue, final TimeUnit timeUnit) {
        connectionCheckoutStarted(operationIdContext);
        Timeout timeout = Timeout.startNow(timeoutValue, timeUnit);
        try {
            stateAndGeneration.throwIfClosedOrPaused();
            PooledConnection connection = getPooledConnection(timeout);
            if (!connection.opened()) {
                connection = openConcurrencyLimiter.openOrGetAvailable(connection, timeout);
            }
            connection.checkedOutForOperation(operationIdContext);
            connectionCheckedOut(operationIdContext, connection);
            return connection;
        } catch (Exception e) {
            throw (RuntimeException) checkOutFailed(e, operationIdContext);
        }
    }

    @Override
    public void getAsync(final OperationIdContext operationIdContext, final SingleResultCallback<InternalConnection> callback) {
        connectionCheckoutStarted(operationIdContext);
        Timeout timeout = Timeout.startNow(settings.getMaxWaitTime(NANOSECONDS));
        SingleResultCallback<PooledConnection> eventSendingCallback = (connection, failure) -> {
            SingleResultCallback<InternalConnection> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
            if (failure == null) {
                connection.checkedOutForOperation(operationIdContext);
                connectionCheckedOut(operationIdContext, connection);
                errHandlingCallback.onResult(connection, null);
            } else {
                errHandlingCallback.onResult(null, checkOutFailed(failure, operationIdContext));
            }
        };
        try {
            stateAndGeneration.throwIfClosedOrPaused();
        } catch (Exception e) {
            eventSendingCallback.onResult(null, e);
            return;
        }
        asyncWorkManager.enqueue(new Task(timeout, t -> {
            if (t != null) {
                eventSendingCallback.onResult(null, t);
            } else {
                PooledConnection connection;
                try {
                    connection = getPooledConnection(timeout);
                } catch (Exception e) {
                    eventSendingCallback.onResult(null, e);
                    return;
                }
                if (connection.opened()) {
                    eventSendingCallback.onResult(connection, null);
                } else {
                    openConcurrencyLimiter.openAsyncWithConcurrencyLimit(connection, timeout, eventSendingCallback);
                }
            }
        }));
    }

    /**
     * Sends {@link ConnectionCheckOutFailedEvent}
     * and returns {@code t} if it is not {@link MongoOpenConnectionInternalException},
     * or returns {@code t.}{@linkplain MongoOpenConnectionInternalException#getCause() getCause()} otherwise.
     */
    private Throwable checkOutFailed(final Throwable t, final OperationIdContext operationIdContext) {
        Throwable result = t;
        Reason reason;
        if (t instanceof MongoTimeoutException) {
            reason = Reason.TIMEOUT;
        } else if (t instanceof MongoOpenConnectionInternalException) {
            reason = Reason.CONNECTION_ERROR;
            result = t.getCause();
        } else if (t instanceof MongoConnectionPoolClearedException) {
            reason = Reason.CONNECTION_ERROR;
        } else if (ConcurrentPool.isPoolClosedException(t)) {
            reason = Reason.POOL_CLOSED;
        } else {
            reason = Reason.UNKNOWN;
        }

        ClusterId clusterId = serverId.getClusterId();
        if (requiresLogging(clusterId)) {
            String message = "Checkout failed for connection to {}:{}. Reason: {}.[ Error: {}]";
            List<LogMessage.Entry> entries = createBasicEntries();
            entries.add(new LogMessage.Entry(REASON_DESCRIPTION, EventReasonMessageResolver.getMessage(reason)));
            entries.add(new LogMessage.Entry(ERROR_DESCRIPTION, reason == Reason.CONNECTION_ERROR ? result.toString() : null));
            logMessage("Connection checkout failed", clusterId, message, entries);
        }
        connectionPoolListener.connectionCheckOutFailed(new ConnectionCheckOutFailedEvent(serverId, operationIdContext.getId(), reason));
        return result;
    }

    @Override
    public void invalidate(@Nullable final Throwable cause) {
        assertFalse(isLoadBalanced());
        if (stateAndGeneration.pauseAndIncrementGeneration(cause)) {
            openConcurrencyLimiter.signalClosedOrPaused();
        }
    }

    @Override
    public void ready() {
        stateAndGeneration.ready();
    }

    public void invalidate(final ObjectId serviceId, final int generation) {
        assertTrue(isLoadBalanced());
        if (generation == InternalConnection.NOT_INITIALIZED_GENERATION) {
            return;
        }
        if (serviceStateManager.incrementGeneration(serviceId, generation)) {
            ClusterId clusterId = serverId.getClusterId();
            if (requiresLogging(clusterId)) {
                String message = "Connection pool for {}:{} cleared for serviceId {}";
                List<LogMessage.Entry> entries = createBasicEntries();
                entries.add(new LogMessage.Entry(SERVICE_ID, serviceId.toHexString()));
                logMessage("Connection pool cleared", clusterId, message, entries);
            }
            connectionPoolListener.connectionPoolCleared(new ConnectionPoolClearedEvent(this.serverId, serviceId));
        }
    }

    @Override
    public void close() {
        if (stateAndGeneration.close()) {
            pool.close();
            backgroundMaintenance.close();
            asyncWorkManager.close();
            openConcurrencyLimiter.signalClosedOrPaused();
            logEventMessage("Connection pool closed", "Connection pool closed for {}:{}");

            connectionPoolListener.connectionPoolClosed(new ConnectionPoolClosedEvent(serverId));
        }
    }

    @Override
    public int getGeneration() {
        return stateAndGeneration.generation();
    }

    private PooledConnection getPooledConnection(final Timeout timeout) throws MongoTimeoutException {
        try {
            UsageTrackingInternalConnection internalConnection = pool.get(timeout.remainingOrInfinite(NANOSECONDS), NANOSECONDS);
            while (shouldPrune(internalConnection)) {
                pool.release(internalConnection, true);
                internalConnection = pool.get(timeout.remainingOrInfinite(NANOSECONDS), NANOSECONDS);
            }
            return new PooledConnection(internalConnection);
        } catch (MongoTimeoutException e) {
            throw createTimeoutException(timeout);
        }
    }

    @Nullable
    private PooledConnection getPooledConnectionImmediateUnfair() {
        UsageTrackingInternalConnection internalConnection = pool.getImmediateUnfair();
        while (internalConnection != null && shouldPrune(internalConnection)) {
            pool.release(internalConnection, true);
            internalConnection = pool.getImmediateUnfair();
        }
        return internalConnection == null ? null : new PooledConnection(internalConnection);
    }

    private MongoTimeoutException createTimeoutException(final Timeout timeout) {
        int numPinnedToCursor = pinnedStatsManager.getNumPinnedToCursor();
        int numPinnedToTransaction = pinnedStatsManager.getNumPinnedToTransaction();
        if (numPinnedToCursor == 0 && numPinnedToTransaction == 0) {
            return new MongoTimeoutException(format("Timed out after %s while waiting for a connection to server %s.",
                    timeout.toUserString(), serverId.getAddress()));
        } else {
            int maxSize = pool.getMaxSize();
            int numInUse = pool.getInUseCount();
            /* At this point in an execution we consider at least one of `numPinnedToCursor`, `numPinnedToTransaction` to be positive.
             * `numPinnedToCursor`, `numPinnedToTransaction` and `numInUse` are not a snapshot view,
             * but we still must maintain the following invariants:
             * - numInUse > 0
             *     we consider at least one of `numPinnedToCursor`, `numPinnedToTransaction` to be positive,
             *     so if we observe `numInUse` to be 0, we have to estimate it based on `numPinnedToCursor` and `numPinnedToTransaction`;
             * - numInUse < maxSize
             *     `numInUse` must not exceed the limit in situations when we estimate `numInUse`;
             * - numPinnedToCursor + numPinnedToTransaction <= numInUse
             *     otherwise the numbers do not make sense.
             */
            if (numInUse == 0) {
                numInUse = Math.min(
                        numPinnedToCursor + numPinnedToTransaction, // must be at least a big as this sum but not bigger than `maxSize`
                        maxSize);
            }
            numPinnedToCursor = Math.min(
                    numPinnedToCursor, // prefer the observed value, but it must not be bigger than `numInUse`
                    numInUse);
            numPinnedToTransaction = Math.min(
                    numPinnedToTransaction, // prefer the observed value, but it must not be bigger than `numInUse` - `numPinnedToCursor`
                    numInUse - numPinnedToCursor);
            int numOtherInUse = numInUse - numPinnedToCursor - numPinnedToTransaction;
            assertTrue(numOtherInUse >= 0);
            assertTrue(numPinnedToCursor + numPinnedToTransaction + numOtherInUse <= maxSize);
            return new MongoTimeoutException(format("Timed out after %s while waiting for a connection to server %s. Details: "
                            + "maxPoolSize: %s, connections in use by cursors: %d, connections in use by transactions: %d, "
                            + "connections in use by other operations: %d",
                    timeout.toUserString(), serverId.getAddress(),
                    sizeToString(maxSize), numPinnedToCursor, numPinnedToTransaction,
                    numOtherInUse));
        }
    }

    @VisibleForTesting(otherwise = PRIVATE)
    ConcurrentPool<UsageTrackingInternalConnection> getPool() {
        return pool;
    }

    /**
     * Synchronously prune idle connections and ensure the minimum pool size.
     */
    @VisibleForTesting(otherwise = PRIVATE)
    void doMaintenance() {
        Predicate<Exception> silentlyComplete = e ->
                e instanceof MongoInterruptedException || e instanceof MongoTimeoutException
                        || e instanceof MongoConnectionPoolClearedException || ConcurrentPool.isPoolClosedException(e);
        try {
            pool.prune();
            if (shouldEnsureMinSize()) {
                pool.ensureMinSize(settings.getMinSize(), newConnection -> {
                    try {
                        openConcurrencyLimiter.openImmediatelyAndTryHandOverOrRelease(new PooledConnection(newConnection));
                    } catch (MongoException | MongoOpenConnectionInternalException e) {
                        RuntimeException actualException = e instanceof MongoOpenConnectionInternalException
                                ? (RuntimeException) e.getCause()
                                : e;
                        sdamProvider.optional().ifPresent(sdam -> {
                            if (!silentlyComplete.test(actualException)) {
                                sdam.handleExceptionBeforeHandshake(SdamIssue.specific(actualException, sdam.context(newConnection)));
                            }
                        });
                        throw actualException;
                    }
                });
            }
        } catch (Exception e) {
            if (!silentlyComplete.test(e)) {
                LOGGER.warn("Exception thrown during connection pool background maintenance task", e);
                throw e;
            }
        }
    }

    private boolean shouldEnsureMinSize() {
        return settings.getMinSize() > 0;
    }

    private boolean shouldPrune(final UsageTrackingInternalConnection connection) {
        return fromPreviousGeneration(connection) || pastMaxLifeTime(connection) || pastMaxIdleTime(connection);
    }

    private boolean pastMaxIdleTime(final UsageTrackingInternalConnection connection) {
        return expired(connection.getLastUsedAt(), System.currentTimeMillis(), settings.getMaxConnectionIdleTime(MILLISECONDS));
    }

    private boolean pastMaxLifeTime(final UsageTrackingInternalConnection connection) {
        return expired(connection.getOpenedAt(), System.currentTimeMillis(), settings.getMaxConnectionLifeTime(MILLISECONDS));
    }

    private boolean fromPreviousGeneration(final UsageTrackingInternalConnection connection) {
        int generation = connection.getGeneration();
        if (generation == InternalConnection.NOT_INITIALIZED_GENERATION) {
            return false;
        }
        ObjectId serviceId = connection.getDescription().getServiceId();
        if (serviceId != null) {
            return serviceStateManager.getGeneration(serviceId) > generation;
        } else {
            return stateAndGeneration.generation() > generation;
        }
    }

    private boolean expired(final long startTime, final long curTime, final long maxTime) {
        return maxTime != 0 && curTime - startTime > maxTime;
    }

    /**
     * Send both current and deprecated events in order to preserve backwards compatibility.
     * Must not throw {@link Exception}s.
     */
    private void connectionPoolCreated(final ConnectionPoolListener connectionPoolListener, final ServerId serverId,
                                             final ConnectionPoolSettings settings) {
        ClusterId clusterId = serverId.getClusterId();
        if (requiresLogging(clusterId)) {
            String message = "Connection pool created for {}:{} using options maxIdleTimeMS={}, minPoolSize={}, "
                    + "maxPoolSize={}, maxConnecting={}, waitQueueTimeoutMS={}";

            List<LogMessage.Entry> entries = createBasicEntries();
            entries.add(new LogMessage.Entry(MAX_IDLE_TIME_MS, settings.getMaxConnectionIdleTime(MILLISECONDS)));
            entries.add(new LogMessage.Entry(MIN_POOL_SIZE, settings.getMinSize()));
            entries.add(new LogMessage.Entry(MAX_POOL_SIZE, settings.getMaxSize()));
            entries.add(new LogMessage.Entry(MAX_CONNECTING, settings.getMaxConnecting()));
            entries.add(new LogMessage.Entry(WAIT_QUEUE_TIMEOUT_MS, settings.getMaxWaitTime(MILLISECONDS)));

            logMessage("Connection pool created", clusterId, message, entries);
        }
        connectionPoolListener.connectionPoolCreated(new ConnectionPoolCreatedEvent(serverId, settings));
        connectionPoolListener.connectionPoolOpened(new com.mongodb.event.ConnectionPoolOpenedEvent(serverId, settings));
    }

    /**
     * Send both current and deprecated events in order to preserve backwards compatibility.
     * Must not throw {@link Exception}s.
     */
    private void connectionCreated(final ConnectionPoolListener connectionPoolListener, final ConnectionId connectionId) {
        logEventMessage("Connection created",
                "Connection created: address={}:{}, driver-generated ID={}",
                connectionId.getLocalValue());

        connectionPoolListener.connectionAdded(new com.mongodb.event.ConnectionAddedEvent(connectionId));
        connectionPoolListener.connectionCreated(new ConnectionCreatedEvent(connectionId));
    }

    /**
     * Send both current and deprecated events in order to preserve backwards compatibility.
     * Must not throw {@link Exception}s.
     */
    private void connectionClosed(final ConnectionPoolListener connectionPoolListener, final ConnectionId connectionId,
                                  final ConnectionClosedEvent.Reason reason) {
        ClusterId clusterId = serverId.getClusterId();
        if (requiresLogging(clusterId)) {
            String errorReason = "There was a socket exception raised by this connection";

            List<LogMessage.Entry> entries = createBasicEntries();
            entries.add(new LogMessage.Entry(DRIVER_CONNECTION_ID, connectionId.getLocalValue()));
            entries.add(new LogMessage.Entry(REASON_DESCRIPTION, EventReasonMessageResolver.getMessage(reason)));
            entries.add(new LogMessage.Entry(ERROR_DESCRIPTION, reason == ERROR ? errorReason : null));

            logMessage("Connection closed",
                    clusterId,
                    "Connection closed: address={}:{}, driver-generated ID={}. Reason: {}.[ Error: {}]",
                    entries);
        }
        connectionPoolListener.connectionRemoved(new com.mongodb.event.ConnectionRemovedEvent(connectionId, getReasonForRemoved(reason)));
        connectionPoolListener.connectionClosed(new ConnectionClosedEvent(connectionId, reason));
    }

    private void connectionCheckedOut(final OperationIdContext operationIdContext, final PooledConnection connection) {
        ConnectionId connectionId = getId(connection);
        logEventMessage("Connection checked out", "Connection checked out: address={}:{}, driver-generated ID={}", connectionId.getLocalValue());

        connectionPoolListener.connectionCheckedOut(new ConnectionCheckedOutEvent(connectionId, operationIdContext.getId()));
    }
    private void connectionCheckoutStarted(final OperationIdContext operationIdContext) {
        logEventMessage("Connection checkout started", "Checkout started for connection to {}:{}");

        connectionPoolListener.connectionCheckOutStarted(new ConnectionCheckOutStartedEvent(serverId, operationIdContext.getId()));
    }

    private com.mongodb.event.ConnectionRemovedEvent.Reason getReasonForRemoved(final ConnectionClosedEvent.Reason reason) {
        com.mongodb.event.ConnectionRemovedEvent.Reason removedReason = com.mongodb.event.ConnectionRemovedEvent.Reason.UNKNOWN;
        switch (reason) {
            case STALE:
                removedReason = com.mongodb.event.ConnectionRemovedEvent.Reason.STALE;
                break;
            case IDLE:
                removedReason = com.mongodb.event.ConnectionRemovedEvent.Reason.MAX_IDLE_TIME_EXCEEDED;
                break;
            case ERROR:
                removedReason = com.mongodb.event.ConnectionRemovedEvent.Reason.ERROR;
                break;
            case POOL_CLOSED:
                removedReason = com.mongodb.event.ConnectionRemovedEvent.Reason.POOL_CLOSED;
                break;
            default:
                break;
        }
        return removedReason;
    }

    /**
     * Must not throw {@link Exception}s.
     */
    private ConnectionId getId(final InternalConnection internalConnection) {
        return internalConnection.getDescription().getConnectionId();
    }

    private boolean isLoadBalanced() {
        return !sdamProvider.optional().isPresent();
    }

    /**
     * @return {@link ConnectionPoolSettings#getMaxSize()} if it is not 0, otherwise returns {@link ConcurrentPool#INFINITE_SIZE}.
     */
    private static int maxSize(final ConnectionPoolSettings settings) {
        return settings.getMaxSize() == 0 ? INFINITE_SIZE : settings.getMaxSize();
    }

    private class PooledConnection implements InternalConnection {
        private final UsageTrackingInternalConnection wrapped;
        private final AtomicBoolean isClosed = new AtomicBoolean();
        private Connection.PinningMode pinningMode;
        private OperationIdContext operationIdContext;

        PooledConnection(final UsageTrackingInternalConnection wrapped) {
            this.wrapped = notNull("wrapped", wrapped);
        }

        @Override
        public int getGeneration() {
            return wrapped.getGeneration();
        }

        /**
         * Associates this with the operation context and establishes the checked out start time
         */
        public void checkedOutForOperation(final OperationIdContext operationIdContext) {
            this.operationIdContext = operationIdContext;
        }

        @Override
        public void open() {
            assertFalse(isClosed.get());
            try {
                connectionCreated(connectionPoolListener, wrapped.getDescription().getConnectionId());
                wrapped.open();
            } catch (Exception e) {
                closeAndHandleOpenFailure();
                throw new MongoOpenConnectionInternalException(e);
            }
            handleOpenSuccess();
        }

        @Override
        public void openAsync(final SingleResultCallback<Void> callback) {
            assertFalse(isClosed.get());
            connectionCreated(connectionPoolListener, wrapped.getDescription().getConnectionId());
            wrapped.openAsync((nullResult, failure) -> {
                if (failure != null) {
                    closeAndHandleOpenFailure();
                    callback.onResult(null, new MongoOpenConnectionInternalException(failure));
                } else {
                    handleOpenSuccess();
                    callback.onResult(nullResult, null);
                }
            });
        }

        @Override
        public void close() {
            // All but the first call is a no-op
            if (!isClosed.getAndSet(true)) {
                unmarkAsPinned();
                connectionCheckedIn();
                if (wrapped.isClosed() || shouldPrune(wrapped)) {
                    pool.release(wrapped, true);
                } else {
                    openConcurrencyLimiter.tryHandOverOrRelease(wrapped);
                }
            }
        }

        private void connectionCheckedIn() {
            ConnectionId connectionId = getId(wrapped);
            logEventMessage("Connection checked in",
                    "Connection checked in: address={}:{}, driver-generated ID={}",
                    connectionId.getLocalValue());

            connectionPoolListener.connectionCheckedIn(new ConnectionCheckedInEvent(connectionId, operationIdContext.getId()));
        }

        void release() {
            if (!isClosed.getAndSet(true)) {
                pool.release(wrapped);
            }
        }

        /**
         * {@linkplain ConcurrentPool#release(Object, boolean) Prune} this connection without sending a {@link ConnectionClosedEvent}.
         * This method must be used if and only if {@link ConnectionCreatedEvent} was not sent for the connection.
         * Must not throw {@link Exception}s.
         */
        void closeSilently() {
            if (!isClosed.getAndSet(true)) {
                wrapped.setCloseSilently();
                pool.release(wrapped, true);
            }
        }

        /**
         * Must not throw {@link Exception}s.
         */
        private void closeAndHandleOpenFailure() {
            if (!isClosed.getAndSet(true)) {
                if (wrapped.getDescription().getServiceId() != null) {
                    invalidate(assertNotNull(wrapped.getDescription().getServiceId()), wrapped.getGeneration());
                }
                pool.release(wrapped, true);
            }
        }

        /**
         * Must not throw {@link Exception}s.
         */
        private void handleOpenSuccess() {
            ConnectionId connectionId = getId(this);
            logEventMessage("Connection ready",
                    "Connection ready: address={}:{}, driver-generated ID={}",
                    connectionId.getLocalValue());

            connectionPoolListener.connectionReady(new ConnectionReadyEvent(connectionId));
        }

        @Override
        public boolean opened() {
            isTrue("open", !isClosed.get());
            return wrapped.opened();
        }

        @Override
        public boolean isClosed() {
            return isClosed.get() || wrapped.isClosed();
        }

        @Override
        public ByteBuf getBuffer(final int capacity) {
            return wrapped.getBuffer(capacity);
        }

        @Override
        public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
            isTrue("open", !isClosed.get());
            wrapped.sendMessage(byteBuffers, lastRequestId);
        }

        @Override
        public <T> T sendAndReceive(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext,
                final RequestContext requestContext, final OperationIdContext operationIdContext) {
            isTrue("open", !isClosed.get());
            return wrapped.sendAndReceive(message, decoder, sessionContext, requestContext, operationIdContext);
        }

        @Override
        public <T> void send(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext) {
            isTrue("open", !isClosed.get());
            wrapped.send(message, decoder, sessionContext);
        }

        @Override
        public <T> T receive(final Decoder<T> decoder, final SessionContext sessionContext) {
            isTrue("open", !isClosed.get());
            return wrapped.receive(decoder, sessionContext);
        }

        @Override
        public boolean supportsAdditionalTimeout() {
            isTrue("open", !isClosed.get());
            return wrapped.supportsAdditionalTimeout();
        }

        @Override
        public <T> T receive(final Decoder<T> decoder, final SessionContext sessionContext, final int additionalTimeout) {
            isTrue("open", !isClosed.get());
            return wrapped.receive(decoder, sessionContext, additionalTimeout);
        }

        @Override
        public boolean hasMoreToCome() {
            isTrue("open", !isClosed.get());
            return wrapped.hasMoreToCome();
        }

        @Override
        public <T> void sendAndReceiveAsync(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext,
                final RequestContext requestContext, final OperationIdContext operationIdContext, final SingleResultCallback<T> callback) {
            isTrue("open", !isClosed.get());
            wrapped.sendAndReceiveAsync(message, decoder, sessionContext, requestContext, operationIdContext, (result, t) -> callback.onResult(result, t));
        }

        @Override
        public ResponseBuffers receiveMessage(final int responseTo) {
            isTrue("open", !isClosed.get());
            return wrapped.receiveMessage(responseTo);
        }

        @Override
        public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
            isTrue("open", !isClosed.get());
            wrapped.sendMessageAsync(byteBuffers, lastRequestId, (result, t) -> callback.onResult(null, t));
        }

        @Override
        public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
            isTrue("open", !isClosed.get());
            wrapped.receiveMessageAsync(responseTo, (result, t) -> callback.onResult(result, t));
        }

        @Override
        public void markAsPinned(final Connection.PinningMode pinningMode) {
            assertNotNull(pinningMode);
            // if the connection is already pinned for some other mode, the additional mode can be ignored.
            // The typical case is the connection is first pinned for a transaction, then pinned for a cursor withing that transaction
            // In this case, the cursor pinning is subsumed by the transaction pinning.
            if (this.pinningMode == null) {
                this.pinningMode = pinningMode;
                pinnedStatsManager.increment(pinningMode);
            }
        }

        void unmarkAsPinned() {
            if (pinningMode != null) {
                pinnedStatsManager.decrement(pinningMode);
            }
        }

        @Override
        public ConnectionDescription getDescription() {
            return wrapped.getDescription();
        }

        @Override
        public ServerDescription getInitialServerDescription() {
            isTrue("open", !isClosed.get());
            return wrapped.getInitialServerDescription();
        }
    }

    /**
     * This internal exception is used to express an exceptional situation encountered when opening a connection.
     * It exists because it allows consolidating the code that sends events for exceptional situations in a
     * {@linkplain #checkOutFailed(Throwable, OperationIdContext) single place}, it must not be observable by an external code.
     */
    private static final class MongoOpenConnectionInternalException extends RuntimeException {
        private static final long serialVersionUID = 1;

        MongoOpenConnectionInternalException(@NonNull final Throwable cause) {
            super(cause);
        }

        @Override
        @NonNull
        public Throwable getCause() {
            return assertNotNull(super.getCause());
        }
    }

    private class UsageTrackingInternalConnectionItemFactory implements ConcurrentPool.ItemFactory<UsageTrackingInternalConnection> {
        private final InternalConnectionFactory internalConnectionFactory;

        UsageTrackingInternalConnectionItemFactory(final InternalConnectionFactory internalConnectionFactory) {
            this.internalConnectionFactory = internalConnectionFactory;
        }

        @Override
        public UsageTrackingInternalConnection create() {
            return new UsageTrackingInternalConnection(internalConnectionFactory.create(serverId, connectionGenerationSupplier),
                    serviceStateManager);
        }

        @Override
        public void close(final UsageTrackingInternalConnection connection) {
            if (!connection.isCloseSilently()) {
                connectionClosed(connectionPoolListener, getId(connection), getReasonForClosing(connection));
            }
            connection.close();
        }

        private ConnectionClosedEvent.Reason getReasonForClosing(final UsageTrackingInternalConnection connection) {
            ConnectionClosedEvent.Reason reason;
            if (connection.isClosed()) {
                reason = ConnectionClosedEvent.Reason.ERROR;
            } else if (fromPreviousGeneration(connection)) {
                reason = ConnectionClosedEvent.Reason.STALE;
            } else if (pastMaxIdleTime(connection)) {
                reason = ConnectionClosedEvent.Reason.IDLE;
            } else {
                reason = ConnectionClosedEvent.Reason.POOL_CLOSED;
            }
            return reason;
        }

        @Override
        public boolean shouldPrune(final UsageTrackingInternalConnection usageTrackingConnection) {
            return DefaultConnectionPool.this.shouldPrune(usageTrackingConnection);
        }
    }

    /**
     * Package-access methods are thread-safe,
     * and only they should be called outside of the {@link OpenConcurrencyLimiter}'s code.
     */
    @ThreadSafe
    private final class OpenConcurrencyLimiter {
        private final ReentrantLock lock;
        private final Condition permitAvailableOrHandedOverOrClosedOrPausedCondition;
        private final int maxPermits;
        private int permits;
        private final Deque<MutableReference<PooledConnection>> desiredConnectionSlots;

        OpenConcurrencyLimiter(final int maxConnecting) {
            lock = new ReentrantLock(true);
            permitAvailableOrHandedOverOrClosedOrPausedCondition = lock.newCondition();
            maxPermits = maxConnecting;
            permits = maxPermits;
            desiredConnectionSlots = new LinkedList<>();
        }

        PooledConnection openOrGetAvailable(final PooledConnection connection, final Timeout timeout) throws MongoTimeoutException {
            PooledConnection result = openWithConcurrencyLimit(connection, OpenWithConcurrencyLimitMode.TRY_GET_AVAILABLE, timeout);
            return assertNotNull(result);
        }

        void openImmediatelyAndTryHandOverOrRelease(final PooledConnection connection) throws MongoTimeoutException {
            assertNull(openWithConcurrencyLimit(connection, OpenWithConcurrencyLimitMode.TRY_HAND_OVER_OR_RELEASE, Timeout.immediate()));
        }

        /**
         * This method can be thought of as operating in two phases.
         * In the first phase it tries to synchronously acquire a permit to open the {@code connection}
         * or get a different {@linkplain PooledConnection#opened() opened} connection if {@code mode} is
         * {@link OpenWithConcurrencyLimitMode#TRY_GET_AVAILABLE} and one becomes available while waiting for a permit.
         * The first phase has one of the following outcomes:
         * <ol>
         *     <li>A {@link MongoTimeoutException} or a different {@link Exception} is thrown,
         *     and the specified {@code connection} is {@linkplain PooledConnection#closeSilently() silently closed}.</li>
         *     <li>An opened connection different from the specified one is returned,
         *     and the specified {@code connection} is {@linkplain PooledConnection#closeSilently() silently closed}.
         *     This outcome is possible only if {@code mode} is {@link OpenWithConcurrencyLimitMode#TRY_GET_AVAILABLE}.</li>
         *     <li>A permit is acquired, {@link #connectionCreated(ConnectionPoolListener, ConnectionId)} is reported
         *     and an attempt to open the specified {@code connection} is made. This is the second phase in which
         *     the {@code connection} is {@linkplain PooledConnection#open() opened synchronously}.
         *     The attempt to open the {@code connection} has one of the following outcomes
         *     combined with releasing the acquired permit:
         *     <ol>
         *         <li>An {@link Exception} is thrown
         *         and the {@code connection} is {@linkplain PooledConnection#closeAndHandleOpenFailure() closed}.</li>
         *         <li>Else if the specified {@code connection} is opened successfully and
         *         {@code mode} is {@link OpenWithConcurrencyLimitMode#TRY_HAND_OVER_OR_RELEASE},
         *         then {@link #tryHandOverOrRelease(UsageTrackingInternalConnection)} is called and {@code null} is returned.</li>
         *         <li>Else the specified {@code connection}, which is now opened, is returned.</li>
         *     </ol>
         *     </li>
         * </ol>
         *
         * @param timeout Applies only to the first phase.
         * @return An {@linkplain PooledConnection#opened() opened} connection which is
         * either the specified {@code connection},
         * or potentially a different one if {@code mode} is {@link OpenWithConcurrencyLimitMode#TRY_GET_AVAILABLE},
         * or {@code null} if {@code mode} is {@link OpenWithConcurrencyLimitMode#TRY_HAND_OVER_OR_RELEASE}.
         * @throws MongoTimeoutException If the first phase timed out.
         */
        @Nullable
        private PooledConnection openWithConcurrencyLimit(final PooledConnection connection, final OpenWithConcurrencyLimitMode mode,
                final Timeout timeout) throws MongoTimeoutException {
            PooledConnection availableConnection;
            try {//phase one
                availableConnection = acquirePermitOrGetAvailableOpenedConnection(
                        mode == OpenWithConcurrencyLimitMode.TRY_GET_AVAILABLE, timeout);
            } catch (Exception e) {
                connection.closeSilently();
                throw e;
            }
            if (availableConnection != null) {
                connection.closeSilently();
                return availableConnection;
            } else {//acquired a permit, phase two
                try {
                    connection.open();
                    if (mode == OpenWithConcurrencyLimitMode.TRY_HAND_OVER_OR_RELEASE) {
                        tryHandOverOrRelease(connection.wrapped);
                        return null;
                    } else {
                        return connection;
                    }
                } finally {
                    releasePermit();
                }
            }
        }

        /**
         * This method is similar to {@link #openWithConcurrencyLimit(PooledConnection, OpenWithConcurrencyLimitMode, Timeout)}
         * with the following differences:
         * <ul>
         *     <li>It does not have the {@code mode} parameter and acts as if this parameter were
         *     {@link OpenWithConcurrencyLimitMode#TRY_GET_AVAILABLE}.</li>
         *     <li>While the first phase is still synchronous, the {@code connection} is
         *     {@linkplain PooledConnection#openAsync(SingleResultCallback) opened asynchronously} in the second phase.</li>
         *     <li>Instead of returning a result or throwing an exception via Java {@code return}/{@code throw} statements,
         *     it calls {@code callback.}{@link SingleResultCallback#onResult(Object, Throwable) onResult(result, failure)}
         *     and passes either a {@link PooledConnection} or an {@link Exception}.</li>
         * </ul>
         */
        void openAsyncWithConcurrencyLimit(
                final PooledConnection connection, final Timeout timeout, final SingleResultCallback<PooledConnection> callback) {
            PooledConnection availableConnection;
            try {//phase one
                availableConnection = acquirePermitOrGetAvailableOpenedConnection(true, timeout);
            } catch (Exception e) {
                connection.closeSilently();
                callback.onResult(null, e);
                return;
            }
            if (availableConnection != null) {
                connection.closeSilently();
                callback.onResult(availableConnection, null);
            } else {//acquired a permit, phase two
                connection.openAsync((nullResult, failure) -> {
                    releasePermit();
                    if (failure != null) {
                        callback.onResult(null, failure);
                    } else {
                        callback.onResult(connection, null);
                    }
                });
            }
        }

        /**
         * @return Either {@code null} if a permit has been acquired, or a {@link PooledConnection}
         * if {@code tryGetAvailable} is {@code true} and an {@linkplain PooledConnection#opened() opened} one becomes available while
         * waiting for a permit.
         * @throws MongoTimeoutException If timed out.
         * @throws MongoInterruptedException If the current thread has its {@linkplain Thread#interrupted() interrupted status}
         * set on entry to this method or is interrupted while waiting to get an available opened connection.
         */
        @Nullable
        private PooledConnection acquirePermitOrGetAvailableOpenedConnection(final boolean tryGetAvailable, final Timeout timeout)
                throws MongoTimeoutException, MongoInterruptedException {
            PooledConnection availableConnection = null;
            boolean expressedDesireToGetAvailableConnection = false;
            lockInterruptibly(lock);
            try {
                if (tryGetAvailable) {
                    /* An attempt to get an available opened connection from the pool (must be done while holding the lock)
                     * happens here at most once to prevent the race condition in the following execution
                     * (actions are specified in the execution total order,
                     * which by definition exists if an execution is either sequentially consistent or linearizable):
                     * 1. Thread#1 starts checking out and gets a non-opened connection.
                     * 2. Thread#2 checks in a connection. Tries to hand it over, but there are no threads desiring to get one.
                     * 3. Thread#1 executes the current code. Expresses the desire to get a connection via the hand-over mechanism,
                     *   but thread#2 has already tried handing over and released its connection to the pool.
                     * As a result, thread#1 is waiting for a permit to open a connection despite one being available in the pool.
                     *
                     * This attempt should be unfair because the current thread (Thread#1) has already waited for its turn fairly.
                     * Waiting fairly again puts the current thread behind other threads, which is unfair to the current thread. */
                    availableConnection = getPooledConnectionImmediateUnfair();
                    if (availableConnection != null) {
                        return availableConnection;
                    }
                    expressDesireToGetAvailableConnection();
                    expressedDesireToGetAvailableConnection = true;
                }
                long remainingNanos = timeout.remainingOrInfinite(NANOSECONDS);
                while (permits == 0
                        // the absence of short-circuiting is of importance
                        & !stateAndGeneration.throwIfClosedOrPaused()
                        & (availableConnection = tryGetAvailable ? tryGetAvailableConnection() : null) == null) {
                    if (Timeout.expired(remainingNanos)) {
                        throw createTimeoutException(timeout);
                    }
                    remainingNanos = awaitNanos(permitAvailableOrHandedOverOrClosedOrPausedCondition, remainingNanos);
                }
                if (availableConnection == null) {
                    assertTrue(permits > 0);
                    permits--;
                }
                return availableConnection;
            } finally {
                try {
                    if (expressedDesireToGetAvailableConnection && availableConnection == null) {
                        giveUpOnTryingToGetAvailableConnection();
                    }
                } finally {
                    lock.unlock();
                }
            }
        }

        private void releasePermit() {
            lockUnfair(lock);
            try {
                assertTrue(permits < maxPermits);
                permits++;
                permitAvailableOrHandedOverOrClosedOrPausedCondition.signal();
            } finally {
                lock.unlock();
            }
        }

        private void expressDesireToGetAvailableConnection() {
            desiredConnectionSlots.addLast(new MutableReference<>());
        }

        @Nullable
        private PooledConnection tryGetAvailableConnection() {
            assertFalse(desiredConnectionSlots.isEmpty());
            PooledConnection result = desiredConnectionSlots.peekFirst().reference;
            if (result != null) {
                desiredConnectionSlots.removeFirst();
                assertTrue(result.opened());
            }
            return result;
        }

        private void giveUpOnTryingToGetAvailableConnection() {
            assertFalse(desiredConnectionSlots.isEmpty());
            PooledConnection connection = desiredConnectionSlots.removeLast().reference;
            if (connection != null) {
                connection.release();
            }
        }

        /**
         * The hand-over mechanism is needed to prevent other threads doing checkout from stealing newly released connections
         * from threads that are waiting for a permit to open a connection.
         */
        void tryHandOverOrRelease(final UsageTrackingInternalConnection openConnection) {
            lockUnfair(lock);
            try {
                for (//iterate from first (head) to last (tail)
                        MutableReference<PooledConnection> desiredConnectionSlot : desiredConnectionSlots) {
                    if (desiredConnectionSlot.reference == null) {
                        desiredConnectionSlot.reference = new PooledConnection(openConnection);
                        permitAvailableOrHandedOverOrClosedOrPausedCondition.signal();
                        return;
                    }
                }
            } finally {
                lock.unlock();
            }
            pool.release(openConnection);
        }

        void signalClosedOrPaused() {
            lockUnfair(lock);
            try {
                permitAvailableOrHandedOverOrClosedOrPausedCondition.signalAll();
            } finally {
                lock.unlock();
            }
        }

        /**
         * @param timeoutNanos See {@link Timeout#started(long, TimePoint)}.
         * @return The remaining duration as per {@link Timeout#remainingOrInfinite(TimeUnit)} if waiting ended early either
         * spuriously or because of receiving a signal.
         */
        private long awaitNanos(final Condition condition, final long timeoutNanos) throws MongoInterruptedException {
            try {
                if (timeoutNanos < 0 || timeoutNanos == Long.MAX_VALUE) {
                    condition.await();
                    return -1;
                } else {
                    return Math.max(0, condition.awaitNanos(timeoutNanos));
                }
            } catch (InterruptedException e) {
                throw new MongoInterruptedException(null, e);
            }
        }
    }

    /**
     * @see OpenConcurrencyLimiter#openWithConcurrencyLimit(PooledConnection, OpenWithConcurrencyLimitMode, Timeout)
     */
    private enum OpenWithConcurrencyLimitMode {
        TRY_GET_AVAILABLE,
        TRY_HAND_OVER_OR_RELEASE
    }

    @NotThreadSafe
    private static final class MutableReference<T> {
        @Nullable
        private T reference;

        private MutableReference() {
        }
    }

    @ThreadSafe
    static final class ServiceStateManager {
        private final ConcurrentHashMap<ObjectId, ServiceState> stateByServiceId = new ConcurrentHashMap<>();

        void addConnection(final ObjectId serviceId) {
            stateByServiceId.compute(serviceId, (k, v) -> {
                if (v == null) {
                    v = new ServiceState();
                }
                v.incrementConnectionCount();
                return v;
            });
        }

        /**
         * Removes the mapping from {@code serviceId} to a {@link ServiceState} if its connection count reaches 0.
         * This is done to prevent memory leaks.
         * <p>
         * This method must be called once for any connection for which {@link #addConnection(ObjectId)} was called.
         */
        void removeConnection(final ObjectId serviceId) {
            stateByServiceId.compute(serviceId, (k, v) -> {
                assertNotNull(v);
                return v.decrementAndGetConnectionCount() == 0 ? null : v;
            });
        }

        /**
         * In some cases we may increment the generation even for an unregistered serviceId, as when open fails on the only connection to
         * a given serviceId. In this case this method does not track the generation increment but does return true.
         *
         * @return true if the generation was incremented
         */
        boolean incrementGeneration(final ObjectId serviceId, final int expectedGeneration) {
            ServiceState state = stateByServiceId.get(serviceId);
            return state == null || state.incrementGeneration(expectedGeneration);
        }

        int getGeneration(final ObjectId serviceId) {
            ServiceState state = stateByServiceId.get(serviceId);
            return state == null ? 0 : state.getGeneration();
        }

        private static final class ServiceState {
            private final AtomicInteger generation = new AtomicInteger();
            private final AtomicInteger connectionCount = new AtomicInteger();

            void incrementConnectionCount() {
                connectionCount.incrementAndGet();
            }

            int decrementAndGetConnectionCount() {
                return connectionCount.decrementAndGet();
            }

            boolean incrementGeneration(final int expectedGeneration) {
                return generation.compareAndSet(expectedGeneration, expectedGeneration + 1);
            }

            public int getGeneration() {
                return generation.get();
            }
        }
    }

    private static final class PinnedStatsManager {
        private final LongAdder numPinnedToCursor = new LongAdder();
        private final LongAdder numPinnedToTransaction = new LongAdder();

        void increment(final Connection.PinningMode pinningMode) {
            switch (pinningMode) {
                case CURSOR:
                    numPinnedToCursor.increment();
                    break;
                case TRANSACTION:
                    numPinnedToTransaction.increment();
                    break;
                default:
                    fail();
            }
        }

        void decrement(final Connection.PinningMode pinningMode) {
            switch (pinningMode) {
                case CURSOR:
                    numPinnedToCursor.decrement();
                    break;
                case TRANSACTION:
                    numPinnedToTransaction.decrement();
                    break;
                default:
                    fail();
            }
        }

        int getNumPinnedToCursor() {
            return numPinnedToCursor.intValue();
        }

        int getNumPinnedToTransaction() {
            return numPinnedToTransaction.intValue();
        }
    }

    /**
     * This class maintains threads needed to perform {@link ConnectionPool#getAsync(OperationIdContext, SingleResultCallback)}.
     */
    @ThreadSafe
    private static class AsyncWorkManager implements AutoCloseable {
        private volatile State state;
        private volatile BlockingQueue<Task> tasks;
        private final Lock lock;
        @Nullable
        private ExecutorService worker;

        AsyncWorkManager(final boolean prestart) {
            state = State.NEW;
            tasks = new LinkedBlockingQueue<>();
            lock = new StampedLock().asWriteLock();
            if (prestart) {
                assertTrue(initUnlessClosed());
            }
        }

        void enqueue(final Task task) {
            lock.lock();
            try {
                if (initUnlessClosed()) {
                    tasks.add(task);
                    return;
                }
            } finally {
                lock.unlock();
            }
            task.failAsClosed();
        }

        /**
         * Invocations of this method must be guarded by {@link #lock}, unless done from the constructor.
         *
         * @return {@code false} iff the {@link #state} is {@link State#CLOSED}.
         */
        private boolean initUnlessClosed() {
            boolean result = true;
            if (state == State.NEW) {
                worker = Executors.newSingleThreadExecutor(new DaemonThreadFactory("AsyncGetter"));
                worker.submit(() -> runAndLogUncaught(this::workerRun));
                state = State.INITIALIZED;
            } else if (state == State.CLOSED) {
                result = false;
            }
            return result;
        }

        /**
         * {@linkplain Thread#interrupt() Interrupts} all workers and causes queued tasks to
         * {@linkplain Task#failAsClosed() fail} asynchronously.
         */
        @Override
        @SuppressWarnings("try")
        public void close() {
            lock.lock();
            try {
                if (state != State.CLOSED) {
                    state = State.CLOSED;
                    if (worker != null) {
                        worker.shutdownNow(); // at this point we interrupt `worker`s thread
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        private void workerRun() {
            while (state != State.CLOSED) {
                try {
                    Task task = tasks.take();
                    if (task.timeout().expired()) {
                        task.failAsTimedOut();
                    } else {
                        task.execute();
                    }
                } catch (InterruptedException closed) {
                    // fail the rest of the tasks and stop
                } catch (Exception e) {
                    LOGGER.error(null, e);
                }
            }
            failAllTasksAfterClosing();
        }

        private void failAllTasksAfterClosing() {
            Queue<Task> localGets;
            lock.lock();
            try {
                assertTrue(state == State.CLOSED);
                // at this point it is guaranteed that no thread enqueues a task
                localGets = tasks;
                if (!tasks.isEmpty()) {
                    tasks = new LinkedBlockingQueue<>();
                }
            } finally {
                lock.unlock();
            }
            localGets.forEach(Task::failAsClosed);
            localGets.clear();
        }

        private void runAndLogUncaught(final Runnable runnable) {
            try {
                runnable.run();
            } catch (Throwable t) {
                LOGGER.error("The pool is not going to work correctly from now on. You may want to recreate the MongoClient", t);
                throw t;
            }
        }

        private enum State {
            NEW,
            INITIALIZED,
            CLOSED
        }
    }

    /**
     * An action that is allowed to be completed (failed or executed) at most once, and a timeout associated with it.
     */
    @NotThreadSafe
    final class Task {
        private final Timeout timeout;
        private final Consumer<RuntimeException> action;
        private boolean completed;

        Task(final Timeout timeout, final Consumer<RuntimeException> action) {
            this.timeout = timeout;
            this.action = action;
        }

        void execute() {
            doComplete(() -> null);
        }

        void failAsClosed() {
            doComplete(pool::poolClosedException);
        }

        void failAsTimedOut() {
            doComplete(() -> createTimeoutException(timeout));
        }

        private void doComplete(final Supplier<RuntimeException> failureSupplier) {
            assertFalse(completed);
            completed = true;
            action.accept(failureSupplier.get());
        }

        Timeout timeout() {
            return timeout;
        }
    }

    /**
     * Methods {@link #start()} and {@link #runOnceAndStop()} must be called sequentially. Each {@link #start()} must be followed by
     * {@link #runOnceAndStop()} unless {@link BackgroundMaintenanceManager} is {@linkplain #close() closed}.
     * <p>
     * This class implements
     * <a href="https://github.com/mongodb/specifications/blob/master/source/connection-monitoring-and-pooling/connection-monitoring-and-pooling.rst#background-thread">
     * CMAP background thread</a>.
     */
    @NotThreadSafe
    private final class BackgroundMaintenanceManager implements AutoCloseable {
        @Nullable
        private final ScheduledExecutorService maintainer;
        @Nullable
        private Future<?> cancellationHandle;
        private boolean initialStart;

        private BackgroundMaintenanceManager() {
            maintainer = settings.getMaintenanceInitialDelay(NANOSECONDS) < Long.MAX_VALUE
                    ? Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("MaintenanceTimer"))
                    : null;
            cancellationHandle = null;
            initialStart = true;
        }

        void start() {
            if (maintainer != null) {
                assertTrue(cancellationHandle == null);
                cancellationHandle = ignoreRejectedExectution(() -> maintainer.scheduleAtFixedRate(
                        DefaultConnectionPool.this::doMaintenance,
                        initialStart ? settings.getMaintenanceInitialDelay(MILLISECONDS) : 0,
                        settings.getMaintenanceFrequency(MILLISECONDS), MILLISECONDS));
                initialStart = false;
            }
        }

        void runOnceAndStop() {
            if (maintainer != null) {
                if (cancellationHandle != null) {
                    cancellationHandle.cancel(false);
                    cancellationHandle = null;
                }
                ignoreRejectedExectution(() -> maintainer.execute(DefaultConnectionPool.this::doMaintenance));
            }
        }

        @Override
        public void close() {
            if (maintainer != null) {
                maintainer.shutdownNow();
            }
        }

        private void ignoreRejectedExectution(final Runnable action) {
            ignoreRejectedExectution(() -> {
                action.run();
                return null;
            });
        }

        @Nullable
        private <T> T ignoreRejectedExectution(final Supplier<T> action) {
            try {
                return action.get();
            } catch (RejectedExecutionException ignored) {
                // `close` either completed or is in progress
                return null;
            }
        }
    }

    @ThreadSafe
    private final class StateAndGeneration {
        private final ReadWriteLock lock;
        private volatile boolean paused;
        private final AtomicBoolean closed;
        private volatile int generation;
        @Nullable
        private Throwable cause;

        StateAndGeneration() {
            lock = new StampedLock().asReadWriteLock();
            paused = true;
            closed = new AtomicBoolean();
            generation = 0;
            cause = null;
        }

        int generation() {
            return generation;
        }

        /**
         * @return {@code true} if and only if the state changed from ready to paused as a result of the operation.
         * The generation is incremented regardless of the returned value.
         */
        boolean pauseAndIncrementGeneration(@Nullable final Throwable cause) {
            boolean result = false;
            lock.writeLock().lock();
            try {
                if (!paused) {
                    paused = true;
                    pool.pause(() -> new MongoConnectionPoolClearedException(serverId, cause));
                    result = true;
                }
                this.cause = cause;
                //noinspection NonAtomicOperationOnVolatileField
                generation++;
                if (result) {
                    logEventMessage("Connection pool cleared", "Connection pool for {}:{} cleared");

                    connectionPoolListener.connectionPoolCleared(new ConnectionPoolClearedEvent(serverId));
                    // one additional run is required to guarantee that a paused pool releases resources
                    backgroundMaintenance.runOnceAndStop();
                }
            } finally {
                lock.writeLock().unlock();
            }
            return result;
        }

        boolean ready() {
            boolean result = false;
            if (paused) {
                lock.writeLock().lock();
                try {
                    if (paused) {
                        paused = false;
                        cause = null;
                        pool.ready();
                        logEventMessage("Connection pool ready", "Connection pool ready for {}:{}");

                        connectionPoolListener.connectionPoolReady(new ConnectionPoolReadyEvent(serverId));
                        backgroundMaintenance.start();
                        result = true;
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }
            return result;
        }

        /**
         * @return {@code true} if and only if the state changed as a result of the operation.
         */
        boolean close() {
            return closed.compareAndSet(false, true);
        }

        /**
         * @return {@code false} which means that the method did not throw.
         * The method returns to allow using it conveniently as part of a condition check when waiting on a {@link Condition}.
         * Short-circuiting operators {@code &&} and {@code ||} must not be used with this method to ensure that it is called.
         * @throws MongoServerUnavailableException If and only if {@linkplain #close() closed}.
         * @throws MongoConnectionPoolClearedException If and only if {@linkplain #pauseAndIncrementGeneration(Throwable) paused}
         * and not {@linkplain #close() closed}.
         */
        boolean throwIfClosedOrPaused() {
            if (closed.get()) {
                throw pool.poolClosedException();
            }
            if (paused) {
                lock.readLock().lock();
                try {
                    if (paused) {
                        throw new MongoConnectionPoolClearedException(serverId, cause);
                    }
                } finally {
                    lock.readLock().unlock();
                }
            }
            return false;
        }

    }
    private void logEventMessage(final String messageId, final String format, final int driverConnectionId) {
        ClusterId clusterId = serverId.getClusterId();
        if (requiresLogging(clusterId)) {
            List<LogMessage.Entry> entries = createBasicEntries();
            entries.add(new LogMessage.Entry(DRIVER_CONNECTION_ID, driverConnectionId));
            logMessage(messageId, clusterId, format, entries);
        }
    }

    private void logEventMessage(final String messageId, final String format) {
        ClusterId clusterId = serverId.getClusterId();
        if (requiresLogging(clusterId)) {
            List<LogMessage.Entry> entries = createBasicEntries();
            logMessage(messageId, clusterId, format, entries);
        }
    }

    private List<LogMessage.Entry> createBasicEntries() {
        List<LogMessage.Entry> entries = new ArrayList<>();
        entries.add(new LogMessage.Entry(SERVER_HOST, serverId.getAddress().getHost()));
        entries.add(new LogMessage.Entry(SERVER_PORT, serverId.getAddress().getPort()));
        return entries;
    }

    private static void logMessage(final String messageId, final ClusterId clusterId, final String format, final List<LogMessage.Entry> entries) {
        STRUCTURED_LOGGER.log(new LogMessage(CONNECTION, DEBUG, messageId, clusterId, entries, format));
    }

    private static boolean requiresLogging(final ClusterId clusterId) {
        return STRUCTURED_LOGGER.isRequired(DEBUG, clusterId);
    }
}
