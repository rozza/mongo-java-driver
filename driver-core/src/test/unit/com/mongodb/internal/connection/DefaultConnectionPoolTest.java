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
import com.mongodb.MongoServerUnavailableException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.connection.ServerId;
import com.mongodb.event.ConnectionCheckOutFailedEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.internal.inject.SameObjectProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ClusterFixture.OPERATION_CONTEXT_FACTORY;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.ClusterFixture.createOperationContext;
import static com.mongodb.connection.ConnectionPoolSettings.builder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class DefaultConnectionPoolTest {

    private static final ServerAddress SERVER_ADDRESS = new ServerAddress();
    private static final ServerId SERVER_ID = new ServerId(new ClusterId("test"), SERVER_ADDRESS);

    private final TestInternalConnectionFactory connectionFactory = new TestInternalConnectionFactory();
    private DefaultConnectionPool pool;

    @AfterEach
    void cleanup() {
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    void shouldGetNonNullConnection() {
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(1).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY);
        pool.ready();

        assertNotNull(pool.get(OPERATION_CONTEXT));
    }

    @Test
    void shouldReuseReleasedConnection() {
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(1).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY);
        pool.ready();

        pool.get(OPERATION_CONTEXT).close();
        pool.get(OPERATION_CONTEXT);

        // Only one connection should have been created
        assertTrue(connectionFactory.getCreatedConnections().size() <= 1);
    }

    @Test
    void shouldReleaseAConnectionBackIntoThePoolOnCloseNotCloseTheUnderlyingConnection() {
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(1).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY);
        pool.ready();

        pool.get(OPERATION_CONTEXT).close();

        assertTrue(!connectionFactory.getCreatedConnections().get(0).isClosed());
    }

    @Test
    void shouldThrowIfPoolIsExhausted() {
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(1).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY);
        pool.ready();

        InternalConnection first = pool.get(createOperationContext(TIMEOUT_SETTINGS.withMaxWaitTimeMS(50)));
        assertNotNull(first);

        assertThrows(MongoTimeoutException.class, () ->
                pool.get(createOperationContext(TIMEOUT_SETTINGS.withMaxWaitTimeMS(50))));
    }

    @Test
    void shouldThrowOnTimeout() throws InterruptedException {
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(1).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY);
        pool.ready();

        TimeoutSettings timeoutSettings = TIMEOUT_SETTINGS.withMaxWaitTimeMS(50);
        pool.get(createOperationContext(timeoutSettings));

        TimeoutTrackingConnectionGetter connectionGetter = new TimeoutTrackingConnectionGetter(pool, timeoutSettings);
        new Thread(connectionGetter).start();

        connectionGetter.getLatch().await();

        assertTrue(connectionGetter.isGotTimeout());
    }

    @Test
    void shouldHaveSizeOf0WithDefaultSettings() {
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(10).maintenanceInitialDelay(5, MINUTES).build(),
                mockSdamProvider(), OPERATION_CONTEXT_FACTORY);
        pool.ready();

        pool.doMaintenance();

        assertTrue(connectionFactory.getCreatedConnections().isEmpty());
    }

    @Tag("Slow")
    @Test
    void shouldEnsureMinPoolSizeAfterMaintenanceTaskRuns() throws InterruptedException {
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(10).minSize(5).maintenanceInitialDelay(5, MINUTES).build(),
                mockSdamProvider(), OPERATION_CONTEXT_FACTORY);
        pool.ready();

        pool.doMaintenance();
        Thread.sleep(500);

        assertTrue(connectionFactory.getCreatedConnections().size() >= 5);
        assertTrue(connectionFactory.getCreatedConnections().get(0).opened());

        pool.invalidate(null);
        pool.ready();
        pool.doMaintenance();
        Thread.sleep(500);

        assertTrue(connectionFactory.getCreatedConnections().size() >= 10);
    }

    @Test
    void shouldInvokeConnectionPoolCreatedEvent() {
        ConnectionPoolListener listener = mock(ConnectionPoolListener.class);
        ConnectionPoolSettings settings = builder().maxSize(10).minSize(5).addConnectionPoolListener(listener).build();

        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, settings, mockSdamProvider(), OPERATION_CONTEXT_FACTORY);

        verify(listener, times(1)).connectionPoolCreated(
                argThat(event -> event.getServerId().equals(SERVER_ID) && event.getSettings().equals(settings)));
    }

    @Test
    void shouldInvokeConnectionPoolClosedEvent() {
        ConnectionPoolListener listener = mock(ConnectionPoolListener.class);
        ConnectionPoolSettings settings = builder().maxSize(10).minSize(5).addConnectionPoolListener(listener).build();
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, settings, mockSdamProvider(), OPERATION_CONTEXT_FACTORY);

        pool.close();

        verify(listener, times(1)).connectionPoolClosed(
                argThat(event -> event.getServerId().equals(SERVER_ID)));
    }

    @Test
    void shouldFireConnectionCreatedToPoolEvent() {
        ConnectionPoolListener listener = mock(ConnectionPoolListener.class);
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(10)
                .addConnectionPoolListener(listener).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY);

        pool.ready();
        pool.get(OPERATION_CONTEXT);

        verify(listener, atLeast(1)).connectionCreated(
                argThat(event -> event.getConnectionId().getServerId().equals(SERVER_ID)));
        verify(listener, atLeast(1)).connectionReady(
                argThat(event -> event.getConnectionId().getServerId().equals(SERVER_ID)));
    }

    @Test
    void shouldFireConnectionRemovedFromPoolEvent() {
        ConnectionPoolListener listener = mock(ConnectionPoolListener.class);
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(10)
                .addConnectionPoolListener(listener).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY);
        pool.ready();
        InternalConnection connection = pool.get(OPERATION_CONTEXT);
        connection.close();

        pool.close();

        verify(listener, atLeast(1)).connectionClosed(
                argThat(event -> event.getConnectionId().getServerId().equals(SERVER_ID)));
    }

    @Test
    void shouldFireConnectionPoolEventsOnCheckOutAndCheckIn() {
        ConnectionPoolListener listener = mock(ConnectionPoolListener.class);
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(1)
                .addConnectionPoolListener(listener).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY);
        pool.ready();
        InternalConnection connection = pool.get(OPERATION_CONTEXT);
        connection.close();

        connection = pool.get(OPERATION_CONTEXT);

        verify(listener, atLeast(1)).connectionCheckedOut(
                argThat(event -> event.getConnectionId().getServerId().equals(SERVER_ID)));

        connection.close();

        verify(listener, atLeast(1)).connectionCheckedIn(
                argThat(event -> event.getConnectionId().getServerId().equals(SERVER_ID)));
    }

    @Test
    void shouldFireMongoConnectionPoolClearedExceptionWhenCheckingOutInPausedState() {
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().build(),
                mockSdamProvider(), OPERATION_CONTEXT_FACTORY);

        assertThrows(MongoConnectionPoolClearedException.class, () -> pool.get(OPERATION_CONTEXT));
    }

    @Test
    void shouldFireMongoConnectionPoolClearedExceptionWhenCheckingOutAsynchronouslyInPausedState() {
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().build(),
                mockSdamProvider(), OPERATION_CONTEXT_FACTORY);
        CompletableFuture<Throwable> caught = new CompletableFuture<>();

        pool.getAsync(OPERATION_CONTEXT, (result, t) -> {
            if (t != null) {
                caught.complete(t);
            }
        });

        assertTrue(caught.isDone());
        assertTrue(caught.join() instanceof MongoConnectionPoolClearedException);
    }

    @Test
    void invalidateShouldRecordCause() {
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().build(),
                mockSdamProvider(), OPERATION_CONTEXT_FACTORY);
        RuntimeException cause = new RuntimeException();

        pool.invalidate(cause);

        MongoConnectionPoolClearedException caught = assertThrows(MongoConnectionPoolClearedException.class,
                () -> pool.get(OPERATION_CONTEXT));
        assertTrue(caught.getCause() == cause);
    }

    @Test
    void shouldNotRepeatReadyClearedEvents() {
        ConnectionPoolListener listener = mock(ConnectionPoolListener.class);
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().addConnectionPoolListener(listener).build(),
                mockSdamProvider(), OPERATION_CONTEXT_FACTORY);

        pool.ready();
        pool.ready();
        pool.invalidate(null);
        pool.invalidate(new RuntimeException());

        verify(listener, times(1)).connectionPoolReady(
                argThat(event -> event.getServerId().equals(SERVER_ID)));
        verify(listener, times(1)).connectionPoolCleared(
                argThat(event -> event.getServerId().equals(SERVER_ID)));
    }

    @Test
    void shouldContinueToFireEventsAfterPoolIsClosed() {
        ConnectionPoolListener listener = mock(ConnectionPoolListener.class);
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory, builder().maxSize(1)
                .addConnectionPoolListener(listener).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY);
        pool.ready();
        InternalConnection connection = pool.get(OPERATION_CONTEXT);
        pool.close();

        connection.close();

        verify(listener, atLeast(1)).connectionCheckedIn(
                argThat(event -> event.getConnectionId().getServerId().equals(SERVER_ID)));
        verify(listener, atLeast(1)).connectionClosed(
                argThat(event -> event.getConnectionId().getServerId().equals(SERVER_ID)));
    }

    @Test
    void shouldSelectConnectionAsynchronouslyIfOneIsImmediatelyAvailable() {
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(1).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY);
        pool.ready();

        assertTrue(selectConnectionAsyncAndGet(pool).opened());
    }

    @Test
    void shouldSelectConnectionAsynchronouslyIfOneIsNotImmediatelyAvailable() throws Throwable {
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(1).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY);
        pool.ready();

        InternalConnection connection = pool.get(OPERATION_CONTEXT);
        ConnectionLatch connectionLatch = selectConnectionAsync(pool);
        connection.close();

        assertNotNull(connectionLatch.get());
    }

    @Test
    void whenGettingAConnectionAsynchronouslyShouldSendMongoTimeoutExceptionToCallbackAfterTimeoutPeriod() {
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(1).maxWaitTime(5, MILLISECONDS).build(),
                mockSdamProvider(), OPERATION_CONTEXT_FACTORY);
        pool.ready();
        pool.get(OPERATION_CONTEXT);
        ConnectionLatch firstConnectionLatch = selectConnectionAsync(pool);
        ConnectionLatch secondConnectionLatch = selectConnectionAsync(pool);

        assertThrows(MongoTimeoutException.class, () -> firstConnectionLatch.get());
        assertThrows(MongoTimeoutException.class, () -> secondConnectionLatch.get());
    }

    @Test
    void invalidateShouldDoNothingWhenPoolIsClosed() {
        pool = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                builder().maxSize(1).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY);
        pool.close();

        pool.invalidate(null);
        // no exception thrown
    }

    private InternalConnection selectConnectionAsyncAndGet(DefaultConnectionPool pool) {
        try {
            return selectConnectionAsync(pool).get();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private ConnectionLatch selectConnectionAsync(DefaultConnectionPool pool) {
        ConnectionLatch serverLatch = new ConnectionLatch();
        pool.getAsync(OPERATION_CONTEXT, (result, e) -> {
            serverLatch.connection = result;
            serverLatch.throwable = e;
            serverLatch.latch.countDown();
        });
        return serverLatch;
    }

    private SameObjectProvider<SdamServerDescriptionManager> mockSdamProvider() {
        return SameObjectProvider.initialized(mock(SdamServerDescriptionManager.class));
    }

    private static class ConnectionLatch {
        CountDownLatch latch = new CountDownLatch(1);
        InternalConnection connection;
        Throwable throwable;

        InternalConnection get() throws Throwable {
            latch.await();
            if (throwable != null) {
                throw throwable;
            }
            return connection;
        }
    }
}
