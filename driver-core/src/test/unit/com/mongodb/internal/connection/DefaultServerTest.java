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

import com.mongodb.MongoException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoServerUnavailableException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.MongoStalePrimaryException;
import com.mongodb.ServerAddress;
import com.mongodb.client.syncadapter.SupplyingCallback;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.inject.SameObjectProvider;
import com.mongodb.internal.time.Timeout;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.CLIENT_METADATA;
import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.MongoCredential.createCredential;
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DefaultServerTest {

    private final ServerId serverId = new ServerId(new ClusterId(), new ServerAddress());

    static Stream<ClusterConnectionMode> connectionModes() {
        return Stream.of(SINGLE, MULTIPLE);
    }

    @ParameterizedTest
    @MethodSource("connectionModes")
    void shouldGetAConnection(ClusterConnectionMode mode) {
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        InternalConnection internalConnection = mock(InternalConnection.class);
        Connection connection = mock(Connection.class);

        when(connectionPool.get(any())).thenReturn(internalConnection);
        when(connectionFactory.create(any(), any(), any())).thenReturn(connection);

        DefaultServer server = new DefaultServer(serverId, mode, connectionPool, connectionFactory,
                mock(ServerMonitor.class), mock(SdamServerDescriptionManager.class),
                mock(ServerListener.class), mock(CommandListener.class), new ClusterClock(), false);

        Connection receivedConnection = server.getConnection(OPERATION_CONTEXT);
        assertEquals(connection, receivedConnection);
        verify(connectionFactory, times(1)).create(any(), any(), any());
    }

    @ParameterizedTest
    @MethodSource("connectionModes")
    void shouldGetAConnectionAsynchronously(ClusterConnectionMode mode) {
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        InternalConnection internalConnection = mock(InternalConnection.class);
        AsyncConnection asyncConnection = mock(AsyncConnection.class);

        when(connectionPool.get(any())).thenReturn(internalConnection);
        when(connectionFactory.createAsync(any(), any(), any())).thenReturn(asyncConnection);
        //noinspection unchecked
        org.mockito.stubbing.Answer<Void> answer = invocation -> {
            ((com.mongodb.internal.async.SingleResultCallback<InternalConnection>) invocation.getArgument(1))
                    .onResult(internalConnection, null);
            return null;
        };
        org.mockito.Mockito.doAnswer(answer).when(connectionPool).getAsync(any(), any());

        DefaultServer server = new DefaultServer(serverId, mode, connectionPool, connectionFactory,
                mock(ServerMonitor.class), mock(SdamServerDescriptionManager.class),
                mock(ServerListener.class), mock(CommandListener.class), new ClusterClock(), false);

        SupplyingCallback<AsyncConnection> callback = new SupplyingCallback<>();
        server.getConnectionAsync(OPERATION_CONTEXT, callback);

        assertEquals(asyncConnection, callback.get());
        verify(connectionFactory, times(1)).createAsync(any(), any(), any());
    }

    @Test
    void shouldThrowMongoServerUnavailableExceptionGettingAConnectionWhenTheServerIsClosed() throws InterruptedException {
        DefaultServer server = new DefaultServer(serverId, SINGLE, mock(ConnectionPool.class),
                mock(ConnectionFactory.class), mock(ServerMonitor.class), mock(SdamServerDescriptionManager.class),
                mock(ServerListener.class), mock(CommandListener.class), new ClusterClock(), false);
        server.close();

        MongoServerUnavailableException ex = assertThrows(MongoServerUnavailableException.class,
                () -> server.getConnection(OPERATION_CONTEXT));
        assertEquals("The server at 127.0.0.1:27017 is no longer available", ex.getMessage());

        CountDownLatch latch = new CountDownLatch(1);
        final AsyncConnection[] receivedConnection = {null};
        final Throwable[] receivedThrowable = {null};
        server.getConnectionAsync(OPERATION_CONTEXT, (result, throwable) -> {
            receivedConnection[0] = result;
            receivedThrowable[0] = throwable;
            latch.countDown();
        });
        latch.await();

        assertNull(receivedConnection[0]);
        assertTrue(receivedThrowable[0] instanceof MongoServerUnavailableException);
        assertEquals("The server at 127.0.0.1:27017 is no longer available", receivedThrowable[0].getMessage());
    }

    static Stream<MongoException> invalidateInvokeListenersExceptions() {
        return Stream.of(
                new MongoStalePrimaryException(""),
                new MongoNotPrimaryException(new BsonDocument(), new ServerAddress()),
                new MongoNodeIsRecoveringException(new BsonDocument(), new ServerAddress()),
                new MongoSocketException("", new ServerAddress()),
                new MongoWriteConcernWithResponseException(new MongoException(""), new Object())
        );
    }

    @ParameterizedTest
    @MethodSource("invalidateInvokeListenersExceptions")
    void invalidateShouldInvokeServerListeners(MongoException exceptionToThrow) {
        ServerListener serverListener = mock(ServerListener.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        SameObjectProvider<SdamServerDescriptionManager> sdamProvider = SameObjectProvider.uninitialized();
        TestServerMonitor serverMonitor = new TestServerMonitor(sdamProvider);
        sdamProvider.initialize(new DefaultSdamServerDescriptionManager(mockCluster(), serverId, serverListener,
                serverMonitor, connectionPool, ClusterConnectionMode.MULTIPLE));
        DefaultServer server = defaultServer(connectionPool, serverMonitor, serverListener, sdamProvider.get(),
                mock(CommandListener.class));
        serverMonitor.updateServerDescription(ServerDescription.builder()
                .address(serverId.getAddress())
                .ok(true)
                .state(ServerConnectionState.CONNECTED)
                .type(ServerType.STANDALONE)
                .build());

        org.mockito.Mockito.clearInvocations(serverListener);

        server.invalidate(exceptionToThrow);

        verify(serverListener, times(1)).serverDescriptionChanged(any());

        server.close();
    }

    static Stream<MongoException> invalidateDoNotInvokeListenersExceptions() {
        return Stream.of(
                new MongoException(""),
                new MongoSecurityException(createCredential("jeff", "admin", "123".toCharArray()), "Auth failed")
        );
    }

    @ParameterizedTest
    @MethodSource("invalidateDoNotInvokeListenersExceptions")
    void invalidateShouldNotInvokeServerListeners(MongoException exceptionToThrow) {
        ServerListener serverListener = mock(ServerListener.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        SameObjectProvider<SdamServerDescriptionManager> sdamProvider = SameObjectProvider.uninitialized();
        TestServerMonitor serverMonitor = new TestServerMonitor(sdamProvider);
        sdamProvider.initialize(new DefaultSdamServerDescriptionManager(mockCluster(), serverId, serverListener,
                serverMonitor, connectionPool, ClusterConnectionMode.MULTIPLE));
        DefaultServer server = defaultServer(connectionPool, serverMonitor, serverListener, sdamProvider.get(),
                mock(CommandListener.class));
        serverMonitor.updateServerDescription(ServerDescription.builder()
                .address(serverId.getAddress())
                .ok(true)
                .state(ServerConnectionState.CONNECTED)
                .type(ServerType.STANDALONE)
                .build());

        org.mockito.Mockito.clearInvocations(serverListener);

        server.invalidate(exceptionToThrow);

        verify(serverListener, never()).serverDescriptionChanged(any());

        server.close();
    }

    static Stream<MongoException> invalidateDoNothingWhenClosedExceptions() {
        return Stream.of(
                new MongoStalePrimaryException(""),
                new MongoNotPrimaryException(new BsonDocument(), new ServerAddress()),
                new MongoNodeIsRecoveringException(new BsonDocument(), new ServerAddress()),
                new MongoSocketException("", new ServerAddress()),
                new MongoWriteConcernWithResponseException(new MongoException(""), new Object()),
                new MongoException(""),
                new MongoSecurityException(createCredential("jeff", "admin", "123".toCharArray()), "Auth failed")
        );
    }

    @ParameterizedTest
    @MethodSource("invalidateDoNothingWhenClosedExceptions")
    void invalidateShouldDoNothingWhenServerIsClosedForAnyException(MongoException exceptionToThrow) {
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        ServerMonitor serverMonitor = mock(ServerMonitor.class);

        DefaultServer server = defaultServer(connectionPool, serverMonitor);
        server.close();

        server.invalidate(exceptionToThrow);

        verify(connectionPool, never()).invalidate(null);
        verify(serverMonitor, never()).connect();
    }

    static Stream<MongoException> failedOpenExceptions() {
        return Stream.of(
                new MongoSocketOpenException("open failed", new ServerAddress(), new IOException()),
                new MongoSocketWriteException("Write failed", new ServerAddress(), new IOException()),
                new MongoSocketReadException("Read failed", new ServerAddress(), new IOException()),
                new MongoSocketReadTimeoutException("Read timed out", new ServerAddress(), new IOException())
        );
    }

    @ParameterizedTest
    @MethodSource("failedOpenExceptions")
    void failedOpenShouldInvalidateTheServer(MongoException exceptionToThrow) {
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(connectionPool.get(any())).thenThrow(exceptionToThrow);
        ServerMonitor serverMonitor = mock(ServerMonitor.class);
        DefaultServer server = defaultServer(connectionPool, serverMonitor);

        MongoException e = assertThrows(MongoException.class, () -> server.getConnection(OPERATION_CONTEXT));
        assertSame(exceptionToThrow, e);
        verify(connectionPool).invalidate(exceptionToThrow);
        verify(serverMonitor).cancelCurrentCheck();
    }

    @Test
    void failedAuthenticationShouldInvalidateTheConnectionPool() {
        MongoSecurityException exceptionToThrow = new MongoSecurityException(
                createCredential("jeff", "admin", "123".toCharArray()), "Auth failed");
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(connectionPool.get(any())).thenThrow(exceptionToThrow);
        ServerMonitor serverMonitor = mock(ServerMonitor.class);
        DefaultServer server = defaultServer(connectionPool, serverMonitor);

        MongoSecurityException e = assertThrows(MongoSecurityException.class,
                () -> server.getConnection(OPERATION_CONTEXT));
        assertSame(exceptionToThrow, e);
        verify(connectionPool).invalidate(exceptionToThrow);
        verify(serverMonitor, never()).connect();
    }

    @ParameterizedTest
    @MethodSource("failedOpenExceptions")
    void failedOpenShouldInvalidateTheServerAsynchronously(MongoException exceptionToThrow) throws InterruptedException {
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        org.mockito.Mockito.doAnswer(invocation -> {
            ((com.mongodb.internal.async.SingleResultCallback<InternalConnection>) invocation.getArgument(1))
                    .onResult(null, exceptionToThrow);
            return null;
        }).when(connectionPool).getAsync(any(), any());
        ServerMonitor serverMonitor = mock(ServerMonitor.class);
        DefaultServer server = defaultServer(connectionPool, serverMonitor);

        CountDownLatch latch = new CountDownLatch(1);
        final AsyncConnection[] receivedConnection = {null};
        final Throwable[] receivedThrowable = {null};
        server.getConnectionAsync(OPERATION_CONTEXT, (result, throwable) -> {
            receivedConnection[0] = result;
            receivedThrowable[0] = throwable;
            latch.countDown();
        });
        latch.await();

        assertNull(receivedConnection[0]);
        assertSame(exceptionToThrow, receivedThrowable[0]);
        verify(connectionPool).invalidate(exceptionToThrow);
        verify(serverMonitor).cancelCurrentCheck();
    }

    @Test
    void failedAuthShouldInvalidateTheConnectionPoolAsynchronously() throws InterruptedException {
        MongoSecurityException exceptionToThrow = new MongoSecurityException(
                createCredential("jeff", "admin", "123".toCharArray()), "Auth failed");
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        org.mockito.Mockito.doAnswer(invocation -> {
            ((com.mongodb.internal.async.SingleResultCallback<InternalConnection>) invocation.getArgument(1))
                    .onResult(null, exceptionToThrow);
            return null;
        }).when(connectionPool).getAsync(any(), any());
        ServerMonitor serverMonitor = mock(ServerMonitor.class);
        DefaultServer server = defaultServer(connectionPool, serverMonitor);

        CountDownLatch latch = new CountDownLatch(1);
        final AsyncConnection[] receivedConnection = {null};
        final Throwable[] receivedThrowable = {null};
        server.getConnectionAsync(OPERATION_CONTEXT, (result, throwable) -> {
            receivedConnection[0] = result;
            receivedThrowable[0] = throwable;
            latch.countDown();
        });
        latch.await();

        assertNull(receivedConnection[0]);
        assertSame(exceptionToThrow, receivedThrowable[0]);
        verify(connectionPool).invalidate(exceptionToThrow);
        verify(serverMonitor, never()).connect();
    }

    private DefaultServer defaultServer(final ConnectionPool connectionPool, final ServerMonitor serverMonitor) {
        ServerListener serverListener = mock(ServerListener.class);
        return defaultServer(connectionPool, serverMonitor, serverListener,
                new DefaultSdamServerDescriptionManager(mockCluster(), serverId, serverListener, serverMonitor,
                        connectionPool, ClusterConnectionMode.MULTIPLE),
                mock(CommandListener.class));
    }

    private DefaultServer defaultServer(final ConnectionPool connectionPool, final ServerMonitor serverMonitor,
                                        final ServerListener serverListener,
                                        final SdamServerDescriptionManager sdam, final CommandListener commandListener) {
        serverMonitor.start();
        return new DefaultServer(serverId, SINGLE, connectionPool, new TestConnectionFactory(), serverMonitor,
                sdam, serverListener, commandListener, new ClusterClock(), false);
    }

    private Cluster mockCluster() {
        return new BaseCluster(new ClusterId(), ClusterSettings.builder().build(),
                mock(ClusterableServerFactory.class), CLIENT_METADATA) {
            @Override
            protected void connect() {
            }

            @Override
            public Cluster.ServersSnapshot getServersSnapshot(final Timeout serverSelectionTimeout,
                                                               final TimeoutContext timeoutContext) {
                return serverAddress -> {
                    throw new UnsupportedOperationException();
                };
            }

            @Override
            public void onChange(final ServerDescriptionChangedEvent event) {
            }
        };
    }
}
