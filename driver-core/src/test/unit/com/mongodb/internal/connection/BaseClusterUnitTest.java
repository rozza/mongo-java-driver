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

import com.mongodb.MongoClientException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.selector.ReadPreferenceServerSelector;
import com.mongodb.internal.selector.ServerAddressSelector;
import com.mongodb.internal.selector.WritableServerSelector;
import com.mongodb.internal.time.Timeout;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.mongodb.ClusterFixture.CLIENT_METADATA;
import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.ClusterFixture.createOperationContext;
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterSettings.builder;
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY;
import static com.mongodb.connection.ServerType.REPLICA_SET_SECONDARY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Add new tests to {@link BaseClusterTest}.
 */
class BaseClusterUnitTest {

    private final ServerAddress firstServer = new ServerAddress("localhost:27017");
    private final ServerAddress secondServer = new ServerAddress("localhost:27018");
    private final ServerAddress thirdServer = new ServerAddress("localhost:27019");
    private final List<ServerAddress> allServers = Arrays.asList(firstServer, secondServer, thirdServer);
    private final TestClusterableServerFactory factory = new TestClusterableServerFactory();

    @Test
    void shouldHaveCurrentDescriptionImmediatelyAfterConstruction() {
        ClusterSettings clusterSettings = builder().mode(MULTIPLE)
                .hosts(Arrays.asList(firstServer, secondServer, thirdServer))
                .serverSelector(new ServerAddressSelector(firstServer))
                .build();
        BaseCluster cluster = new BaseCluster(new ClusterId(), clusterSettings, factory, CLIENT_METADATA) {
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

        assertEquals(new ClusterDescription(clusterSettings.getMode(), ClusterType.UNKNOWN,
                        Collections.emptyList(), clusterSettings, factory.getSettings()),
                cluster.getCurrentDescription());

        assertThrows(MongoTimeoutException.class, () ->
                cluster.selectServer(clusterDescription -> Collections.emptyList(),
                        createOperationContext(TIMEOUT_SETTINGS.withServerSelectionTimeoutMS(1))));

        assertThrows(MongoTimeoutException.class, () ->
                cluster.selectServer(clusterDescription -> Collections.emptyList(),
                        createOperationContext(TIMEOUT_SETTINGS
                                .withServerSelectionTimeoutMS(1L)
                                .withTimeout(1L, MILLISECONDS))));
    }

    @Test
    void shouldGetClusterSettings() {
        ClusterSettings clusterSettings = builder().mode(MULTIPLE)
                .hosts(Arrays.asList(firstServer, secondServer, thirdServer))
                .serverSelectionTimeout(1, SECONDS)
                .serverSelector(new ServerAddressSelector(firstServer))
                .build();
        MultiServerCluster cluster = new MultiServerCluster(new ClusterId(), clusterSettings, factory, CLIENT_METADATA);

        assertEquals(clusterSettings, cluster.getSettings());
    }

    @Test
    void shouldComposeServerSelectorPassedToSelectServerWithServerSelectorInClusterSettings() {
        MultiServerCluster cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts(Arrays.asList(firstServer, secondServer, thirdServer))
                        .serverSelectionTimeout(1, SECONDS)
                        .serverSelector(new ServerAddressSelector(firstServer))
                        .build(),
                factory, CLIENT_METADATA);
        factory.sendNotification(firstServer, REPLICA_SET_SECONDARY, allServers);
        factory.sendNotification(secondServer, REPLICA_SET_SECONDARY, allServers);
        factory.sendNotification(thirdServer, REPLICA_SET_PRIMARY, allServers);

        assertEquals(firstServer,
                cluster.selectServer(new ReadPreferenceServerSelector(ReadPreference.secondary()), OPERATION_CONTEXT)
                        .getServerDescription().getAddress());
    }

    @Test
    void shouldUseServerSelectorPassedToSelectServerIfServerSelectorInClusterSettingsIsNull() {
        MultiServerCluster cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts(Arrays.asList(firstServer, secondServer, thirdServer))
                        .build(),
                factory, CLIENT_METADATA);
        factory.sendNotification(firstServer, REPLICA_SET_SECONDARY, allServers);
        factory.sendNotification(secondServer, REPLICA_SET_SECONDARY, allServers);
        factory.sendNotification(thirdServer, REPLICA_SET_PRIMARY, allServers);

        assertEquals(firstServer,
                cluster.selectServer(new ServerAddressSelector(firstServer),
                                createOperationContext(TIMEOUT_SETTINGS.withServerSelectionTimeoutMS(1_000)))
                        .getServerDescription().getAddress());
    }

    @Test
    void shouldApplyLocalThresholdWhenCustomServerSelectorIsPresent() {
        MultiServerCluster cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts(Arrays.asList(firstServer, secondServer, thirdServer))
                        .serverSelectionTimeout(1, SECONDS)
                        .serverSelector(new ReadPreferenceServerSelector(ReadPreference.secondary()))
                        .localThreshold(5, MILLISECONDS)
                        .build(),
                factory, CLIENT_METADATA);
        factory.sendNotification(firstServer, 1L, REPLICA_SET_SECONDARY, allServers);
        factory.sendNotification(secondServer, 7L, REPLICA_SET_SECONDARY, allServers);
        factory.sendNotification(thirdServer, 1L, REPLICA_SET_PRIMARY, allServers);

        assertEquals(firstServer,
                cluster.selectServer(new ReadPreferenceServerSelector(ReadPreference.nearest()), OPERATION_CONTEXT)
                        .getServerDescription().getAddress());
    }

    @Test
    void shouldApplyLocalThresholdWhenCustomServerSelectorIsAbsent() {
        MultiServerCluster cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .serverSelectionTimeout(1, SECONDS)
                        .hosts(Arrays.asList(firstServer, secondServer, thirdServer))
                        .localThreshold(5, MILLISECONDS)
                        .build(),
                factory, CLIENT_METADATA);
        factory.sendNotification(firstServer, 1L, REPLICA_SET_SECONDARY, allServers);
        factory.sendNotification(secondServer, 7L, REPLICA_SET_SECONDARY, allServers);
        factory.sendNotification(thirdServer, 1L, REPLICA_SET_PRIMARY, allServers);

        assertEquals(firstServer,
                cluster.selectServer(new ReadPreferenceServerSelector(ReadPreference.secondary()), OPERATION_CONTEXT)
                        .getServerDescription().getAddress());
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 0})
    void shouldTimeoutWithUsefulMessage(long serverSelectionTimeoutMS) {
        MultiServerCluster cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts(Arrays.asList(firstServer, secondServer))
                        .build(),
                factory, CLIENT_METADATA);

        factory.sendNotification(firstServer, ServerDescription.builder().type(ServerType.UNKNOWN)
                .state(ServerConnectionState.CONNECTING)
                .address(firstServer)
                .exception(new MongoInternalException("oops"))
                .build());

        MongoTimeoutException e = assertThrows(MongoTimeoutException.class, () ->
                cluster.selectServer(new WritableServerSelector(),
                        createOperationContext(TIMEOUT_SETTINGS.withServerSelectionTimeoutMS(serverSelectionTimeoutMS))));

        assertTrue(e.getMessage().startsWith("Timed out while waiting for a server "
                + "that matches WritableServerSelector. Client view of cluster state is {type=UNKNOWN"));
        assertTrue(e.getMessage().contains("{address=localhost:27017, type=UNKNOWN, state=CONNECTING, "
                + "exception={com.mongodb.MongoInternalException: oops}}"));
        assertTrue(e.getMessage().contains("{address=localhost:27018, type=UNKNOWN, state=CONNECTING}"));
    }

    @ParameterizedTest
    @ValueSource(longs = {30, 0, -1})
    void shouldSelectServer(long serverSelectionTimeoutMS) {
        MultiServerCluster cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts(Arrays.asList(firstServer, secondServer, thirdServer))
                        .build(),
                factory, CLIENT_METADATA);
        factory.sendNotification(firstServer, REPLICA_SET_SECONDARY, allServers);
        factory.sendNotification(secondServer, REPLICA_SET_SECONDARY, allServers);
        factory.sendNotification(thirdServer, REPLICA_SET_PRIMARY, allServers);

        try {
            assertEquals(thirdServer,
                    cluster.selectServer(new ReadPreferenceServerSelector(ReadPreference.primary()),
                                    createOperationContext(TIMEOUT_SETTINGS.withServerSelectionTimeoutMS(serverSelectionTimeoutMS)))
                            .getServerDescription().getAddress());
        } finally {
            cluster.close();
        }
    }

    @Test
    @Tag("Slow")
    void shouldWaitIndefinitelyForAServerUntilInterrupted() throws InterruptedException {
        MultiServerCluster cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts(Arrays.asList(firstServer, secondServer, thirdServer))
                        .build(),
                factory, CLIENT_METADATA);

        CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            try {
                cluster.selectServer(new ReadPreferenceServerSelector(ReadPreference.primary()),
                        createOperationContext(TIMEOUT_SETTINGS.withServerSelectionTimeoutMS(-1_000)));
            } catch (MongoInterruptedException e) {
                latch.countDown();
            }
        });
        thread.start();
        Thread.sleep(1000);
        thread.interrupt();
        boolean interrupted = latch.await(com.mongodb.ClusterFixture.TIMEOUT, SECONDS);

        assertTrue(interrupted);

        cluster.close();
    }

    @ParameterizedTest
    @ValueSource(longs = {30, 0, -1})
    void shouldSelectServerAsynchronouslyWhenServerIsAlreadyAvailable(long serverSelectionTimeoutMS) throws Throwable {
        MultiServerCluster cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts(Arrays.asList(firstServer, secondServer, thirdServer))
                        .build(),
                factory, CLIENT_METADATA);
        factory.sendNotification(firstServer, REPLICA_SET_SECONDARY, allServers);

        try {
            ServerDescription serverDescription = selectServerAsync(cluster, firstServer, serverSelectionTimeoutMS)
                    .getDescription();
            assertEquals(firstServer, serverDescription.getAddress());
        } finally {
            cluster.close();
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {500, -1})
    void shouldSelectServerAsynchronouslyWhenServerIsNotYetAvailable(long serverSelectionTimeoutMS) throws Throwable {
        MultiServerCluster cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts(Arrays.asList(firstServer, secondServer, thirdServer))
                        .build(),
                factory, CLIENT_METADATA);

        try {
            ServerLatch secondServerLatch = selectServerAsync(cluster, secondServer, serverSelectionTimeoutMS);
            ServerLatch thirdServerLatch = selectServerAsync(cluster, thirdServer, serverSelectionTimeoutMS);
            factory.sendNotification(secondServer, REPLICA_SET_SECONDARY, allServers);
            factory.sendNotification(thirdServer, REPLICA_SET_SECONDARY, allServers);

            assertEquals(secondServer, secondServerLatch.getDescription().getAddress());
            assertEquals(thirdServer, thirdServerLatch.getDescription().getAddress());
        } finally {
            cluster.close();
        }
    }

    @Test
    void whenSelectingServerAsynchronouslyShouldSendMongoClientExceptionToCallbackIfClusterIsClosedBeforeSuccess()
            throws Throwable {
        MultiServerCluster cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts(Arrays.asList(firstServer, secondServer, thirdServer))
                        .build(),
                factory, CLIENT_METADATA);

        try {
            ServerLatch serverLatch = selectServerAsync(cluster, firstServer);
            cluster.close();
            assertThrows(MongoClientException.class, () -> serverLatch.get());
        } finally {
            cluster.close();
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {100, 0})
    void whenSelectingServerAsynchronouslyShouldSendMongoTimeoutExceptionToCallbackAfterTimeoutPeriod(
            long serverSelectionTimeoutMS) {
        MultiServerCluster cluster = new MultiServerCluster(new ClusterId(),
                builder().mode(MULTIPLE)
                        .hosts(Arrays.asList(firstServer, secondServer, thirdServer))
                        .build(),
                factory, CLIENT_METADATA);

        try {
            assertThrows(MongoTimeoutException.class, () ->
                    selectServerAsyncAndGet(cluster, firstServer, serverSelectionTimeoutMS));
        } finally {
            cluster.close();
        }
    }

    private Server selectServerAsyncAndGet(BaseCluster cluster, ServerAddress serverAddress, long serverSelectionTimeoutMS)
            throws Throwable {
        return selectServerAsync(cluster, serverAddress, serverSelectionTimeoutMS).get();
    }

    private ServerLatch selectServerAsync(BaseCluster cluster, ServerAddress serverAddress) {
        return selectServerAsync(cluster, serverAddress, 1_000);
    }

    private ServerLatch selectServerAsync(BaseCluster cluster, ServerAddress serverAddress, long serverSelectionTimeoutMS) {
        ServerLatch serverLatch = new ServerLatch();
        cluster.selectServerAsync(new ServerAddressSelector(serverAddress),
                createOperationContext(TIMEOUT_SETTINGS.withServerSelectionTimeoutMS(serverSelectionTimeoutMS)),
                (result, e) -> {
                    serverLatch.server = result != null ? result.getServer() : null;
                    serverLatch.serverDescription = result != null ? result.getServerDescription() : null;
                    serverLatch.throwable = e;
                    serverLatch.latch.countDown();
                });
        return serverLatch;
    }

    private static class ServerLatch {
        CountDownLatch latch = new CountDownLatch(1);
        Server server;
        ServerDescription serverDescription;
        Throwable throwable;

        Server get() throws Throwable {
            latch.await();
            if (throwable != null) {
                throw throwable;
            }
            return server;
        }

        ServerDescription getDescription() throws Throwable {
            latch.await();
            if (throwable != null) {
                throw throwable;
            }
            return serverDescription;
        }
    }
}
