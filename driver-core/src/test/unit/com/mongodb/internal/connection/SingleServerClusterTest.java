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

import com.mongodb.MongoIncompatibleDriverException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.event.ClusterListener;
import com.mongodb.internal.selector.WritableServerSelector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.mongodb.ClusterFixture.CLIENT_METADATA;
import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static com.mongodb.connection.ClusterType.UNKNOWN;
import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.connection.ServerType.STANDALONE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SingleServerClusterTest {

    private static final ClusterId CLUSTER_ID = new ClusterId();
    private final ServerAddress firstServer = new ServerAddress("localhost:27017");
    private final TestClusterableServerFactory factory = new TestClusterableServerFactory();

    @BeforeEach
    void setUp() {
        Time.makeTimeConstant();
    }

    @AfterEach
    void tearDown() {
        Time.makeTimeMove();
    }

    @Test
    void shouldUpdateDescriptionWhenTheServerConnects() {
        SingleServerCluster cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(SINGLE).hosts(Arrays.asList(firstServer)).build(), factory, CLIENT_METADATA);

        try {
            sendNotification(firstServer, STANDALONE);

            assertEquals(ClusterType.STANDALONE, cluster.getCurrentDescription().getType());
            assertEquals(SINGLE, cluster.getCurrentDescription().getConnectionMode());
            assertEquals(getDescriptions(), ClusterDescriptionHelper.getAll(cluster.getCurrentDescription()));
        } finally {
            cluster.close();
        }
    }

    @Test
    void shouldGetServerWhenOpen() {
        SingleServerCluster cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(SINGLE).hosts(Arrays.asList(firstServer)).build(), factory, CLIENT_METADATA);

        try {
            sendNotification(firstServer, STANDALONE);

            assertEquals(factory.getServer(firstServer),
                    cluster.getServersSnapshot(OPERATION_CONTEXT.getTimeoutContext().computeServerSelectionTimeout(),
                            OPERATION_CONTEXT.getTimeoutContext()).getServer(firstServer));
        } finally {
            cluster.close();
        }
    }

    @Test
    void shouldNotGetServersSnapshotWhenClosed() {
        SingleServerCluster cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(SINGLE).hosts(Arrays.asList(firstServer)).build(), factory, CLIENT_METADATA);
        cluster.close();

        assertThrows(IllegalStateException.class, () ->
                cluster.getServersSnapshot(OPERATION_CONTEXT.getTimeoutContext().computeServerSelectionTimeout(),
                        OPERATION_CONTEXT.getTimeoutContext()));
    }

    @Test
    void shouldHaveNoServersOfTheWrongTypeInTheDescription() {
        SingleServerCluster cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(SINGLE).requiredClusterType(ClusterType.SHARDED)
                        .hosts(Arrays.asList(firstServer)).build(),
                factory, CLIENT_METADATA);

        try {
            sendNotification(firstServer, ServerType.REPLICA_SET_PRIMARY);

            assertEquals(ClusterType.SHARDED, cluster.getCurrentDescription().getType());
            assertTrue(ClusterDescriptionHelper.getAll(cluster.getCurrentDescription()).isEmpty());
        } finally {
            cluster.close();
        }
    }

    @Test
    void shouldHaveServerInDescriptionWhenReplicaSetNameMatchesRequiredOne() {
        SingleServerCluster cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(SINGLE).requiredReplicaSetName("test1")
                        .hosts(Arrays.asList(firstServer)).build(),
                factory, CLIENT_METADATA);

        try {
            sendNotification(firstServer, ServerType.REPLICA_SET_PRIMARY, "test1");

            assertEquals(REPLICA_SET, cluster.getCurrentDescription().getType());
            assertEquals(getDescriptions(), ClusterDescriptionHelper.getAll(cluster.getCurrentDescription()));
        } finally {
            cluster.close();
        }
    }

    @Test
    void getServerShouldThrowWhenClusterIsIncompatible() {
        SingleServerCluster cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(SINGLE).hosts(Arrays.asList(firstServer))
                        .serverSelectionTimeout(1, SECONDS).build(),
                factory, CLIENT_METADATA);

        try {
            sendNotification(firstServer, getBuilder(firstServer).minWireVersion(1000).maxWireVersion(1000).build());

            assertThrows(MongoIncompatibleDriverException.class, () ->
                    cluster.selectServer(new WritableServerSelector(), OPERATION_CONTEXT));
        } finally {
            cluster.close();
        }
    }

    @Test
    void shouldConnectToServer() {
        SingleServerCluster cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(SINGLE).hosts(Collections.singletonList(firstServer)).build(),
                factory, CLIENT_METADATA);

        cluster.connect();

        assertEquals(1, factory.getServer(firstServer).getConnectCount());
    }

    @Test
    void shouldFireClusterEvents() {
        ServerDescription serverDescription = ServerDescription.builder()
                .address(firstServer)
                .ok(true)
                .state(CONNECTED)
                .type(ServerType.REPLICA_SET_SECONDARY)
                .hosts(new HashSet<>(Arrays.asList("localhost:27017", "localhost:27018", "localhost:27019")))
                .build();

        ClusterListener listener = mock(ClusterListener.class);

        SingleServerCluster cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(SINGLE).hosts(Collections.singletonList(firstServer))
                        .addClusterListener(listener).build(),
                factory, CLIENT_METADATA);

        verify(listener).clusterOpening(argThat(event -> event.getClusterId().equals(CLUSTER_ID)));
        verify(listener).clusterDescriptionChanged(argThat(event ->
                event.getClusterId().equals(CLUSTER_ID)));

        factory.getServer(firstServer).sendNotification(serverDescription);

        verify(listener).clusterDescriptionChanged(argThat(event ->
                event.getClusterId().equals(CLUSTER_ID)
                        && event.getNewDescription().getType() == REPLICA_SET));

        cluster.close();

        verify(listener).clusterClosed(argThat(event -> event.getClusterId().equals(CLUSTER_ID)));
    }

    private void sendNotification(ServerAddress serverAddress, ServerType serverType) {
        sendNotification(serverAddress, serverType, null);
    }

    private void sendNotification(ServerAddress serverAddress, ServerType serverType, String replicaSetName) {
        sendNotification(serverAddress, getBuilder(serverAddress, serverType, replicaSetName).build());
    }

    private void sendNotification(ServerAddress serverAddress, ServerDescription serverDescription) {
        factory.getServer(serverAddress).sendNotification(serverDescription);
    }

    private Set<ServerDescription> getDescriptions() {
        return Collections.singleton(factory.getServer(firstServer).getDescription());
    }

    private ServerDescription.Builder getBuilder(ServerAddress serverAddress) {
        return ServerDescription.builder().address(serverAddress).type(STANDALONE).ok(true).state(CONNECTED);
    }

    private ServerDescription.Builder getBuilder(ServerAddress serverAddress, ServerType serverType, String replicaSetName) {
        return ServerDescription.builder().address(serverAddress).type(serverType).setName(replicaSetName).ok(true).state(CONNECTED);
    }
}
