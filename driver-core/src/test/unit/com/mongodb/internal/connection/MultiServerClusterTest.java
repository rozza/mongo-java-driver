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

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.event.ClusterListener;
import com.mongodb.internal.selector.WritableServerSelector;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import static com.mongodb.ClusterFixture.CLIENT_METADATA;
import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static com.mongodb.connection.ClusterType.SHARDED;
import static com.mongodb.connection.ClusterType.UNKNOWN;
import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.connection.ServerType.REPLICA_SET_GHOST;
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY;
import static com.mongodb.connection.ServerType.REPLICA_SET_SECONDARY;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static com.mongodb.connection.ServerType.STANDALONE;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getAll;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getByServerAddress;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MultiServerClusterTest {

    private static final ClusterId CLUSTER_ID = new ClusterId();
    private final ServerAddress firstServer = new ServerAddress("localhost:27017");
    private final ServerAddress secondServer = new ServerAddress("localhost:27018");
    private final ServerAddress thirdServer = new ServerAddress("localhost:27019");
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
    void shouldIncludeSettingsInClusterDescription() {
        MultiServerCluster cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(MULTIPLE)
                .serverSelectionTimeout(1, MILLISECONDS)
                .hosts(Collections.singletonList(firstServer)).build(), factory, CLIENT_METADATA);
        sendNotification(firstServer, REPLICA_SET_PRIMARY);

        assertNotNull(cluster.getCurrentDescription().getClusterSettings());
        assertNotNull(cluster.getCurrentDescription().getServerSettings());
    }

    @Test
    void shouldCorrectReportDescriptionWhenConnectedToAPrimary() {
        MultiServerCluster cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(MULTIPLE).hosts(Collections.singletonList(firstServer)).build(),
                factory, CLIENT_METADATA);

        sendNotification(firstServer, REPLICA_SET_PRIMARY);

        assertEquals(REPLICA_SET, cluster.getCurrentDescription().getType());
        assertEquals(MULTIPLE, cluster.getCurrentDescription().getConnectionMode());
    }

    @Test
    void shouldNotGetServersSnapshotWhenClosed() {
        MultiServerCluster cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().hosts(Arrays.asList(firstServer)).mode(MULTIPLE).build(),
                factory, CLIENT_METADATA);
        cluster.close();

        assertThrows(IllegalStateException.class, () ->
                cluster.getServersSnapshot(OPERATION_CONTEXT.getTimeoutContext().computeServerSelectionTimeout(),
                        OPERATION_CONTEXT.getTimeoutContext()));
    }

    @Test
    void shouldDiscoverAllHostsInTheClusterWhenNotifiedByThePrimary() {
        MultiServerCluster cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(MULTIPLE).hosts(Collections.singletonList(firstServer)).build(),
                factory, CLIENT_METADATA);

        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, Arrays.asList(firstServer, secondServer, thirdServer));

        assertEquals(factory.getDescriptions(firstServer, secondServer, thirdServer),
                getAll(cluster.getCurrentDescription()));
    }

    @Test
    void shouldDiscoverAllHostsWhenNotifiedBySecondaryAndThereIsNoPrimary() {
        MultiServerCluster cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(MULTIPLE).hosts(Collections.singletonList(firstServer)).build(),
                factory, CLIENT_METADATA);

        factory.sendNotification(firstServer, REPLICA_SET_SECONDARY, Arrays.asList(firstServer, secondServer, thirdServer));

        assertEquals(factory.getDescriptions(firstServer, secondServer, thirdServer),
                getAll(cluster.getCurrentDescription()));
    }

    @Test
    void shouldDiscoverAllPassivesInTheCluster() {
        MultiServerCluster cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(MULTIPLE).hosts(Collections.singletonList(firstServer)).build(),
                factory, CLIENT_METADATA);

        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, Collections.singletonList(firstServer),
                Arrays.asList(secondServer, thirdServer));

        assertEquals(factory.getDescriptions(firstServer, secondServer, thirdServer),
                getAll(cluster.getCurrentDescription()));
    }

    @Test
    void shouldRemoveServerWhenItNoLongerAppearsInHostsReportedByPrimary() {
        MultiServerCluster cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().hosts(Arrays.asList(firstServer, secondServer, thirdServer)).build(),
                factory, CLIENT_METADATA);
        sendNotification(firstServer, REPLICA_SET_PRIMARY);
        sendNotification(secondServer, REPLICA_SET_SECONDARY);
        sendNotification(thirdServer, REPLICA_SET_SECONDARY);

        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, Arrays.asList(firstServer, secondServer));

        assertEquals(factory.getDescriptions(firstServer, secondServer),
                getAll(cluster.getCurrentDescription()));
        assertTrue(factory.getServer(thirdServer).isClosed());
    }

    @Test
    void shouldRemoveServerOfWrongTypeWhenTypeIsReplicaSet() {
        MultiServerCluster cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().requiredClusterType(REPLICA_SET)
                        .hosts(Arrays.asList(firstServer, secondServer)).build(),
                factory, CLIENT_METADATA);

        sendNotification(secondServer, SHARD_ROUTER);

        assertEquals(REPLICA_SET, cluster.getCurrentDescription().getType());
        assertEquals(factory.getDescriptions(firstServer), getAll(cluster.getCurrentDescription()));
    }

    @Test
    void shouldIgnoreEmptyHostListWhenTypeIsReplicaSet() {
        MultiServerCluster cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().requiredClusterType(REPLICA_SET)
                        .hosts(Arrays.asList(firstServer, secondServer)).build(),
                factory, CLIENT_METADATA);

        factory.sendNotification(secondServer, REPLICA_SET_GHOST, Collections.emptyList());

        assertEquals(REPLICA_SET, cluster.getCurrentDescription().getType());
        assertEquals(factory.getDescriptions(firstServer, secondServer), getAll(cluster.getCurrentDescription()));
        assertEquals(REPLICA_SET_GHOST, getByServerAddress(cluster.getCurrentDescription(), secondServer).getType());
    }

    @Test
    void shouldRemoveServerOfWrongTypeWhenTypeIsSharded() {
        MultiServerCluster cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().requiredClusterType(SHARDED)
                        .hosts(Arrays.asList(firstServer, secondServer)).build(),
                factory, CLIENT_METADATA);
        sendNotification(firstServer, SHARD_ROUTER);

        sendNotification(secondServer, REPLICA_SET_PRIMARY);

        assertEquals(SHARDED, cluster.getCurrentDescription().getType());
        assertEquals(factory.getDescriptions(firstServer), getAll(cluster.getCurrentDescription()));
    }

    @Test
    void shouldNotSetClusterTypeWhenConnectedToStandaloneWhenSeedListSizeIsGreaterThanOne() {
        MultiServerCluster cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().serverSelectionTimeout(1, MILLISECONDS)
                        .mode(MULTIPLE).hosts(Arrays.asList(firstServer, secondServer)).build(),
                factory, CLIENT_METADATA);

        sendNotification(firstServer, STANDALONE);

        assertEquals(UNKNOWN, cluster.getCurrentDescription().getType());
    }

    @Test
    void shouldRetainStandaloneServerGivenHostsListOfSize1() {
        MultiServerCluster cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(MULTIPLE).hosts(Collections.singletonList(firstServer)).build(),
                factory, CLIENT_METADATA);

        sendNotification(firstServer, STANDALONE);

        assertEquals(ClusterType.STANDALONE, cluster.getCurrentDescription().getType());
        assertEquals(factory.getDescriptions(firstServer), getAll(cluster.getCurrentDescription()));
    }

    @Test
    void shouldInvalidateExistingPrimaryWhenNewPrimaryNotifies() {
        MultiServerCluster cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().hosts(Arrays.asList(firstServer, secondServer)).build(),
                factory, CLIENT_METADATA);
        sendNotification(firstServer, REPLICA_SET_PRIMARY);

        sendNotification(secondServer, REPLICA_SET_PRIMARY);

        assertEquals(CONNECTING, factory.getDescription(firstServer).getState());
        assertEquals(factory.getDescriptions(firstServer, secondServer, thirdServer),
                getAll(cluster.getCurrentDescription()));
    }

    @Test
    void shouldInvalidateNewPrimaryIfElectionIdIsLessThanPreviouslyReported() {
        MultiServerCluster cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().hosts(Arrays.asList(firstServer, secondServer)).build(),
                factory, CLIENT_METADATA);

        ObjectId electionId = new ObjectId(new Date(1000));
        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, Arrays.asList(firstServer, secondServer, thirdServer), electionId);

        ObjectId outdatedElectionId = new ObjectId(new Date(999));
        factory.sendNotification(secondServer, REPLICA_SET_PRIMARY, Arrays.asList(firstServer, secondServer, thirdServer),
                outdatedElectionId);

        assertEquals(CONNECTED, factory.getDescription(firstServer).getState());
        assertEquals(REPLICA_SET_PRIMARY, factory.getDescription(firstServer).getType());
        assertEquals(CONNECTING, factory.getDescription(secondServer).getState());
        assertEquals(factory.getDescriptions(firstServer, secondServer, thirdServer),
                getAll(cluster.getCurrentDescription()));
    }

    @Test
    void shouldThrowFromGetServerIfClusterIsClosed() {
        MultiServerCluster cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().serverSelectionTimeout(100, MILLISECONDS)
                        .hosts(Collections.singletonList(firstServer)).mode(MULTIPLE).build(),
                factory, CLIENT_METADATA);
        cluster.close();

        assertThrows(IllegalStateException.class, () ->
                cluster.selectServer(new WritableServerSelector(), OPERATION_CONTEXT));
    }

    @Test
    void shouldConnectToAllServers() {
        MultiServerCluster cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().hosts(Arrays.asList(firstServer, secondServer)).build(),
                factory, CLIENT_METADATA);

        cluster.connect();

        assertEquals(1, factory.getServer(firstServer).getConnectCount());
        assertEquals(1, factory.getServer(secondServer).getConnectCount());
    }

    private void sendNotification(ServerAddress serverAddress, ServerType serverType) {
        factory.sendNotification(serverAddress, serverType, Arrays.asList(firstServer, secondServer, thirdServer));
    }
}
