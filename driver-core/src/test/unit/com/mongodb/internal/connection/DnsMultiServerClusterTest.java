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

import com.mongodb.ClusterFixture;
import com.mongodb.MongoConfigurationException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.event.ClusterListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterType.SHARDED;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class DnsMultiServerClusterTest {

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
    void shouldInitializeFromDnsSrvMonitor() {
        String srvHost = "test1.test.build.10gen.cc";
        ClusterListener clusterListener = mock(ClusterListener.class);
        DnsSrvRecordMonitor dnsSrvRecordMonitor = mock(DnsSrvRecordMonitor.class);
        MongoConfigurationException exception = new MongoConfigurationException("test");
        final DnsSrvRecordInitializer[] initializerHolder = new DnsSrvRecordInitializer[1];

        DnsSrvRecordMonitorFactory dnsSrvRecordMonitorFactory = (hostName, srvServiceName, dnsSrvRecordListener) -> {
            initializerHolder[0] = dnsSrvRecordListener;
            return dnsSrvRecordMonitor;
        };

        DnsMultiServerCluster cluster = new DnsMultiServerCluster(new ClusterId(),
                ClusterSettings.builder()
                        .addClusterListener(clusterListener)
                        .serverSelectionTimeout(1, TimeUnit.MILLISECONDS)
                        .srvHost(srvHost)
                        .mode(MULTIPLE)
                        .build(),
                factory, ClusterFixture.CLIENT_METADATA, dnsSrvRecordMonitorFactory);

        assertNotNull(initializerHolder[0]);
        verify(dnsSrvRecordMonitor).start();

        // Before initialization
        ClusterDescription description = cluster.getCurrentDescription();
        assertNotNull(description);

        // Initialize with exception
        initializerHolder[0].initialize(exception);
        description = cluster.getCurrentDescription();
        assertTrue(description.getServerDescriptions().isEmpty());
        assertEquals(exception, description.getSrvResolutionException());

        // Initialize with servers
        initializerHolder[0].initialize(new HashSet<>(Arrays.asList(firstServer, secondServer)));
        verify(clusterListener, atLeastOnce()).clusterDescriptionChanged(any());

        // Servers notify
        factory.sendNotification(firstServer, SHARD_ROUTER);
        factory.sendNotification(secondServer, SHARD_ROUTER);
        TestServer firstTestServer = factory.getServer(firstServer);
        TestServer secondTestServer = factory.getServer(secondServer);
        ClusterDescription clusterDescription = cluster.getCurrentDescription();

        assertEquals(SHARDED, clusterDescription.getType());
        assertEquals(factory.getDescriptions(firstServer, secondServer),
                ClusterDescriptionHelper.getAll(clusterDescription));
        assertNull(clusterDescription.getSrvResolutionException());
        assertFalse(firstTestServer.isClosed());
        assertFalse(secondTestServer.isClosed());

        // Initialize with different servers
        initializerHolder[0].initialize(Arrays.asList(secondServer, thirdServer));
        factory.sendNotification(secondServer, SHARD_ROUTER);
        TestServer thirdTestServer = factory.getServer(thirdServer);
        clusterDescription = cluster.getCurrentDescription();

        assertEquals(SHARDED, clusterDescription.getType());
        assertEquals(factory.getDescriptions(secondServer, thirdServer),
                ClusterDescriptionHelper.getAll(clusterDescription));
        assertNull(clusterDescription.getSrvResolutionException());
        assertTrue(firstTestServer.isClosed());
        assertFalse(secondTestServer.isClosed());
        assertFalse(thirdTestServer.isClosed());

        // Initialize with another exception (should be ignored)
        initializerHolder[0].initialize(exception);
        clusterDescription = cluster.getCurrentDescription();

        assertEquals(SHARDED, clusterDescription.getType());
        assertEquals(factory.getDescriptions(secondServer, thirdServer),
                ClusterDescriptionHelper.getAll(clusterDescription));
        assertNull(clusterDescription.getSrvResolutionException());
        assertTrue(firstTestServer.isClosed());
        assertFalse(secondTestServer.isClosed());
        assertFalse(thirdTestServer.isClosed());

        // Close cluster
        cluster.close();
        verify(dnsSrvRecordMonitor).close();
    }
}
