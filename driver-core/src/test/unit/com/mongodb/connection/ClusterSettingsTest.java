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

package com.mongodb.connection;

import com.mongodb.ConnectionString;
import com.mongodb.ServerAddress;
import com.mongodb.UnixServerAddress;
import com.mongodb.event.ClusterListener;
import com.mongodb.internal.selector.WritableServerSelector;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class ClusterSettingsTest {

    private final List<ServerAddress> hosts = Arrays.asList(new ServerAddress("localhost"), new ServerAddress("localhost", 30000));
    private final WritableServerSelector serverSelector = new WritableServerSelector();

    @Test
    void shouldSetAllDefaultValues() {
        ClusterSettings settings = ClusterSettings.builder().build();

        assertEquals(Collections.singletonList(new ServerAddress()), settings.getHosts());
        assertEquals(ClusterConnectionMode.SINGLE, settings.getMode());
        assertEquals(ClusterType.UNKNOWN, settings.getRequiredClusterType());
        assertNull(settings.getRequiredReplicaSetName());
        assertNull(settings.getServerSelector());
        assertEquals(30, settings.getServerSelectionTimeout(TimeUnit.SECONDS));
        assertTrue(settings.getClusterListeners().isEmpty());
        assertNull(settings.getSrvMaxHosts());
        assertEquals("mongodb", settings.getSrvServiceName());
    }

    @Test
    void shouldSetAllProperties() {
        ClusterListener listenerOne = mock(ClusterListener.class);
        ClusterListener listenerTwo = mock(ClusterListener.class);
        ClusterListener listenerThree = mock(ClusterListener.class);

        ClusterSettings settings = ClusterSettings.builder()
                .hosts(hosts)
                .mode(ClusterConnectionMode.MULTIPLE)
                .requiredClusterType(ClusterType.REPLICA_SET)
                .requiredReplicaSetName("foo")
                .localThreshold(1, TimeUnit.SECONDS)
                .serverSelector(serverSelector)
                .serverSelectionTimeout(1, TimeUnit.SECONDS)
                .addClusterListener(listenerOne)
                .addClusterListener(listenerTwo)
                .build();

        assertEquals(hosts, settings.getHosts());
        assertEquals(ClusterConnectionMode.MULTIPLE, settings.getMode());
        assertEquals(ClusterType.REPLICA_SET, settings.getRequiredClusterType());
        assertEquals("foo", settings.getRequiredReplicaSetName());
        assertEquals(serverSelector, settings.getServerSelector());
        assertEquals(1000, settings.getServerSelectionTimeout(TimeUnit.MILLISECONDS));
        assertEquals(Arrays.asList(listenerOne, listenerTwo), settings.getClusterListeners());

        settings = ClusterSettings.builder(settings).clusterListenerList(Collections.singletonList(listenerThree)).build();

        assertEquals(Collections.singletonList(listenerThree), settings.getClusterListeners());
    }

    @Test
    void shouldApplySettings() {
        ClusterListener listenerOne = mock(ClusterListener.class);
        ClusterListener listenerTwo = mock(ClusterListener.class);
        ClusterSettings defaultSettings = ClusterSettings.builder().build();
        ClusterSettings customSettings = ClusterSettings.builder()
                .hosts(hosts)
                .mode(ClusterConnectionMode.MULTIPLE)
                .requiredClusterType(ClusterType.REPLICA_SET)
                .requiredReplicaSetName("foo")
                .serverSelector(serverSelector)
                .localThreshold(10, TimeUnit.MILLISECONDS)
                .serverSelectionTimeout(1, TimeUnit.SECONDS)
                .addClusterListener(listenerOne)
                .addClusterListener(listenerTwo)
                .build();

        assertEquals(customSettings, ClusterSettings.builder().applySettings(customSettings).build());
        assertEquals(defaultSettings, ClusterSettings.builder(customSettings).applySettings(defaultSettings).build());
    }

    @Test
    void shouldApplySettingsForSrv() {
        ClusterSettings defaultSettings = ClusterSettings.builder().build();
        ClusterSettings customSettings = ClusterSettings.builder()
                .hosts(Collections.singletonList(new ServerAddress("localhost")))
                .srvMaxHosts(4)
                .srvServiceName("foo")
                .build();

        assertEquals(customSettings, ClusterSettings.builder().applySettings(customSettings).build());
        assertEquals(defaultSettings, ClusterSettings.builder(customSettings).applySettings(defaultSettings).build());
    }

    @Test
    void whenHostsContainsMoreThanOneElementAndModeIsSingleShouldThrow() {
        assertThrows(IllegalArgumentException.class, () -> {
            ClusterSettings.builder()
                    .hosts(Arrays.asList(new ServerAddress("host1"), new ServerAddress("host2")))
                    .mode(ClusterConnectionMode.SINGLE)
                    .build();
        });
    }

    @Test
    void whenHostsContainsMoreThanOneElementAndModeIsLoadBalancedShouldThrow() {
        assertThrows(IllegalArgumentException.class, () -> {
            ClusterSettings.builder()
                    .hosts(Arrays.asList(new ServerAddress("host1"), new ServerAddress("host2")))
                    .mode(ClusterConnectionMode.LOAD_BALANCED)
                    .build();
        });
    }

    @Test
    void whenSrvHostIsSpecifiedAndModeIsSingleShouldThrow() {
        assertThrows(IllegalArgumentException.class, () -> {
            ClusterSettings.builder()
                    .srvHost("foo.bar.com")
                    .mode(ClusterConnectionMode.SINGLE)
                    .build();
        });
    }

    @Test
    void whenSrvHostIsSpecifiedShouldSetModeToMultipleIfModeIsNotConfigured() {
        ClusterSettings settings = ClusterSettings.builder()
                .srvHost("foo.bar.com")
                .build();

        assertEquals("foo.bar.com", settings.getSrvHost());
        assertEquals(ClusterConnectionMode.MULTIPLE, settings.getMode());
    }

    @Test
    void whenSrvHostIsSpecifiedShouldUseConfiguredModeIsLoadBalanced() {
        ClusterSettings settings = ClusterSettings.builder()
                .srvHost("foo.bar.com")
                .mode(ClusterConnectionMode.LOAD_BALANCED)
                .build();

        assertEquals("foo.bar.com", settings.getSrvHost());
        assertEquals(ClusterConnectionMode.LOAD_BALANCED, settings.getMode());
    }

    @Test
    void whenSrvHostContainsColonShouldThrow() {
        assertThrows(IllegalArgumentException.class, () -> {
            ClusterSettings.builder()
                    .srvHost("foo.bar.com:27017")
                    .build();
        });
    }

    @Test
    void whenConnectionStringIsAppliedToBuilderAllPropertiesShouldBeSet() {
        // Single host
        ClusterSettings settings = ClusterSettings.builder()
                .requiredReplicaSetName("test")
                .applyConnectionString(new ConnectionString("mongodb://example.com:27018"))
                .build();

        assertEquals(ClusterConnectionMode.SINGLE, settings.getMode());
        assertEquals(Collections.singletonList(new ServerAddress("example.com:27018")), settings.getHosts());
        assertEquals(ClusterType.UNKNOWN, settings.getRequiredClusterType());
        assertNull(settings.getRequiredReplicaSetName());
        assertNull(settings.getSrvMaxHosts());
        assertEquals("mongodb", settings.getSrvServiceName());

        // With replica set name set after connection string
        settings = ClusterSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://example.com:27018"))
                .requiredReplicaSetName("test")
                .build();

        assertEquals(ClusterConnectionMode.MULTIPLE, settings.getMode());
        assertEquals(Collections.singletonList(new ServerAddress("example.com:27018")), settings.getHosts());
        assertEquals(ClusterType.REPLICA_SET, settings.getRequiredClusterType());
        assertEquals("test", settings.getRequiredReplicaSetName());
        assertNull(settings.getSrvMaxHosts());
        assertEquals("mongodb", settings.getSrvServiceName());

        // With directConnection=false, single host
        settings = ClusterSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://example.com:27018/?directConnection=false"))
                .build();

        assertEquals(ClusterConnectionMode.MULTIPLE, settings.getMode());
        assertEquals(Collections.singletonList(new ServerAddress("example.com:27018")), settings.getHosts());
        assertEquals(ClusterType.UNKNOWN, settings.getRequiredClusterType());
        assertNull(settings.getRequiredReplicaSetName());
        assertNull(settings.getSrvMaxHosts());
        assertEquals("mongodb", settings.getSrvServiceName());

        // With directConnection=false, multiple hosts
        settings = ClusterSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://example.com:27017,example.com:27018/?directConnection=false"))
                .build();

        assertEquals(ClusterConnectionMode.MULTIPLE, settings.getMode());
        assertEquals(Arrays.asList(new ServerAddress("example.com:27017"), new ServerAddress("example.com:27018")), settings.getHosts());
        assertEquals(ClusterType.UNKNOWN, settings.getRequiredClusterType());
        assertNull(settings.getRequiredReplicaSetName());
        assertNull(settings.getSrvMaxHosts());
        assertEquals("mongodb", settings.getSrvServiceName());

        // With directConnection=true
        settings = ClusterSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://example.com:27018/?directConnection=true"))
                .build();

        assertEquals(ClusterConnectionMode.SINGLE, settings.getMode());
        assertEquals(Collections.singletonList(new ServerAddress("example.com:27018")), settings.getHosts());
        assertEquals(ClusterType.UNKNOWN, settings.getRequiredClusterType());
        assertNull(settings.getRequiredReplicaSetName());
        assertNull(settings.getSrvMaxHosts());
        assertEquals("mongodb", settings.getSrvServiceName());

        // Mode overridden by connection string with replicaSet
        settings = ClusterSettings.builder()
                .mode(ClusterConnectionMode.SINGLE)
                .applyConnectionString(new ConnectionString("mongodb://example.com:27018/?replicaSet=test"))
                .build();

        assertEquals(ClusterConnectionMode.MULTIPLE, settings.getMode());
        assertEquals(Collections.singletonList(new ServerAddress("example.com:27018")), settings.getHosts());
        assertEquals(ClusterType.REPLICA_SET, settings.getRequiredClusterType());
        assertEquals("test", settings.getRequiredReplicaSetName());
        assertNull(settings.getSrvMaxHosts());
        assertEquals("mongodb", settings.getSrvServiceName());

        // Multiple hosts
        settings = ClusterSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://example.com:27018,example.com:27019"))
                .build();

        assertEquals(ClusterConnectionMode.MULTIPLE, settings.getMode());
        assertEquals(Arrays.asList(new ServerAddress("example.com:27018"), new ServerAddress("example.com:27019")), settings.getHosts());
        assertEquals(ClusterType.UNKNOWN, settings.getRequiredClusterType());
        assertNull(settings.getRequiredReplicaSetName());
        assertNull(settings.getSrvMaxHosts());
        assertEquals("mongodb", settings.getSrvServiceName());

        // Server selection timeout
        settings = ClusterSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://example.com:27018/?serverSelectionTimeoutMS=50000"))
                .build();

        assertEquals(50000, settings.getServerSelectionTimeout(TimeUnit.MILLISECONDS));

        // Local threshold
        settings = ClusterSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://localhost/?localThresholdMS=99"))
                .build();

        assertEquals(99, settings.getLocalThreshold(TimeUnit.MILLISECONDS));

        // Load balanced
        settings = ClusterSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://example.com:27018/?loadBalanced=true"))
                .build();

        assertEquals(ClusterConnectionMode.LOAD_BALANCED, settings.getMode());
        assertEquals(Collections.singletonList(new ServerAddress("example.com:27018")), settings.getHosts());
    }

    @Test
    void whenClusterTypeIsUnknownAndReplicaSetNameIsSetShouldSetClusterTypeToReplicaSetAndModeToMultiple() {
        ClusterSettings settings = ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress()))
                .requiredReplicaSetName("yeah").build();

        assertEquals(ClusterType.REPLICA_SET, settings.getRequiredClusterType());
        assertEquals(ClusterConnectionMode.MULTIPLE, settings.getMode());
    }

    @Test
    void connectionModeShouldDefaultToSingleIfReplicaSetNameIsNotSetAndOneHostOrMultipleIfMore() {
        ClusterSettings settings = ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress())).build();
        assertEquals(ClusterConnectionMode.SINGLE, settings.getMode());

        settings = ClusterSettings.builder().hosts(hosts).build();
        assertEquals(ClusterConnectionMode.MULTIPLE, settings.getMode());
    }

    @Test
    void whenAValidModeIsSpecifiedShouldUseIt() {
        ClusterConnectionMode mode = ClusterConnectionMode.LOAD_BALANCED;
        ClusterSettings settings = ClusterSettings.builder().mode(mode).build();

        assertEquals(mode, settings.getMode());
    }

    @Test
    void whenModeIsSingleAndHostsSizeIsGreaterThanOneShouldThrow() {
        assertThrows(IllegalArgumentException.class, () ->
                ClusterSettings.builder()
                        .hosts(Arrays.asList(new ServerAddress(), new ServerAddress("other")))
                        .mode(ClusterConnectionMode.SINGLE)
                        .build());

        assertThrows(IllegalArgumentException.class, () ->
                ClusterSettings.builder()
                        .applyConnectionString(new ConnectionString("mongodb://host1,host2/"))
                        .mode(ClusterConnectionMode.SINGLE)
                        .build());
    }

    @Test
    void whenClusterTypeIsStandaloneAndMultipleHostsAreSpecifiedShouldThrow() {
        assertThrows(IllegalArgumentException.class, () ->
                ClusterSettings.builder()
                        .hosts(Arrays.asList(new ServerAddress(), new ServerAddress("other")))
                        .requiredClusterType(ClusterType.STANDALONE)
                        .build());
    }

    @Test
    void whenReplicaSetNameIsSpecifiedAndTypeIsStandaloneShouldThrow() {
        assertThrows(IllegalArgumentException.class, () ->
                ClusterSettings.builder()
                        .hosts(Arrays.asList(new ServerAddress(), new ServerAddress("other")))
                        .requiredReplicaSetName("foo")
                        .requiredClusterType(ClusterType.STANDALONE)
                        .build());
    }

    @Test
    void whenReplicaSetNameIsSpecifiedAndTypeIsShardedShouldThrow() {
        assertThrows(IllegalArgumentException.class, () ->
                ClusterSettings.builder()
                        .hosts(Arrays.asList(new ServerAddress(), new ServerAddress("other")))
                        .requiredReplicaSetName("foo")
                        .requiredClusterType(ClusterType.SHARDED)
                        .build());
    }

    @Test
    void shouldThrowIfHostsListIsNull() {
        assertThrows(IllegalArgumentException.class, () -> ClusterSettings.builder().hosts(null).build());
    }

    @Test
    void shouldThrowIfHostsListIsEmpty() {
        assertThrows(IllegalArgumentException.class, () -> ClusterSettings.builder().hosts(Collections.emptyList()).build());
    }

    @Test
    void shouldThrowIfHostsListContainsNullValue() {
        assertThrows(IllegalArgumentException.class, () -> ClusterSettings.builder().hosts(Collections.singletonList(null)).build());
    }

    @Test
    void shouldRemoveDuplicateHosts() {
        ClusterSettings settings = ClusterSettings.builder().hosts(Arrays.asList(
                new ServerAddress("server1"),
                new ServerAddress("server2"),
                new ServerAddress("server1"))).build();

        assertEquals(Arrays.asList(new ServerAddress("server1"), new ServerAddress("server2")), settings.getHosts());
    }

    @Test
    void identicalSettingsShouldBeEqual() {
        assertEquals(
                ClusterSettings.builder().hosts(hosts).build(),
                ClusterSettings.builder().hosts(hosts).build());
        assertEquals(
                ClusterSettings.builder()
                        .hosts(hosts)
                        .mode(ClusterConnectionMode.MULTIPLE)
                        .requiredClusterType(ClusterType.REPLICA_SET)
                        .requiredReplicaSetName("foo")
                        .serverSelector(serverSelector)
                        .serverSelectionTimeout(1, TimeUnit.SECONDS)
                        .build(),
                ClusterSettings.builder()
                        .hosts(hosts)
                        .mode(ClusterConnectionMode.MULTIPLE)
                        .requiredClusterType(ClusterType.REPLICA_SET)
                        .requiredReplicaSetName("foo")
                        .serverSelector(serverSelector)
                        .serverSelectionTimeout(1, TimeUnit.SECONDS)
                        .build());
    }

    @Test
    void differentSettingsShouldNotBeEqual() {
        assertNotEquals(
                ClusterSettings.builder()
                        .hosts(hosts)
                        .mode(ClusterConnectionMode.MULTIPLE)
                        .requiredClusterType(ClusterType.REPLICA_SET)
                        .requiredReplicaSetName("foo")
                        .serverSelector(serverSelector)
                        .serverSelectionTimeout(1, TimeUnit.SECONDS)
                        .build(),
                ClusterSettings.builder().hosts(hosts).build());
    }

    @Test
    void identicalSettingsShouldHaveSameHashCode() {
        assertEquals(
                ClusterSettings.builder().hosts(hosts).build().hashCode(),
                ClusterSettings.builder().hosts(hosts).build().hashCode());
        assertEquals(
                ClusterSettings.builder()
                        .hosts(hosts)
                        .mode(ClusterConnectionMode.MULTIPLE)
                        .requiredClusterType(ClusterType.REPLICA_SET)
                        .requiredReplicaSetName("foo")
                        .serverSelector(serverSelector)
                        .serverSelectionTimeout(1, TimeUnit.SECONDS)
                        .build().hashCode(),
                ClusterSettings.builder()
                        .hosts(hosts)
                        .mode(ClusterConnectionMode.MULTIPLE)
                        .requiredClusterType(ClusterType.REPLICA_SET)
                        .requiredReplicaSetName("foo")
                        .serverSelector(serverSelector)
                        .serverSelectionTimeout(1, TimeUnit.SECONDS)
                        .build().hashCode());
    }

    @Test
    void differentSettingsShouldHaveDifferentHashCodes() {
        assertNotEquals(
                ClusterSettings.builder()
                        .hosts(hosts)
                        .mode(ClusterConnectionMode.MULTIPLE)
                        .requiredClusterType(ClusterType.REPLICA_SET)
                        .requiredReplicaSetName("foo")
                        .serverSelector(serverSelector)
                        .serverSelectionTimeout(1, TimeUnit.SECONDS)
                        .build().hashCode(),
                ClusterSettings.builder().hosts(hosts).build().hashCode());
    }

    @Test
    void shouldReplaceUnknownServerAddressSubclassInstancesWithServerAddress() {
        ClusterSettings settings = ClusterSettings.builder().hosts(Arrays.asList(
                new ServerAddress("server1"),
                new ServerAddressSubclass("server2"),
                new UnixServerAddress("mongodb.sock"))).build();

        assertEquals(Arrays.asList(
                new ServerAddress("server1"),
                new ServerAddress("server2"),
                new UnixServerAddress("mongodb.sock")),
                settings.getHosts());
    }

    @Test
    void listOfClusterListenersShouldBeUnmodifiable() {
        ClusterSettings settings = ClusterSettings.builder().hosts(hosts).build();

        assertThrows(UnsupportedOperationException.class, () -> settings.getClusterListeners().add(mock(ClusterListener.class)));
    }

    @Test
    void clusterListenerShouldNotBeNull() {
        assertThrows(IllegalArgumentException.class, () -> ClusterSettings.builder().addClusterListener(null));
    }

    static class ServerAddressSubclass extends ServerAddress {
        ServerAddressSubclass(final String host) {
            super(host);
        }
    }
}
