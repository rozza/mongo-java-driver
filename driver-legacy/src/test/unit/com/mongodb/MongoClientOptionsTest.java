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

package com.mongodb;

import com.mongodb.connection.ClusterSettings;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerMonitorListener;
import com.mongodb.selector.ServerSelector;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.mongodb.CustomMatchers.isTheSameAs;
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class MongoClientOptionsTest {

    @Test
    @DisplayName("should set the correct default values")
    void shouldSetTheCorrectDefaultValues() {
        MongoClientOptions options = new MongoClientOptions.Builder().build();

        assertNull(options.getApplicationName());
        assertEquals(WriteConcern.ACKNOWLEDGED, options.getWriteConcern());
        assertTrue(options.getRetryWrites());
        assertTrue(options.getRetryReads());
        assertEquals(MongoClientSettings.getDefaultCodecRegistry(), options.getCodecRegistry());
        assertEquals(UuidRepresentation.UNSPECIFIED, options.getUuidRepresentation());
        assertEquals(0, options.getMinConnectionsPerHost());
        assertEquals(100, options.getConnectionsPerHost());
        assertEquals(2, options.getMaxConnecting());
        assertNull(options.getTimeout());
        assertEquals(10000, options.getConnectTimeout());
        assertEquals(ReadPreference.primary(), options.getReadPreference());
        assertNull(options.getServerSelector());
        assertFalse(options.isSslEnabled());
        assertFalse(options.isSslInvalidHostNameAllowed());
        assertNull(options.getSslContext());
        assertEquals(DefaultDBDecoder.FACTORY, options.getDbDecoderFactory());
        assertEquals(DefaultDBEncoder.FACTORY, options.getDbEncoderFactory());
        assertEquals(15, options.getLocalThreshold());
        assertTrue(options.isCursorFinalizerEnabled());
        assertEquals(10000, options.getHeartbeatFrequency());
        assertEquals(500, options.getMinHeartbeatFrequency());
        assertEquals(30000, options.getServerSelectionTimeout());

        assertTrue(options.getCommandListeners().isEmpty());
        assertTrue(options.getClusterListeners().isEmpty());
        assertTrue(options.getConnectionPoolListeners().isEmpty());
        assertTrue(options.getServerListeners().isEmpty());
        assertTrue(options.getServerMonitorListeners().isEmpty());

        assertTrue(options.getCompressorList().isEmpty());
        assertNull(options.getAutoEncryptionSettings());
        assertNull(options.getServerApi());

        assertNull(options.getSrvMaxHosts());
        assertEquals("mongodb", options.getSrvServiceName());
    }

    @Test
    @DisplayName("should handle illegal arguments")
    void shouldHandleIllegalArguments() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();

        assertThrows(IllegalArgumentException.class, () -> builder.dbDecoderFactory(null));
        assertThrows(IllegalArgumentException.class, () -> builder.dbEncoderFactory(null));
    }

    @Test
    @DisplayName("should build with set options")
    void shouldBuildWithSetOptions() throws NoSuchAlgorithmException {
        MyDBEncoderFactory encoderFactory = new MyDBEncoderFactory();
        ServerSelector serverSelector = mock(ServerSelector.class);
        CommandListener commandListener = mock(CommandListener.class);
        ClusterListener clusterListener = mock(ClusterListener.class);
        ServerListener serverListener = mock(ServerListener.class);
        ServerMonitorListener serverMonitorListener = mock(ServerMonitorListener.class);
        AutoEncryptionSettings autoEncryptionSettings = AutoEncryptionSettings.builder()
                .keyVaultNamespace("admin.keys")
                .kmsProviders(Collections.singletonMap("local", Collections.singletonMap("key", new byte[64])))
                .build();
        CodecRegistry codecRegistry = mock(CodecRegistry.class);
        ServerApi serverApi = ServerApi.builder().version(ServerApiVersion.V1).build();

        MongoClientOptions options = MongoClientOptions.builder()
                .applicationName("appName")
                .readPreference(ReadPreference.secondary())
                .retryWrites(true)
                .retryReads(false)
                .writeConcern(WriteConcern.JOURNALED)
                .readConcern(ReadConcern.MAJORITY)
                .minConnectionsPerHost(30)
                .connectionsPerHost(500)
                .timeout(10_000)
                .connectTimeout(100)
                .socketTimeout(700)
                .serverSelector(serverSelector)
                .serverSelectionTimeout(150)
                .maxWaitTime(200)
                .maxConnectionIdleTime(300)
                .maxConnectionLifeTime(400)
                .maxConnecting(1)
                .maintenanceInitialDelay(100)
                .maintenanceFrequency(100)
                .sslEnabled(true)
                .sslInvalidHostNameAllowed(true)
                .sslContext(SSLContext.getDefault())
                .dbDecoderFactory(LazyDBDecoder.FACTORY)
                .heartbeatFrequency(5)
                .minHeartbeatFrequency(11)
                .heartbeatConnectTimeout(15)
                .heartbeatSocketTimeout(20)
                .localThreshold(25)
                .requiredReplicaSetName("test")
                .cursorFinalizerEnabled(false)
                .dbEncoderFactory(encoderFactory)
                .compressorList(Collections.singletonList(MongoCompressor.createZlibCompressor()))
                .autoEncryptionSettings(autoEncryptionSettings)
                .codecRegistry(codecRegistry)
                .addCommandListener(commandListener)
                .addClusterListener(clusterListener)
                .addServerListener(serverListener)
                .addServerMonitorListener(serverMonitorListener)
                .uuidRepresentation(UuidRepresentation.C_SHARP_LEGACY)
                .serverApi(serverApi)
                .build();

        assertEquals("appName", options.getApplicationName());
        assertEquals(ReadPreference.secondary(), options.getReadPreference());
        assertEquals(WriteConcern.JOURNALED, options.getWriteConcern());
        assertEquals(ReadConcern.MAJORITY, options.getReadConcern());
        assertEquals(serverSelector, options.getServerSelector());
        assertTrue(options.getRetryWrites());
        assertFalse(options.getRetryReads());
        assertEquals(150, options.getServerSelectionTimeout());
        assertEquals(Long.valueOf(10_000), options.getTimeout());
        assertEquals(200, options.getMaxWaitTime());
        assertEquals(300, options.getMaxConnectionIdleTime());
        assertEquals(400, options.getMaxConnectionLifeTime());
        assertEquals(1, options.getMaxConnecting());
        assertEquals(100, options.getMaintenanceInitialDelay());
        assertEquals(100, options.getMaintenanceFrequency());
        assertEquals(30, options.getMinConnectionsPerHost());
        assertEquals(500, options.getConnectionsPerHost());
        assertEquals(100, options.getConnectTimeout());
        assertEquals(700, options.getSocketTimeout());
        assertTrue(options.isSslEnabled());
        assertTrue(options.isSslInvalidHostNameAllowed());
        assertEquals(SSLContext.getDefault(), options.getSslContext());
        assertEquals(LazyDBDecoder.FACTORY, options.getDbDecoderFactory());
        assertEquals(encoderFactory, options.getDbEncoderFactory());
        assertEquals(5, options.getHeartbeatFrequency());
        assertEquals(11, options.getMinHeartbeatFrequency());
        assertEquals(15, options.getHeartbeatConnectTimeout());
        assertEquals(20, options.getHeartbeatSocketTimeout());
        assertEquals(25, options.getLocalThreshold());
        assertEquals("test", options.getRequiredReplicaSetName());
        assertFalse(options.isCursorFinalizerEnabled());
        assertEquals(Collections.singletonList(MongoCompressor.createZlibCompressor()), options.getCompressorList());
        assertEquals(autoEncryptionSettings, options.getAutoEncryptionSettings());
        assertEquals(Collections.singletonList(clusterListener), options.getClusterListeners());
        assertEquals(Collections.singletonList(commandListener), options.getCommandListeners());
        assertEquals(Collections.singletonList(serverListener), options.getServerListeners());
        assertEquals(Collections.singletonList(serverMonitorListener), options.getServerMonitorListeners());
        assertEquals(UuidRepresentation.C_SHARP_LEGACY, options.getUuidRepresentation());
        assertEquals(serverApi, options.getServerApi());

        // Test asMongoClientSettings
        MongoCredential credential = MongoCredential.createCredential("user1", "app1", "pwd".toCharArray());
        MongoClientSettings settings = options.asMongoClientSettings(
                Collections.singletonList(new ServerAddress("host1")), null, SINGLE, credential);

        assertEquals(credential, settings.getCredential());
        assertEquals(ReadPreference.secondary(), settings.getReadPreference());
        assertEquals("appName", settings.getApplicationName());
        assertEquals(WriteConcern.JOURNALED, settings.getWriteConcern());
        assertTrue(settings.getRetryWrites());
        assertFalse(settings.getRetryReads());
        assertEquals(autoEncryptionSettings, settings.getAutoEncryptionSettings());
        assertEquals(codecRegistry, settings.getCodecRegistry());
        assertEquals(Collections.singletonList(commandListener), settings.getCommandListeners());
        assertEquals(Collections.singletonList(MongoCompressor.createZlibCompressor()), settings.getCompressorList());
        assertEquals(ReadConcern.MAJORITY, settings.getReadConcern());
        assertEquals(UuidRepresentation.C_SHARP_LEGACY, settings.getUuidRepresentation());
        assertEquals(serverApi, settings.getServerApi());
        assertEquals(Long.valueOf(10_000), settings.getTimeout(TimeUnit.MILLISECONDS));

        // Test builder from settings
        MongoClientOptions optionsFromSettings = MongoClientOptions.builder(settings).build();

        assertEquals("appName", optionsFromSettings.getApplicationName());
        assertEquals(ReadPreference.secondary(), optionsFromSettings.getReadPreference());
        assertEquals(WriteConcern.JOURNALED, optionsFromSettings.getWriteConcern());
        assertEquals(ReadConcern.MAJORITY, optionsFromSettings.getReadConcern());
        assertEquals(serverSelector, optionsFromSettings.getServerSelector());
        assertTrue(optionsFromSettings.getRetryWrites());
        assertFalse(optionsFromSettings.getRetryReads());
        assertEquals(150, optionsFromSettings.getServerSelectionTimeout());
        assertEquals(200, optionsFromSettings.getMaxWaitTime());
        assertEquals(300, optionsFromSettings.getMaxConnectionIdleTime());
        assertEquals(400, optionsFromSettings.getMaxConnectionLifeTime());
        assertEquals(settings.getConnectionPoolSettings().getMaxConnecting(), optionsFromSettings.getMaxConnecting());
        assertEquals(100, optionsFromSettings.getMaintenanceInitialDelay());
        assertEquals(settings.getConnectionPoolSettings().getMaintenanceInitialDelay(TimeUnit.MILLISECONDS),
                optionsFromSettings.getMaintenanceInitialDelay());
        assertEquals(100, optionsFromSettings.getMaintenanceFrequency());
        assertEquals(settings.getConnectionPoolSettings().getMaintenanceFrequency(TimeUnit.MILLISECONDS),
                optionsFromSettings.getMaintenanceFrequency());
        assertEquals(30, optionsFromSettings.getMinConnectionsPerHost());
        assertEquals(500, optionsFromSettings.getConnectionsPerHost());
        assertEquals(100, optionsFromSettings.getConnectTimeout());
        assertEquals(700, optionsFromSettings.getSocketTimeout());
        assertTrue(optionsFromSettings.isSslEnabled());
        assertTrue(optionsFromSettings.isSslInvalidHostNameAllowed());
        assertEquals(SSLContext.getDefault(), optionsFromSettings.getSslContext());
        assertEquals(5, optionsFromSettings.getHeartbeatFrequency());
        assertEquals(11, optionsFromSettings.getMinHeartbeatFrequency());
        assertEquals(15, optionsFromSettings.getHeartbeatConnectTimeout());
        assertEquals(20, optionsFromSettings.getHeartbeatSocketTimeout());
        assertEquals(25, optionsFromSettings.getLocalThreshold());
        assertEquals("test", optionsFromSettings.getRequiredReplicaSetName());
        assertEquals(Collections.singletonList(MongoCompressor.createZlibCompressor()), optionsFromSettings.getCompressorList());
        assertEquals(autoEncryptionSettings, optionsFromSettings.getAutoEncryptionSettings());
        assertEquals(Collections.singletonList(clusterListener), optionsFromSettings.getClusterListeners());
        assertEquals(Collections.singletonList(commandListener), optionsFromSettings.getCommandListeners());
        assertEquals(Collections.singletonList(serverListener), optionsFromSettings.getServerListeners());
        assertEquals(Collections.singletonList(serverMonitorListener), optionsFromSettings.getServerMonitorListeners());
        assertEquals(UuidRepresentation.C_SHARP_LEGACY, optionsFromSettings.getUuidRepresentation());
        assertEquals(serverApi, optionsFromSettings.getServerApi());
    }

    @Test
    @DisplayName("should create settings with SRV protocol")
    void shouldCreateSettingsWithSRVProtocol() {
        assertThrows(IllegalArgumentException.class, () ->
                MongoClientOptions.builder().build().asMongoClientSettings(null, "test3.test.build.10gen.cc", SINGLE, null));

        assertThrows(IllegalArgumentException.class, () ->
                MongoClientOptions.builder().build().asMongoClientSettings(
                        java.util.Arrays.asList(new ServerAddress("host1"), new ServerAddress("host2")),
                        "test3.test.build.10gen.cc", MULTIPLE, null));

        assertThrows(IllegalArgumentException.class, () ->
                MongoClientOptions.builder().build().asMongoClientSettings(null, "test3.test.build.10gen.cc:27018", MULTIPLE, null));

        assertThrows(IllegalArgumentException.class, () ->
                MongoClientOptions.builder().build().asMongoClientSettings(null, "test3.test.build.10gen.cc:27017", MULTIPLE, null));

        MongoClientSettings settings = MongoClientOptions.builder().build()
                .asMongoClientSettings(null, "test3.test.build.10gen.cc", MULTIPLE, null);
        assertEquals(ClusterSettings.builder().srvHost("test3.test.build.10gen.cc").build(), settings.getClusterSettings());

        MongoClientOptions srvOptions = MongoClientOptions.builder()
                .srvServiceName("test")
                .srvMaxHosts(4)
                .build();
        settings = srvOptions.asMongoClientSettings(null, "test3.test.build.10gen.cc", MULTIPLE, null);

        assertEquals(ClusterSettings.builder().srvHost("test3.test.build.10gen.cc")
                .srvServiceName("test")
                .srvMaxHosts(4)
                .build(), settings.getClusterSettings());
        assertEquals("test", srvOptions.getSrvServiceName());
        assertEquals(Integer.valueOf(4), srvOptions.getSrvMaxHosts());
    }

    @Test
    @DisplayName("should be easy to create new options from existing")
    void shouldBeEasyToCreateNewOptionsFromExisting() throws NoSuchAlgorithmException {
        MongoClientOptions options = MongoClientOptions.builder()
                .applicationName("appName")
                .readPreference(ReadPreference.secondary())
                .retryReads(true)
                .uuidRepresentation(UuidRepresentation.STANDARD)
                .writeConcern(WriteConcern.JOURNALED)
                .minConnectionsPerHost(30)
                .connectionsPerHost(500)
                .timeout(10_000)
                .connectTimeout(100)
                .socketTimeout(700)
                .serverSelectionTimeout(150)
                .maxWaitTime(200)
                .maxConnectionIdleTime(300)
                .maxConnectionLifeTime(400)
                .maxConnecting(1)
                .sslEnabled(true)
                .sslInvalidHostNameAllowed(true)
                .sslContext(SSLContext.getDefault())
                .dbDecoderFactory(LazyDBDecoder.FACTORY)
                .heartbeatFrequency(5)
                .minHeartbeatFrequency(11)
                .heartbeatConnectTimeout(15)
                .heartbeatSocketTimeout(20)
                .localThreshold(25)
                .requiredReplicaSetName("test")
                .cursorFinalizerEnabled(false)
                .dbEncoderFactory(new MyDBEncoderFactory())
                .addCommandListener(mock(CommandListener.class))
                .addConnectionPoolListener(mock(ConnectionPoolListener.class))
                .addClusterListener(mock(ClusterListener.class))
                .addServerListener(mock(ServerListener.class))
                .addServerMonitorListener(mock(ServerMonitorListener.class))
                .compressorList(Collections.singletonList(MongoCompressor.createZlibCompressor()))
                .autoEncryptionSettings(AutoEncryptionSettings.builder()
                        .keyVaultNamespace("admin.keys")
                        .kmsProviders(Collections.singletonMap("local", Collections.singletonMap("key", new byte[64])))
                        .build())
                .serverApi(ServerApi.builder().version(ServerApiVersion.V1).build())
                .build();

        assertThat(MongoClientOptions.builder(options).build(), isTheSameAs(options));
    }

    @Test
    @DisplayName("applicationName can be 128 bytes when encoded as UTF-8")
    void applicationNameCanBe128Bytes() {
        String applicationName = new String(new char[126]).replace('\0', 'a') + "\u00A0";

        MongoClientOptions options = MongoClientOptions.builder().applicationName(applicationName).build();

        assertEquals(applicationName, options.getApplicationName());
    }

    @Test
    @DisplayName("should throw IllegalArgumentException if applicationName exceeds 128 bytes when encoded as UTF-8")
    void shouldThrowIfApplicationNameExceeds128Bytes() {
        String applicationName = new String(new char[127]).replace('\0', 'a') + "\u00A0";

        assertThrows(IllegalArgumentException.class,
                () -> MongoClientOptions.builder().applicationName(applicationName));
    }

    @Test
    @DisplayName("should add command listeners")
    void shouldAddCommandListeners() {
        CommandListener commandListenerOne = mock(CommandListener.class);
        CommandListener commandListenerTwo = mock(CommandListener.class);
        CommandListener commandListenerThree = mock(CommandListener.class);

        MongoClientOptions options = MongoClientOptions.builder().build();
        assertEquals(0, options.getCommandListeners().size());

        options = MongoClientOptions.builder()
                .addCommandListener(commandListenerOne)
                .build();
        assertEquals(1, options.getCommandListeners().size());
        assertSame(commandListenerOne, options.getCommandListeners().get(0));

        options = MongoClientOptions.builder()
                .addCommandListener(commandListenerOne)
                .addCommandListener(commandListenerTwo)
                .build();
        assertEquals(2, options.getCommandListeners().size());
        assertSame(commandListenerOne, options.getCommandListeners().get(0));
        assertSame(commandListenerTwo, options.getCommandListeners().get(1));

        MongoClientOptions copiedOptions = MongoClientOptions.builder(options).addCommandListener(commandListenerThree).build();
        assertEquals(3, copiedOptions.getCommandListeners().size());
        assertSame(commandListenerOne, copiedOptions.getCommandListeners().get(0));
        assertSame(commandListenerTwo, copiedOptions.getCommandListeners().get(1));
        assertSame(commandListenerThree, copiedOptions.getCommandListeners().get(2));
        assertEquals(2, options.getCommandListeners().size());
        assertSame(commandListenerOne, options.getCommandListeners().get(0));
        assertSame(commandListenerTwo, options.getCommandListeners().get(1));
    }

    @Test
    @DisplayName("should add connection pool listeners")
    void shouldAddConnectionPoolListeners() {
        ConnectionPoolListener listenerOne = mock(ConnectionPoolListener.class);
        ConnectionPoolListener listenerTwo = mock(ConnectionPoolListener.class);
        ConnectionPoolListener listenerThree = mock(ConnectionPoolListener.class);

        MongoClientOptions options = MongoClientOptions.builder().build();
        assertEquals(0, options.getConnectionPoolListeners().size());

        options = MongoClientOptions.builder()
                .addConnectionPoolListener(listenerOne)
                .build();
        assertEquals(1, options.getConnectionPoolListeners().size());
        assertSame(listenerOne, options.getConnectionPoolListeners().get(0));

        options = MongoClientOptions.builder()
                .addConnectionPoolListener(listenerOne)
                .addConnectionPoolListener(listenerTwo)
                .build();
        assertEquals(2, options.getConnectionPoolListeners().size());
        assertSame(listenerOne, options.getConnectionPoolListeners().get(0));
        assertSame(listenerTwo, options.getConnectionPoolListeners().get(1));

        MongoClientOptions copiedOptions = MongoClientOptions.builder(options).addConnectionPoolListener(listenerThree).build();
        assertEquals(3, copiedOptions.getConnectionPoolListeners().size());
        assertSame(listenerOne, copiedOptions.getConnectionPoolListeners().get(0));
        assertSame(listenerTwo, copiedOptions.getConnectionPoolListeners().get(1));
        assertSame(listenerThree, copiedOptions.getConnectionPoolListeners().get(2));
        assertEquals(2, options.getConnectionPoolListeners().size());
        assertSame(listenerOne, options.getConnectionPoolListeners().get(0));
        assertSame(listenerTwo, options.getConnectionPoolListeners().get(1));
    }

    @Test
    @DisplayName("should add cluster listeners")
    void shouldAddClusterListeners() {
        ClusterListener listenerOne = mock(ClusterListener.class);
        ClusterListener listenerTwo = mock(ClusterListener.class);
        ClusterListener listenerThree = mock(ClusterListener.class);

        MongoClientOptions options = MongoClientOptions.builder().build();
        assertEquals(0, options.getClusterListeners().size());

        options = MongoClientOptions.builder()
                .addClusterListener(listenerOne)
                .build();
        assertEquals(1, options.getClusterListeners().size());
        assertSame(listenerOne, options.getClusterListeners().get(0));

        options = MongoClientOptions.builder()
                .addClusterListener(listenerOne)
                .addClusterListener(listenerTwo)
                .build();
        assertEquals(2, options.getClusterListeners().size());
        assertSame(listenerOne, options.getClusterListeners().get(0));
        assertSame(listenerTwo, options.getClusterListeners().get(1));

        MongoClientOptions copiedOptions = MongoClientOptions.builder(options).addClusterListener(listenerThree).build();
        assertEquals(3, copiedOptions.getClusterListeners().size());
        assertSame(listenerOne, copiedOptions.getClusterListeners().get(0));
        assertSame(listenerTwo, copiedOptions.getClusterListeners().get(1));
        assertSame(listenerThree, copiedOptions.getClusterListeners().get(2));
        assertEquals(2, options.getClusterListeners().size());
        assertSame(listenerOne, options.getClusterListeners().get(0));
        assertSame(listenerTwo, options.getClusterListeners().get(1));
    }

    @Test
    @DisplayName("should add server listeners")
    void shouldAddServerListeners() {
        ServerListener listenerOne = mock(ServerListener.class);
        ServerListener listenerTwo = mock(ServerListener.class);
        ServerListener listenerThree = mock(ServerListener.class);

        MongoClientOptions options = MongoClientOptions.builder().build();
        assertEquals(0, options.getServerListeners().size());

        options = MongoClientOptions.builder()
                .addServerListener(listenerOne)
                .build();
        assertEquals(1, options.getServerListeners().size());
        assertSame(listenerOne, options.getServerListeners().get(0));

        options = MongoClientOptions.builder()
                .addServerListener(listenerOne)
                .addServerListener(listenerTwo)
                .build();
        assertEquals(2, options.getServerListeners().size());
        assertSame(listenerOne, options.getServerListeners().get(0));
        assertSame(listenerTwo, options.getServerListeners().get(1));

        MongoClientOptions copiedOptions = MongoClientOptions.builder(options).addServerListener(listenerThree).build();
        assertEquals(3, copiedOptions.getServerListeners().size());
        assertSame(listenerOne, copiedOptions.getServerListeners().get(0));
        assertSame(listenerTwo, copiedOptions.getServerListeners().get(1));
        assertSame(listenerThree, copiedOptions.getServerListeners().get(2));
        assertEquals(2, options.getServerListeners().size());
        assertSame(listenerOne, options.getServerListeners().get(0));
        assertSame(listenerTwo, options.getServerListeners().get(1));
    }

    @Test
    @DisplayName("should add server monitor listeners")
    void shouldAddServerMonitorListeners() {
        ServerMonitorListener listenerOne = mock(ServerMonitorListener.class);
        ServerMonitorListener listenerTwo = mock(ServerMonitorListener.class);
        ServerMonitorListener listenerThree = mock(ServerMonitorListener.class);

        MongoClientOptions options = MongoClientOptions.builder().build();
        assertEquals(0, options.getServerMonitorListeners().size());

        options = MongoClientOptions.builder()
                .addServerMonitorListener(listenerOne)
                .build();
        assertEquals(1, options.getServerMonitorListeners().size());
        assertSame(listenerOne, options.getServerMonitorListeners().get(0));

        options = MongoClientOptions.builder()
                .addServerMonitorListener(listenerOne)
                .addServerMonitorListener(listenerTwo)
                .build();
        assertEquals(2, options.getServerMonitorListeners().size());
        assertSame(listenerOne, options.getServerMonitorListeners().get(0));
        assertSame(listenerTwo, options.getServerMonitorListeners().get(1));

        MongoClientOptions copiedOptions = MongoClientOptions.builder(options).addServerMonitorListener(listenerThree).build();
        assertEquals(3, copiedOptions.getServerMonitorListeners().size());
        assertSame(listenerOne, copiedOptions.getServerMonitorListeners().get(0));
        assertSame(listenerTwo, copiedOptions.getServerMonitorListeners().get(1));
        assertSame(listenerThree, copiedOptions.getServerMonitorListeners().get(2));
        assertEquals(2, options.getServerMonitorListeners().size());
        assertSame(listenerOne, options.getServerMonitorListeners().get(0));
        assertSame(listenerTwo, options.getServerMonitorListeners().get(1));
    }

    @Test
    @DisplayName("builder should copy all values from the existing MongoClientOptions")
    void builderShouldCopyAllValuesFromExistingOptions() throws NoSuchAlgorithmException {
        MongoClientOptions options = MongoClientOptions.builder()
                .applicationName("appName")
                .readPreference(ReadPreference.secondary())
                .writeConcern(WriteConcern.JOURNALED)
                .retryWrites(true)
                .retryReads(true)
                .uuidRepresentation(UuidRepresentation.STANDARD)
                .minConnectionsPerHost(30)
                .connectionsPerHost(500)
                .timeout(10_000)
                .connectTimeout(100)
                .socketTimeout(700)
                .serverSelectionTimeout(150)
                .maxWaitTime(200)
                .maxConnectionIdleTime(300)
                .maxConnectionLifeTime(400)
                .maxConnecting(1)
                .sslEnabled(true)
                .sslInvalidHostNameAllowed(true)
                .sslContext(SSLContext.getDefault())
                .dbDecoderFactory(LazyDBDecoder.FACTORY)
                .heartbeatFrequency(5)
                .minHeartbeatFrequency(11)
                .heartbeatConnectTimeout(15)
                .heartbeatSocketTimeout(20)
                .localThreshold(25)
                .requiredReplicaSetName("test")
                .cursorFinalizerEnabled(false)
                .dbEncoderFactory(new MyDBEncoderFactory())
                .addCommandListener(mock(CommandListener.class))
                .addClusterListener(mock(ClusterListener.class))
                .addConnectionPoolListener(mock(ConnectionPoolListener.class))
                .addServerListener(mock(ServerListener.class))
                .addServerMonitorListener(mock(ServerMonitorListener.class))
                .compressorList(Collections.singletonList(MongoCompressor.createZlibCompressor()))
                .autoEncryptionSettings(null)
                .build();

        MongoClientOptions copy = MongoClientOptions.builder(options).build();

        assertEquals(options, copy);
    }

    @Test
    @DisplayName("should allow 0 (infinite) connectionsPerHost")
    void shouldAllow0ConnectionsPerHost() {
        assertEquals(0, MongoClientOptions.builder().connectionsPerHost(0).build().getConnectionsPerHost());
    }

    private static class MyDBEncoderFactory implements DBEncoderFactory {
        @Override
        public DBEncoder create() {
            return new DefaultDBEncoder();
        }
    }
}
