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

import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ProxySettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.TransportSettings;
import com.mongodb.event.CommandListener;
import com.mongodb.spi.dns.DnsClient;
import com.mongodb.spi.dns.InetAddressResolver;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.mongodb.CustomMatchers.isTheSameAs;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class MongoClientSettingsTest {

    @Test
    @DisplayName("should set the correct default values")
    void shouldSetCorrectDefaults() {
        MongoClientSettings settings = MongoClientSettings.builder().build();

        assertEquals(WriteConcern.ACKNOWLEDGED, settings.getWriteConcern());
        assertTrue(settings.getRetryWrites());
        assertTrue(settings.getRetryReads());
        assertEquals(ReadConcern.DEFAULT, settings.getReadConcern());
        assertEquals(ReadPreference.primary(), settings.getReadPreference());
        assertTrue(settings.getCommandListeners().isEmpty());
        assertNull(settings.getApplicationName());
        assertEquals(LoggerSettings.builder().build(), settings.getLoggerSettings());
        assertEquals(ClusterSettings.builder().build(), settings.getClusterSettings());
        assertEquals(ConnectionPoolSettings.builder().build(), settings.getConnectionPoolSettings());
        assertEquals(SocketSettings.builder().build(), settings.getSocketSettings());
        assertEquals(ProxySettings.builder().build(), settings.getSocketSettings().getProxySettings());
        assertEquals(SocketSettings.builder().readTimeout(10000, TimeUnit.MILLISECONDS).build(),
                settings.getHeartbeatSocketSettings());
        assertEquals(ServerSettings.builder().build(), settings.getServerSettings());
        assertNull(settings.getTransportSettings());
        assertEquals(Collections.emptyList(), settings.getCompressorList());
        assertNull(settings.getCredential());
        assertEquals(UuidRepresentation.UNSPECIFIED, settings.getUuidRepresentation());
        assertNull(settings.getContextProvider());
        assertNull(settings.getDnsClient());
        assertNull(settings.getInetAddressResolver());
        assertNull(settings.getTimeout(TimeUnit.MILLISECONDS));
    }

    @Test
    @DisplayName("should handle illegal arguments")
    void shouldHandleIllegalArguments() {
        MongoClientSettings.Builder builder = MongoClientSettings.builder();

        assertThrows(IllegalArgumentException.class, () -> builder.readPreference(null));
        assertThrows(IllegalArgumentException.class, () -> builder.writeConcern(null));
        assertThrows(IllegalArgumentException.class, () -> builder.credential(null));
        assertThrows(IllegalArgumentException.class, () -> builder.codecRegistry(null));
        assertThrows(IllegalArgumentException.class, () -> builder.transportSettings(null));
        assertThrows(IllegalArgumentException.class, () -> builder.addCommandListener(null));
        assertThrows(IllegalArgumentException.class, () -> builder.compressorList(null));
        assertThrows(IllegalArgumentException.class, () -> builder.uuidRepresentation(null));
    }

    @Test
    @DisplayName("should build with set configuration")
    void shouldBuildWithSetConfiguration() {
        TransportSettings transportSettings = TransportSettings.nettyBuilder().build();
        MongoCredential credential = MongoCredential.createMongoX509Credential("test");
        CodecRegistry codecRegistry = mock(CodecRegistry.class);
        CommandListener commandListener = mock(CommandListener.class);
        ClusterSettings clusterSettings = ClusterSettings.builder()
                .hosts(Collections.singletonList(new ServerAddress("localhost")))
                .requiredReplicaSetName("test").build();
        ContextProvider contextProvider = mock(ContextProvider.class);
        DnsClient dnsClient = mock(DnsClient.class);
        InetAddressResolver inetAddressResolver = mock(InetAddressResolver.class);

        MongoClientSettings settings = MongoClientSettings.builder()
                .readPreference(ReadPreference.secondary())
                .writeConcern(WriteConcern.JOURNALED)
                .retryWrites(true)
                .retryReads(true)
                .readConcern(ReadConcern.LOCAL)
                .applicationName("app1")
                .addCommandListener(commandListener)
                .credential(credential)
                .codecRegistry(codecRegistry)
                .applyToClusterSettings(builder -> builder.applySettings(clusterSettings))
                .transportSettings(transportSettings)
                .compressorList(Collections.singletonList(MongoCompressor.createZlibCompressor()))
                .uuidRepresentation(UuidRepresentation.STANDARD)
                .contextProvider(contextProvider)
                .dnsClient(dnsClient)
                .inetAddressResolver(inetAddressResolver)
                .timeout(1000, TimeUnit.SECONDS)
                .build();

        assertEquals(ReadPreference.secondary(), settings.getReadPreference());
        assertEquals(WriteConcern.JOURNALED, settings.getWriteConcern());
        assertTrue(settings.getRetryWrites());
        assertTrue(settings.getRetryReads());
        assertEquals(ReadConcern.LOCAL, settings.getReadConcern());
        assertEquals("app1", settings.getApplicationName());
        assertEquals(SocketSettings.builder().build(), settings.getSocketSettings());
        assertEquals(SocketSettings.builder().readTimeout(10000, TimeUnit.MILLISECONDS).build(),
                settings.getHeartbeatSocketSettings());
        assertEquals(commandListener, settings.getCommandListeners().get(0));
        assertEquals(codecRegistry, settings.getCodecRegistry());
        assertEquals(credential, settings.getCredential());
        assertEquals(clusterSettings, settings.getClusterSettings());
        assertEquals(transportSettings, settings.getTransportSettings());
        assertEquals(Collections.singletonList(MongoCompressor.createZlibCompressor()), settings.getCompressorList());
        assertEquals(UuidRepresentation.STANDARD, settings.getUuidRepresentation());
        assertEquals(contextProvider, settings.getContextProvider());
        assertEquals(dnsClient, settings.getDnsClient());
        assertEquals(inetAddressResolver, settings.getInetAddressResolver());
        assertEquals(Long.valueOf(1_000_000), settings.getTimeout(TimeUnit.MILLISECONDS));
    }

    @Test
    @DisplayName("should be easy to create new settings from existing")
    void shouldCreateFromExisting() {
        MongoClientSettings settings = MongoClientSettings.builder().build();
        assertThat(MongoClientSettings.builder(settings).build(), isTheSameAs(settings));

        MongoCredential credential = MongoCredential.createMongoX509Credential("test");
        CodecRegistry codecRegistry = mock(CodecRegistry.class);
        CommandListener commandListener = mock(CommandListener.class);
        ContextProvider contextProvider = mock(ContextProvider.class);
        DnsClient dnsClient = mock(DnsClient.class);
        InetAddressResolver inetAddressResolver = mock(InetAddressResolver.class);

        settings = MongoClientSettings.builder()
                .heartbeatConnectTimeoutMS(24000)
                .heartbeatSocketTimeoutMS(12000)
                .readPreference(ReadPreference.secondary())
                .writeConcern(WriteConcern.JOURNALED)
                .retryWrites(true)
                .retryReads(true)
                .readConcern(ReadConcern.LOCAL)
                .applicationName("app1")
                .addCommandListener(commandListener)
                .applyToLoggerSettings(builder -> builder.maxDocumentLength(10))
                .applyToClusterSettings(builder -> builder.hosts(Collections.singletonList(new ServerAddress("localhost")))
                        .requiredReplicaSetName("test"))
                .credential(credential)
                .codecRegistry(codecRegistry)
                .compressorList(Collections.singletonList(MongoCompressor.createZlibCompressor()))
                .contextProvider(contextProvider)
                .dnsClient(dnsClient)
                .inetAddressResolver(inetAddressResolver)
                .timeout(0, TimeUnit.SECONDS)
                .build();

        assertThat(MongoClientSettings.builder(settings).build(), isTheSameAs(settings));
    }

    @Test
    @DisplayName("applicationName can be 128 bytes when encoded as UTF-8")
    void applicationNameCanBe128Bytes() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 126; i++) {
            sb.append('a');
        }
        sb.append('\u00A0');
        String applicationName = sb.toString();

        MongoClientSettings settings = MongoClientSettings.builder().applicationName(applicationName).build();
        assertEquals(applicationName, settings.getApplicationName());
    }

    @Test
    @DisplayName("should throw IllegalArgumentException if applicationName exceeds 128 bytes when encoded as UTF-8")
    void shouldThrowForLongApplicationName() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 127; i++) {
            sb.append('a');
        }
        sb.append('\u00A0');
        String applicationName = sb.toString();

        assertThrows(IllegalArgumentException.class,
                () -> MongoClientSettings.builder().applicationName(applicationName));
    }

    @Test
    @DisplayName("should throw an exception if the timeout is invalid")
    void shouldThrowForInvalidTimeout() {
        MongoClientSettings.Builder builder = MongoClientSettings.builder();

        assertThrows(IllegalArgumentException.class, () -> builder.timeout(500, TimeUnit.NANOSECONDS));
        assertThrows(IllegalArgumentException.class, () -> builder.timeout(-1, TimeUnit.SECONDS));
        assertThrows(IllegalStateException.class, () -> {
            ConnectionString connectionString = new ConnectionString("mongodb://localhost/?timeoutMS=-1");
            builder.applyConnectionString(connectionString).build();
        });
    }

    @Test
    @DisplayName("should add command listeners")
    void shouldAddCommandListeners() {
        CommandListener commandListenerOne = mock(CommandListener.class);
        CommandListener commandListenerTwo = mock(CommandListener.class);
        CommandListener commandListenerThree = mock(CommandListener.class);

        MongoClientSettings settings = MongoClientSettings.builder().build();
        assertEquals(0, settings.getCommandListeners().size());

        settings = MongoClientSettings.builder()
                .addCommandListener(commandListenerOne)
                .build();
        assertEquals(1, settings.getCommandListeners().size());
        assertTrue(settings.getCommandListeners().get(0) == commandListenerOne);

        settings = MongoClientSettings.builder()
                .addCommandListener(commandListenerOne)
                .addCommandListener(commandListenerTwo)
                .build();
        assertEquals(2, settings.getCommandListeners().size());
        assertTrue(settings.getCommandListeners().get(0) == commandListenerOne);
        assertTrue(settings.getCommandListeners().get(1) == commandListenerTwo);

        MongoClientSettings copied = MongoClientSettings.builder(settings)
                .addCommandListener(commandListenerThree).build();
        assertEquals(3, copied.getCommandListeners().size());
        assertTrue(copied.getCommandListeners().get(0) == commandListenerOne);
        assertTrue(copied.getCommandListeners().get(1) == commandListenerTwo);
        assertTrue(copied.getCommandListeners().get(2) == commandListenerThree);
        assertEquals(2, settings.getCommandListeners().size());
        assertTrue(settings.getCommandListeners().get(0) == commandListenerOne);
        assertTrue(settings.getCommandListeners().get(1) == commandListenerTwo);
    }

    @Test
    @DisplayName("should build settings from a connection string")
    void shouldBuildFromConnectionString() {
        ConnectionString connectionString = new ConnectionString("mongodb://user:pass@host1:1,host2:2/"
                + "?authMechanism=SCRAM-SHA-1&authSource=test"
                + "&minPoolSize=5&maxPoolSize=10"
                + "&waitQueueTimeoutMS=150&maxIdleTimeMS=200&maxLifeTimeMS=300"
                + "&connectTimeoutMS=2500"
                + "&socketTimeoutMS=5500"
                + "&serverSelectionTimeoutMS=25000"
                + "&localThresholdMS=30"
                + "&heartbeatFrequencyMS=20000"
                + "&appName=MyApp"
                + "&replicaSet=test"
                + "&retryWrites=true"
                + "&retryReads=true"
                + "&ssl=true&sslInvalidHostNameAllowed=true"
                + "&w=majority&wTimeoutMS=2500"
                + "&readPreference=secondary"
                + "&readConcernLevel=majority"
                + "&compressors=zlib&zlibCompressionLevel=5"
                + "&uuidRepresentation=standard"
                + "&timeoutMS=10000"
                + "&proxyHost=proxy.com"
                + "&proxyPort=1080"
                + "&proxyUsername=username"
                + "&proxyPassword=password");

        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connectionString).build();

        MongoClientSettings expected = MongoClientSettings.builder()
                .applyToClusterSettings(builder -> builder
                        .hosts(Arrays.asList(new ServerAddress("host1", 1), new ServerAddress("host2", 2)))
                        .mode(ClusterConnectionMode.MULTIPLE)
                        .requiredReplicaSetName("test")
                        .serverSelectionTimeout(25000, TimeUnit.MILLISECONDS)
                        .localThreshold(30, TimeUnit.MILLISECONDS))
                .applyToConnectionPoolSettings(builder -> builder
                        .minSize(5)
                        .maxSize(10)
                        .maxWaitTime(150, TimeUnit.MILLISECONDS)
                        .maxConnectionLifeTime(300, TimeUnit.MILLISECONDS)
                        .maxConnectionIdleTime(200, TimeUnit.MILLISECONDS))
                .applyToServerSettings(builder -> builder
                        .heartbeatFrequency(20000, TimeUnit.MILLISECONDS))
                .applyToSocketSettings(builder -> builder
                        .connectTimeout(2500, TimeUnit.MILLISECONDS)
                        .readTimeout(5500, TimeUnit.MILLISECONDS)
                        .applyToProxySettings(proxyBuilder -> proxyBuilder
                                .host("proxy.com")
                                .port(1080)
                                .username("username")
                                .password("password")))
                .applyToSslSettings(builder -> builder
                        .enabled(true)
                        .invalidHostNameAllowed(true))
                .readConcern(ReadConcern.MAJORITY)
                .readPreference(ReadPreference.secondary())
                .writeConcern(WriteConcern.MAJORITY.withWTimeout(2500, TimeUnit.MILLISECONDS))
                .applicationName("MyApp")
                .credential(MongoCredential.createScramSha1Credential("user", "test", "pass".toCharArray()))
                .compressorList(Collections.singletonList(
                        MongoCompressor.createZlibCompressor().withProperty(MongoCompressor.LEVEL, 5)))
                .retryWrites(true)
                .retryReads(true)
                .uuidRepresentation(UuidRepresentation.STANDARD)
                .timeout(10000, TimeUnit.MILLISECONDS)
                .build();

        assertThat(settings, isTheSameAs(expected));
    }

    @Test
    @DisplayName("should build settings from a connection string with default values")
    void shouldBuildFromConnectionStringWithDefaults() {
        MongoClientSettings.Builder builder = MongoClientSettings.builder()
                .applyToClusterSettings(b -> b
                        .hosts(Collections.singletonList(new ServerAddress("localhost", 27017)))
                        .mode(ClusterConnectionMode.SINGLE)
                        .serverSelectionTimeout(25000, TimeUnit.MILLISECONDS)
                        .localThreshold(30, TimeUnit.MILLISECONDS))
                .applyToConnectionPoolSettings(b -> b
                        .minSize(5)
                        .maxSize(10)
                        .maxWaitTime(150, TimeUnit.MILLISECONDS)
                        .maxConnectionLifeTime(300, TimeUnit.MILLISECONDS)
                        .maxConnectionIdleTime(200, TimeUnit.MILLISECONDS))
                .applyToServerSettings(b -> b
                        .heartbeatFrequency(20000, TimeUnit.MILLISECONDS))
                .applyToSocketSettings(b -> b
                        .connectTimeout(2500, TimeUnit.MILLISECONDS)
                        .readTimeout(5500, TimeUnit.MILLISECONDS)
                        .applyToProxySettings(proxyBuilder -> proxyBuilder
                                .host("proxy.com")
                                .port(1080)
                                .username("username")
                                .password("password")))
                .applyToSslSettings(b -> b
                        .enabled(true)
                        .invalidHostNameAllowed(true))
                .readConcern(ReadConcern.MAJORITY)
                .readPreference(ReadPreference.secondary())
                .writeConcern(WriteConcern.MAJORITY.withWTimeout(2500, TimeUnit.MILLISECONDS))
                .applicationName("MyApp")
                .credential(MongoCredential.createScramSha1Credential("user", "test", "pass".toCharArray()))
                .compressorList(Collections.singletonList(
                        MongoCompressor.createZlibCompressor().withProperty(MongoCompressor.LEVEL, 5)))
                .retryWrites(true)
                .retryReads(true);

        MongoClientSettings expectedSettings = builder.build();
        MongoClientSettings settingsWithDefaultConnectionStringApplied = builder
                .applyConnectionString(new ConnectionString("mongodb://localhost"))
                .build();

        assertThat(settingsWithDefaultConnectionStringApplied, isTheSameAs(expectedSettings));
    }

    @Test
    @DisplayName("should use the socket settings connectionTimeout for the heartbeat settings")
    void shouldUseSocketTimeoutForHeartbeat() {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyToSocketSettings(builder -> builder
                        .connectTimeout(42, TimeUnit.SECONDS)
                        .readTimeout(60, TimeUnit.SECONDS)
                        .receiveBufferSize(22)
                        .sendBufferSize(10))
                .build();

        assertEquals(SocketSettings.builder().connectTimeout(42, TimeUnit.SECONDS)
                .readTimeout(42, TimeUnit.SECONDS).build(), settings.getHeartbeatSocketSettings());

        settings = MongoClientSettings.builder(settings)
                .applyToSocketSettings(builder -> builder.connectTimeout(21, TimeUnit.SECONDS))
                .build();

        assertEquals(SocketSettings.builder().connectTimeout(21, TimeUnit.SECONDS)
                .readTimeout(21, TimeUnit.SECONDS).build(), settings.getHeartbeatSocketSettings());
    }

    @Test
    @DisplayName("should use the proxy settings for the heartbeat settings")
    void shouldUseProxyForHeartbeat() {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyToSocketSettings(builder -> builder
                        .connectTimeout(42, TimeUnit.SECONDS)
                        .readTimeout(60, TimeUnit.SECONDS)
                        .applyToProxySettings(proxyBuilder -> proxyBuilder
                                .host("proxy.com")
                                .port(1080)
                                .username("username")
                                .password("password")))
                .build();

        assertEquals(SocketSettings.builder().connectTimeout(42, TimeUnit.SECONDS)
                .readTimeout(42, TimeUnit.SECONDS)
                .applyToProxySettings(proxyBuilder -> proxyBuilder
                        .host("proxy.com")
                        .port(1080)
                        .username("username")
                        .password("password"))
                .build(), settings.getHeartbeatSocketSettings());
    }

    @Test
    @DisplayName("should use the configured heartbeat timeouts for the heartbeat settings")
    void shouldUseConfiguredHeartbeatTimeouts() {
        MongoClientSettings settings = MongoClientSettings.builder()
                .heartbeatConnectTimeoutMS(24000)
                .heartbeatSocketTimeoutMS(12000)
                .applyToSocketSettings(builder -> builder
                        .connectTimeout(42, TimeUnit.SECONDS)
                        .readTimeout(60, TimeUnit.SECONDS)
                        .receiveBufferSize(22)
                        .sendBufferSize(10))
                .build();

        assertEquals(SocketSettings.builder().connectTimeout(24, TimeUnit.SECONDS)
                .readTimeout(12, TimeUnit.SECONDS).build(), settings.getHeartbeatSocketSettings());

        settings = MongoClientSettings.builder(settings)
                .applyToSocketSettings(builder -> builder.connectTimeout(21, TimeUnit.SECONDS))
                .build();

        assertEquals(SocketSettings.builder().connectTimeout(24, TimeUnit.SECONDS)
                .readTimeout(12, TimeUnit.SECONDS).build(), settings.getHeartbeatSocketSettings());
    }

    @Test
    @DisplayName("should only have the following fields in the builder")
    void shouldOnlyHaveExpectedBuilderFields() {
        List<String> actual = Arrays.stream(MongoClientSettings.Builder.class.getDeclaredFields())
                .filter(f -> !f.isSynthetic())
                .map(Field::getName)
                .sorted()
                .collect(Collectors.toList());

        List<String> expected = Arrays.asList("applicationName", "autoEncryptionSettings", "clusterSettingsBuilder",
                "codecRegistry", "commandListeners", "compressorList", "connectionPoolSettingsBuilder",
                "contextProvider", "credential", "dnsClient", "heartbeatConnectTimeoutMS",
                "heartbeatSocketTimeoutMS", "inetAddressResolver", "loggerSettingsBuilder",
                "observabilitySettings",
                "readConcern", "readPreference", "retryReads", "retryWrites", "serverApi",
                "serverSettingsBuilder", "socketSettingsBuilder", "sslSettingsBuilder", "timeoutMS",
                "transportSettings", "uuidRepresentation", "writeConcern");

        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("should only have the following methods in the builder")
    void shouldOnlyHaveExpectedBuilderMethods() {
        List<String> actual = Arrays.stream(MongoClientSettings.Builder.class.getDeclaredMethods())
                .filter(m -> !m.isSynthetic())
                .map(Method::getName)
                .sorted()
                .collect(Collectors.toList());

        List<String> expected = Arrays.asList("addCommandListener", "applicationName", "applyConnectionString",
                "applyToClusterSettings", "applyToConnectionPoolSettings", "applyToLoggerSettings",
                "applyToServerSettings", "applyToSocketSettings", "applyToSslSettings", "autoEncryptionSettings",
                "build", "codecRegistry", "commandListenerList", "compressorList", "contextProvider", "credential",
                "dnsClient", "heartbeatConnectTimeoutMS", "heartbeatSocketTimeoutMS", "inetAddressResolver",
                "observabilitySettings", "readConcern", "readPreference",
                "retryReads", "retryWrites", "serverApi", "timeout", "transportSettings",
                "uuidRepresentation", "writeConcern");

        assertEquals(expected, actual);
    }
}
