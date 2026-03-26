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

import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.client.internal.MongoDatabaseImpl;
import com.mongodb.client.internal.TestOperationExecutor;
import com.mongodb.client.model.geojson.MultiPolygon;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.connection.ClientMetadata;
import com.mongodb.internal.connection.Cluster;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.UuidCodec;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.json.JsonObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static com.mongodb.CustomMatchers.isTheSameAs;
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static com.mongodb.MongoCredential.createMongoX509Credential;
import static com.mongodb.ReadPreference.secondary;
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.bson.UuidRepresentation.C_SHARP_LEGACY;
import static org.bson.UuidRepresentation.STANDARD;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MongoClientTest {

    private static final CodecRegistry CODEC_REGISTRY = fromProviders(new ValueCodecProvider());
    private static final TimeoutSettings TIMEOUT_SETTINGS =
            new TimeoutSettings(30_000, 10_000, 0, null, java.util.concurrent.TimeUnit.SECONDS.toMillis(5));

    @Test
    @DisplayName("default codec registry should contain all supported providers")
    void defaultCodecRegistryShouldContainAllSupportedProviders() {
        CodecRegistry codecRegistry = getDefaultCodecRegistry();

        assertNotNull(codecRegistry.get(BsonDocument.class));
        assertNotNull(codecRegistry.get(BasicDBObject.class));
        assertNotNull(codecRegistry.get(Document.class));
        assertNotNull(codecRegistry.get(Integer.class));
        assertNotNull(codecRegistry.get(MultiPolygon.class));
        assertNotNull(codecRegistry.get(Collection.class));
        assertNotNull(codecRegistry.get(Iterable.class));
        assertNotNull(codecRegistry.get(JsonObject.class));
    }

    @ParameterizedTest
    @MethodSource("constructWithCorrectSettingsArgs")
    @DisplayName("should construct with correct settings")
    void shouldConstructWithCorrectSettings(final MongoClient client, final ClusterSettings clusterSettings,
                                            final MongoCredential credential) {
        try {
            assertEquals(clusterSettings, client.getDelegate().getSettings().getClusterSettings());
            assertEquals(credential, client.getCredential());
        } finally {
            client.close();
        }
    }

    private static Stream<Object[]> constructWithCorrectSettingsArgs() {
        return Stream.of(
                new Object[]{
                        new MongoClient(),
                        ClusterSettings.builder().build(),
                        null
                },
                new Object[]{
                        new MongoClient("host:27018"),
                        ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress("host:27018"))).mode(SINGLE).build(),
                        null
                },
                new Object[]{
                        new MongoClient("host", 27018),
                        ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress("host:27018"))).mode(SINGLE).build(),
                        null
                },
                new Object[]{
                        new MongoClient("mongodb://host:27018"),
                        ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress("host:27018"))).mode(SINGLE).build(),
                        null
                },
                new Object[]{
                        new MongoClient("mongodb://user:pwd@host:27018"),
                        ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress("host:27018"))).mode(SINGLE).build(),
                        MongoCredential.createCredential("user", "admin", "pwd".toCharArray())
                },
                new Object[]{
                        new MongoClient("mongodb+srv://test3.test.build.10gen.cc"),
                        ClusterSettings.builder().srvHost("test3.test.build.10gen.cc").mode(MULTIPLE).build(),
                        null
                },
                new Object[]{
                        new MongoClient("mongodb+srv://user:pwd@test3.test.build.10gen.cc"),
                        ClusterSettings.builder().srvHost("test3.test.build.10gen.cc").mode(MULTIPLE).build(),
                        MongoCredential.createCredential("user", "admin", "pwd".toCharArray())
                },
                new Object[]{
                        new MongoClient(new ConnectionString("mongodb://host:27018")),
                        ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress("host:27018"))).mode(SINGLE).build(),
                        null
                },
                new Object[]{
                        new MongoClient(new ConnectionString("mongodb://user:pwd@host:27018")),
                        ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress("host:27018"))).mode(SINGLE).build(),
                        MongoCredential.createCredential("user", "admin", "pwd".toCharArray())
                },
                new Object[]{
                        new MongoClient(new ConnectionString("mongodb+srv://test3.test.build.10gen.cc")),
                        ClusterSettings.builder().srvHost("test3.test.build.10gen.cc").mode(MULTIPLE).build(),
                        null
                },
                new Object[]{
                        new MongoClient(MongoClientSettings.builder()
                                .applyConnectionString(new ConnectionString("mongodb://host:27018")).build()),
                        ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress("host:27018"))).mode(SINGLE).build(),
                        null
                },
                new Object[]{
                        new MongoClient(new MongoClientURI("mongodb://host:27018")),
                        ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress("host:27018"))).mode(SINGLE).build(),
                        null
                },
                new Object[]{
                        new MongoClient(new MongoClientURI("mongodb://host:27018/?replicaSet=rs0")),
                        ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress("host:27018"))).mode(MULTIPLE)
                                .requiredReplicaSetName("rs0").build(),
                        null
                },
                new Object[]{
                        new MongoClient(new MongoClientURI("mongodb://user:pwd@host:27018")),
                        ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress("host:27018"))).mode(SINGLE).build(),
                        MongoCredential.createCredential("user", "admin", "pwd".toCharArray())
                },
                new Object[]{
                        new MongoClient(new MongoClientURI("mongodb://host1:27018,host2:27018")),
                        ClusterSettings.builder().hosts(Arrays.asList(new ServerAddress("host1:27018"), new ServerAddress("host2:27018")))
                                .mode(MULTIPLE).build(),
                        null
                },
                new Object[]{
                        new MongoClient(new MongoClientURI("mongodb+srv://test3.test.build.10gen.cc")),
                        ClusterSettings.builder().srvHost("test3.test.build.10gen.cc").mode(MULTIPLE).build(),
                        null
                },
                new Object[]{
                        new MongoClient("host:27018", MongoClientOptions.builder().serverSelectionTimeout(5).build()),
                        ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress("host:27018"))).mode(SINGLE)
                                .serverSelectionTimeout(5, MILLISECONDS).build(),
                        null
                },
                new Object[]{
                        new MongoClient(new ServerAddress("host:27018")),
                        ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress("host:27018"))).mode(SINGLE).build(),
                        null
                },
                new Object[]{
                        new MongoClient(new ServerAddress("host:27018"), MongoClientOptions.builder().serverSelectionTimeout(5).build()),
                        ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress("host:27018"))).mode(SINGLE)
                                .serverSelectionTimeout(5, MILLISECONDS).build(),
                        null
                },
                new Object[]{
                        new MongoClient(new ServerAddress("host:27018"), createMongoX509Credential(),
                                MongoClientOptions.builder().serverSelectionTimeout(5).build()),
                        ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress("host:27018"))).mode(SINGLE)
                                .serverSelectionTimeout(5, MILLISECONDS).build(),
                        createMongoX509Credential()
                },
                new Object[]{
                        new MongoClient(Collections.singletonList(new ServerAddress("host:27018"))),
                        ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress("host:27018"))).mode(MULTIPLE).build(),
                        null
                },
                new Object[]{
                        new MongoClient(Collections.singletonList(new ServerAddress("host:27018")),
                                MongoClientOptions.builder().serverSelectionTimeout(5).build()),
                        ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress("host:27018"))).mode(MULTIPLE)
                                .serverSelectionTimeout(5, MILLISECONDS).build(),
                        null
                },
                new Object[]{
                        new MongoClient(Collections.singletonList(new ServerAddress("host:27018")), createMongoX509Credential(),
                                MongoClientOptions.builder().serverSelectionTimeout(5).build()),
                        ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress("host:27018"))).mode(MULTIPLE)
                                .serverSelectionTimeout(5, MILLISECONDS).build(),
                        createMongoX509Credential()
                }
        );
    }

    @ParameterizedTest
    @MethodSource("wrapMongoDBDriverInformationArgs")
    @DisplayName("should wrap MongoDBDriverInformation with legacy information")
    void shouldWrapMongoDBDriverInformationWithLegacyInformation(final MongoClient client,
                                                                  final MongoDriverInformation mongoDriverInformation) {
        try {
            assertEquals(mongoDriverInformation.getDriverNames(), client.getDelegate().getMongoDriverInformation().getDriverNames());
            assertEquals(mongoDriverInformation.getDriverPlatforms(),
                    client.getDelegate().getMongoDriverInformation().getDriverPlatforms());
            assertEquals(mongoDriverInformation.getDriverVersions(),
                    client.getDelegate().getMongoDriverInformation().getDriverVersions());
        } finally {
            client.close();
        }
    }

    private static Stream<Object[]> wrapMongoDBDriverInformationArgs() {
        MongoDriverInformation legacyOnly = MongoDriverInformation.builder().driverName("legacy").build();
        MongoDriverInformation testPlusLegacy = MongoDriverInformation.builder(
                MongoDriverInformation.builder().driverName("test").driverPlatform("osx").driverVersion("1.0").build())
                .driverName("legacy").build();

        return Stream.of(
                new Object[]{new MongoClient(), legacyOnly},
                new Object[]{new MongoClient("host:27018"), legacyOnly},
                new Object[]{new MongoClient("host", 27018), legacyOnly},
                new Object[]{new MongoClient("mongodb://host:27018"), legacyOnly},
                new Object[]{new MongoClient(new ConnectionString("mongodb://host:27018")), legacyOnly},
                new Object[]{
                        new MongoClient(new ConnectionString("mongodb://host:27018"),
                                MongoDriverInformation.builder().driverName("test").driverPlatform("osx").driverVersion("1.0").build()),
                        testPlusLegacy
                },
                new Object[]{
                        new MongoClient(MongoClientSettings.builder()
                                .applyConnectionString(new ConnectionString("mongodb://host:27018")).build()),
                        legacyOnly
                },
                new Object[]{
                        new MongoClient(MongoClientSettings.builder()
                                .applyConnectionString(new ConnectionString("mongodb://host:27018")).build(),
                                MongoDriverInformation.builder().driverName("test").driverPlatform("osx").driverVersion("1.0").build()),
                        testPlusLegacy
                },
                new Object[]{new MongoClient(new MongoClientURI("mongodb://host:27018")), legacyOnly},
                new Object[]{
                        new MongoClient(new MongoClientURI("mongodb://host:27018"),
                                MongoDriverInformation.builder().driverName("test").driverPlatform("osx").driverVersion("1.0").build()),
                        testPlusLegacy
                },
                new Object[]{
                        new MongoClient("host:27018", MongoClientOptions.builder().serverSelectionTimeout(5).build()),
                        legacyOnly
                },
                new Object[]{new MongoClient(new ServerAddress("host:27018")), legacyOnly},
                new Object[]{
                        new MongoClient(new ServerAddress("host:27018"), MongoClientOptions.builder().serverSelectionTimeout(5).build()),
                        legacyOnly
                },
                new Object[]{
                        new MongoClient(new ServerAddress("host:27018"), createMongoX509Credential(),
                                MongoClientOptions.builder().serverSelectionTimeout(5).build()),
                        legacyOnly
                },
                new Object[]{
                        new MongoClient(new ServerAddress("host:27018"), createMongoX509Credential(),
                                MongoClientOptions.builder().serverSelectionTimeout(5).build(),
                                MongoDriverInformation.builder().driverName("test").driverPlatform("osx").driverVersion("1.0").build()),
                        testPlusLegacy
                },
                new Object[]{new MongoClient(Collections.singletonList(new ServerAddress("host:27018"))), legacyOnly},
                new Object[]{
                        new MongoClient(Collections.singletonList(new ServerAddress("host:27018")),
                                MongoClientOptions.builder().serverSelectionTimeout(5).build()),
                        legacyOnly
                },
                new Object[]{
                        new MongoClient(Collections.singletonList(new ServerAddress("host:27018")),
                                MongoClientOptions.builder().serverSelectionTimeout(5).build()),
                        legacyOnly
                },
                new Object[]{
                        new MongoClient(Collections.singletonList(new ServerAddress("host:27018")),
                                MongoClientOptions.builder().serverSelectionTimeout(5).build()),
                        legacyOnly
                },
                new Object[]{
                        new MongoClient(Collections.singletonList(new ServerAddress("host:27018")), createMongoX509Credential(),
                                MongoClientOptions.builder().serverSelectionTimeout(5).build()),
                        legacyOnly
                },
                new Object[]{
                        new MongoClient(Collections.singletonList(new ServerAddress("host:27018")), createMongoX509Credential(),
                                MongoClientOptions.builder().serverSelectionTimeout(5).build(),
                                MongoDriverInformation.builder().driverName("test").driverPlatform("osx").driverVersion("1.0").build()),
                        testPlusLegacy
                }
        );
    }

    @Test
    @DisplayName("should preserve original options")
    void shouldPreserveOriginalOptions() {
        MongoClientOptions options = MongoClientOptions.builder().cursorFinalizerEnabled(false).build();
        MongoClient client = new MongoClient("localhost", options);

        try {
            assertEquals(options, client.getMongoClientOptions());
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("should preserve original options from MongoClientURI")
    void shouldPreserveOriginalOptionsFromMongoClientURI() {
        MongoClientOptions.Builder builder = MongoClientOptions.builder().cursorFinalizerEnabled(false);
        MongoClient client = new MongoClient(new MongoClientURI("mongodb://localhost", builder));

        try {
            assertEquals(builder.build(), client.getMongoClientOptions());
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("should manage cursor cleaning service if enabled")
    void shouldManageCursorCleaningServiceIfEnabled() {
        MongoClient client = new MongoClient("localhost", MongoClientOptions.builder().cursorFinalizerEnabled(true).build());

        assertNotNull(client.getCursorCleaningService());

        client.close();

        assertTrue(client.getCursorCleaningService().isShutdown());
    }

    @Test
    @DisplayName("should not create cursor cleaning service if disabled")
    void shouldNotCreateCursorCleaningServiceIfDisabled() {
        MongoClient client = new MongoClient("localhost", MongoClientOptions.builder().cursorFinalizerEnabled(false).build());

        try {
            assertNull(client.getCursorCleaningService());
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("should get specified options")
    void shouldGetSpecifiedOptions() {
        MongoClientOptions options = MongoClientOptions.builder().cursorFinalizerEnabled(false).build();
        MongoClient client = new MongoClient("localhost", options);

        try {
            assertEquals(options, client.getMongoClientOptions());
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("should get options from specified settings")
    void shouldGetOptionsFromSpecifiedSettings() {
        MongoClientSettings settings = MongoClientSettings.builder().writeConcern(WriteConcern.MAJORITY).build();
        MongoClient client = new MongoClient(settings);

        try {
            assertEquals(MongoClientOptions.builder(settings).build(), client.getMongoClientOptions());
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("should validate the ChangeStreamIterable pipeline data correctly")
    void shouldValidateTheChangeStreamIterablePipelineDataCorrectly() {
        TestOperationExecutor executor = new TestOperationExecutor(new ArrayList<>());
        Cluster clusterStub = mock(Cluster.class);
        when(clusterStub.getClientMetadata()).thenReturn(new ClientMetadata("test", MongoDriverInformation.builder().build()));

        MongoClientImpl client = new MongoClientImpl(clusterStub, null, MongoClientSettings.builder().build(), null, executor);

        assertThrows(IllegalArgumentException.class, () -> client.watch((Class<?>) null));

        assertThrows(IllegalArgumentException.class, () -> client.watch(Collections.singletonList(null)).into(new ArrayList<>()));
    }

    @Test
    @DisplayName("should pass the correct settings to getDatabase")
    void shouldPassTheCorrectSettingsToGetDatabase() {
        MongoClientOptions options = MongoClientOptions.builder()
                .readPreference(secondary())
                .writeConcern(WriteConcern.MAJORITY)
                .readConcern(ReadConcern.MAJORITY)
                .retryWrites(true)
                .codecRegistry(CODEC_REGISTRY)
                .uuidRepresentation(STANDARD)
                .build();
        MongoClient client = new MongoClient("localhost", options);

        try {
            Object database = client.getDatabase("name");
            assertThat(database, isTheSameAs(new MongoDatabaseImpl("name", client.getCodecRegistry(), secondary(),
                    WriteConcern.MAJORITY, true, true, ReadConcern.MAJORITY, STANDARD, null,
                    TIMEOUT_SETTINGS.withMaxWaitTimeMS(120_000), client.getOperationExecutor())));
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("should create registry reflecting UuidRepresentation")
    void shouldCreateRegistryReflectingUuidRepresentation() {
        MongoClientOptions options = MongoClientOptions.builder()
                .codecRegistry(CODEC_REGISTRY)
                .uuidRepresentation(C_SHARP_LEGACY)
                .build();

        MongoClient client = new MongoClient("localhost", options);

        try {
            assertEquals(C_SHARP_LEGACY, ((UuidCodec) client.getCodecRegistry().get(UUID.class)).getUuidRepresentation());
        } finally {
            client.close();
        }
    }
}
