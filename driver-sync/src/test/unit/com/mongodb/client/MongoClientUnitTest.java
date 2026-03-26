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

package com.mongodb.client;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.internal.ChangeStreamIterableImpl;
import com.mongodb.client.internal.ListDatabasesIterableImpl;
import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.client.internal.MongoDatabaseImpl;
import com.mongodb.client.internal.TestOperationExecutor;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.connection.ClientMetadata;
import com.mongodb.internal.connection.Cluster;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.UuidCodec;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static com.mongodb.CustomMatchers.isTheSameAs;
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.ReadPreference.secondary;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.bson.UuidRepresentation.C_SHARP_LEGACY;
import static org.bson.UuidRepresentation.UNSPECIFIED;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.withUuidRepresentation;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MongoClientUnitTest {

    private static final CodecRegistry CODEC_REGISTRY = fromProviders(new ValueCodecProvider());
    private static final TimeoutSettings TIMEOUT_SETTINGS = new TimeoutSettings(30_000, 10_000, 0, null, SECONDS.toMillis(120));

    private static Cluster mockCluster() {
        Cluster cluster = mock(Cluster.class);
        when(cluster.getClientMetadata()).thenReturn(new ClientMetadata(null, MongoDriverInformation.builder().build()));
        return cluster;
    }

    @Test
    @DisplayName("should pass the correct settings to getDatabase")
    void shouldPassCorrectSettingsToGetDatabase() {
        MongoClientSettings settings = MongoClientSettings.builder()
                .readPreference(secondary())
                .writeConcern(WriteConcern.MAJORITY)
                .readConcern(ReadConcern.MAJORITY)
                .retryWrites(true)
                .codecRegistry(CODEC_REGISTRY)
                .build();
        MongoClientImpl client = new MongoClientImpl(mockCluster(), null, settings, null, new TestOperationExecutor(new ArrayList<>()));

        MongoDatabase database = client.getDatabase("name");

        MongoDatabaseImpl expectedDatabase = new MongoDatabaseImpl("name",
                withUuidRepresentation(CODEC_REGISTRY, UNSPECIFIED), secondary(),
                WriteConcern.MAJORITY, true, true, ReadConcern.MAJORITY, UNSPECIFIED, null,
                TIMEOUT_SETTINGS, new TestOperationExecutor(new ArrayList<>()));
        assertThat(database, isTheSameAs(expectedDatabase));
    }

    @Test
    @DisplayName("should use ListDatabasesIterableImpl correctly without session")
    void shouldUseListDatabasesIterableImplCorrectlyWithoutSession() {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null));
        MongoClientImpl client = new MongoClientImpl(mockCluster(), null, MongoClientSettings.builder().build(), null, executor);

        MongoIterable<?> listDatabasesIterable = client.listDatabases();
        assertThat(listDatabasesIterable, isTheSameAs(new ListDatabasesIterableImpl<>(null, Document.class,
                withUuidRepresentation(getDefaultCodecRegistry(), UNSPECIFIED), primary(), executor, true, TIMEOUT_SETTINGS)));

        MongoIterable<?> listDatabasesIterable2 = client.listDatabases(BsonDocument.class);
        assertThat(listDatabasesIterable2, isTheSameAs(new ListDatabasesIterableImpl<>(null, BsonDocument.class,
                withUuidRepresentation(getDefaultCodecRegistry(), UNSPECIFIED), primary(), executor, true, TIMEOUT_SETTINGS)));

        MongoIterable<String> listDatabaseNamesIterable = client.listDatabaseNames();
        assertThat(((com.mongodb.client.internal.MappingIterable<?, ?>) listDatabaseNamesIterable).getMapped(),
                isTheSameAs(new ListDatabasesIterableImpl<>(null, BsonDocument.class,
                        withUuidRepresentation(getDefaultCodecRegistry(), UNSPECIFIED), primary(), executor, true, TIMEOUT_SETTINGS)
                        .nameOnly(true)));

        client.close();
    }

    @Test
    @DisplayName("should use ListDatabasesIterableImpl correctly with session")
    void shouldUseListDatabasesIterableImplCorrectlyWithSession() {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null));
        MongoClientImpl client = new MongoClientImpl(mockCluster(), null, MongoClientSettings.builder().build(), null, executor);
        ClientSession session = mock(ClientSession.class);

        MongoIterable<?> listDatabasesIterable = client.listDatabases(session);
        assertThat(listDatabasesIterable, isTheSameAs(new ListDatabasesIterableImpl<>(session, Document.class,
                withUuidRepresentation(getDefaultCodecRegistry(), UNSPECIFIED), primary(), executor, true, TIMEOUT_SETTINGS)));

        MongoIterable<?> listDatabasesIterable2 = client.listDatabases(session, BsonDocument.class);
        assertThat(listDatabasesIterable2, isTheSameAs(new ListDatabasesIterableImpl<>(session, BsonDocument.class,
                withUuidRepresentation(getDefaultCodecRegistry(), UNSPECIFIED), primary(), executor, true, TIMEOUT_SETTINGS)));

        MongoIterable<String> listDatabaseNamesIterable = client.listDatabaseNames(session);
        assertThat(((com.mongodb.client.internal.MappingIterable<?, ?>) listDatabaseNamesIterable).getMapped(),
                isTheSameAs(new ListDatabasesIterableImpl<>(session, BsonDocument.class,
                        withUuidRepresentation(getDefaultCodecRegistry(), UNSPECIFIED), primary(), executor, true, TIMEOUT_SETTINGS)
                        .nameOnly(true)));

        client.close();
    }

    @Test
    @DisplayName("should create ChangeStreamIterable correctly without session")
    void shouldCreateChangeStreamIterableCorrectlyWithoutSession() {
        TestOperationExecutor executor = new TestOperationExecutor(new ArrayList<>());
        MongoNamespace namespace = new MongoNamespace("admin", "_ignored");
        MongoClientSettings settings = MongoClientSettings.builder()
                .readPreference(secondary())
                .readConcern(ReadConcern.MAJORITY)
                .codecRegistry(getDefaultCodecRegistry())
                .build();
        MongoClientImpl client = new MongoClientImpl(mockCluster(), null, settings, null, executor);

        ChangeStreamIterable<Document> changeStreamIterable = client.watch();
        assertThat(changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl<>(null, namespace,
                withUuidRepresentation(getDefaultCodecRegistry(), UNSPECIFIED),
                settings.getReadPreference(), settings.getReadConcern(), executor, Collections.emptyList(), Document.class,
                ChangeStreamLevel.CLIENT, true, TIMEOUT_SETTINGS), Arrays.asList("codec")));

        ChangeStreamIterable<Document> changeStreamIterable2 = client.watch(Arrays.asList(new Document("$match", 1)));
        assertThat(changeStreamIterable2, isTheSameAs(new ChangeStreamIterableImpl<>(null, namespace,
                withUuidRepresentation(getDefaultCodecRegistry(), UNSPECIFIED),
                settings.getReadPreference(), settings.getReadConcern(), executor,
                Arrays.asList(new Document("$match", 1)), Document.class,
                ChangeStreamLevel.CLIENT, true, TIMEOUT_SETTINGS), Arrays.asList("codec")));

        ChangeStreamIterable<BsonDocument> changeStreamIterable3 = client.watch(
                Arrays.asList(new Document("$match", 1)), BsonDocument.class);
        assertThat(changeStreamIterable3, isTheSameAs(new ChangeStreamIterableImpl<>(null, namespace,
                withUuidRepresentation(getDefaultCodecRegistry(), UNSPECIFIED),
                settings.getReadPreference(), settings.getReadConcern(), executor,
                Arrays.asList(new Document("$match", 1)), BsonDocument.class,
                ChangeStreamLevel.CLIENT, true, TIMEOUT_SETTINGS), Arrays.asList("codec")));
    }

    @Test
    @DisplayName("should create ChangeStreamIterable correctly with session")
    void shouldCreateChangeStreamIterableCorrectlyWithSession() {
        TestOperationExecutor executor = new TestOperationExecutor(new ArrayList<>());
        MongoNamespace namespace = new MongoNamespace("admin", "_ignored");
        MongoClientSettings settings = MongoClientSettings.builder()
                .readPreference(secondary())
                .readConcern(ReadConcern.MAJORITY)
                .codecRegistry(getDefaultCodecRegistry())
                .build();
        MongoClientImpl client = new MongoClientImpl(mockCluster(), null, settings, null, executor);
        ClientSession session = mock(ClientSession.class);

        ChangeStreamIterable<Document> changeStreamIterable = client.watch(session);
        assertThat(changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl<>(session, namespace,
                withUuidRepresentation(getDefaultCodecRegistry(), UNSPECIFIED),
                settings.getReadPreference(), settings.getReadConcern(), executor, Collections.emptyList(), Document.class,
                ChangeStreamLevel.CLIENT, true, TIMEOUT_SETTINGS), Arrays.asList("codec")));

        ChangeStreamIterable<Document> changeStreamIterable2 = client.watch(session,
                Arrays.asList(new Document("$match", 1)));
        assertThat(changeStreamIterable2, isTheSameAs(new ChangeStreamIterableImpl<>(session, namespace,
                withUuidRepresentation(getDefaultCodecRegistry(), UNSPECIFIED),
                settings.getReadPreference(), settings.getReadConcern(), executor,
                Arrays.asList(new Document("$match", 1)), Document.class,
                ChangeStreamLevel.CLIENT, true, TIMEOUT_SETTINGS), Arrays.asList("codec")));

        ChangeStreamIterable<BsonDocument> changeStreamIterable3 = client.watch(session,
                Arrays.asList(new Document("$match", 1)), BsonDocument.class);
        assertThat(changeStreamIterable3, isTheSameAs(new ChangeStreamIterableImpl<>(session, namespace,
                withUuidRepresentation(getDefaultCodecRegistry(), UNSPECIFIED),
                settings.getReadPreference(), settings.getReadConcern(), executor,
                Arrays.asList(new Document("$match", 1)), BsonDocument.class,
                ChangeStreamLevel.CLIENT, true, TIMEOUT_SETTINGS), Arrays.asList("codec")));
    }

    @Test
    @DisplayName("should validate the ChangeStreamIterable pipeline data correctly")
    void shouldValidateChangeStreamIterablePipelineDataCorrectly() {
        TestOperationExecutor executor = new TestOperationExecutor(new ArrayList<>());
        MongoClientImpl client = new MongoClientImpl(mockCluster(), null, MongoClientSettings.builder().build(), null, executor);

        assertThrows(IllegalArgumentException.class, () -> client.watch((Class<?>) null));
        assertThrows(IllegalArgumentException.class, () -> client.watch(Arrays.asList((Document) null)).into(new ArrayList<>()));
    }

    @Test
    @DisplayName("should get the cluster description")
    void shouldGetTheClusterDescription() {
        ClusterDescription clusterDescription = new ClusterDescription(ClusterConnectionMode.SINGLE, ClusterType.STANDALONE,
                Arrays.asList(ServerDescription.builder()
                        .address(new ServerAddress())
                        .type(ServerType.UNKNOWN)
                        .state(ServerConnectionState.CONNECTING)
                        .build()));
        MongoDriverInformation driverInformation = MongoDriverInformation.builder().build();
        Cluster cluster = mock(Cluster.class);
        when(cluster.getCurrentDescription()).thenReturn(clusterDescription);
        when(cluster.getClientMetadata()).thenReturn(new ClientMetadata("test", driverInformation));

        MongoClientSettings settings = MongoClientSettings.builder().build();
        MongoClientImpl client = new MongoClientImpl(cluster, driverInformation, settings, null, new TestOperationExecutor(new ArrayList<>()));

        assertEquals(clusterDescription, client.getClusterDescription());
    }

    @Test
    @DisplayName("should create registry reflecting UuidRepresentation")
    void shouldCreateRegistryReflectingUuidRepresentation() {
        CodecRegistry codecRegistry = fromProviders(Arrays.asList(new ValueCodecProvider()));
        MongoClientSettings settings = MongoClientSettings.builder()
                .codecRegistry(codecRegistry)
                .uuidRepresentation(C_SHARP_LEGACY)
                .build();

        MongoClientImpl client = new MongoClientImpl(mockCluster(), null, settings, null, new TestOperationExecutor(new ArrayList<>()));

        assertEquals(C_SHARP_LEGACY, ((UuidCodec) client.getCodecRegistry().get(java.util.UUID.class)).getUuidRepresentation());

        client.close();
    }
}
