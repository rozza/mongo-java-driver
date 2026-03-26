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

package com.mongodb.client.internal;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.internal.observability.micrometer.TracingManager;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.UuidCodec;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.mongodb.CustomMatchers.isTheSameAs;
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.ReadPreference.secondary;
import static org.bson.UuidRepresentation.UNSPECIFIED;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class MongoClusterTest {

    private static final CodecRegistry CODEC_REGISTRY = fromProviders(new ValueCodecProvider());
    private static final MongoClientSettings CLIENT_SETTINGS = MongoClientSettings.builder().build();
    private static final TimeoutSettings TIMEOUT_SETTINGS = TimeoutSettings.create(CLIENT_SETTINGS);
    private final Cluster cluster = mock(Cluster.class);
    private final MongoClient originator = mock(MongoClient.class);
    private final ServerSessionPool serverSessionPool = mock(ServerSessionPool.class);
    private final OperationExecutor operationExecutor = mock(OperationExecutor.class);

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
        TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
        MongoClusterImpl mongoCluster = createMongoCluster(settings, executor);

        MongoDatabase database = mongoCluster.getDatabase("name");
        assertThat(database, isTheSameAs(new MongoDatabaseImpl("name", CODEC_REGISTRY, secondary(),
                WriteConcern.MAJORITY, true, true, ReadConcern.MAJORITY, UNSPECIFIED, null,
                TIMEOUT_SETTINGS, new TestOperationExecutor(Collections.emptyList()))));
    }

    @Test
    @DisplayName("should behave correctly when using withCodecRegistry")
    void shouldBehaveCorrectlyWithCodecRegistry() {
        CodecRegistry newCodecRegistry = fromProviders(new ValueCodecProvider());
        MongoClusterImpl mongoCluster = (MongoClusterImpl) createMongoCluster().withCodecRegistry(newCodecRegistry);

        assertEquals(UNSPECIFIED, ((UuidCodec) mongoCluster.getCodecRegistry().get(java.util.UUID.class)).getUuidRepresentation());
        assertThat(mongoCluster, isTheSameAs(createMongoCluster(
                MongoClientSettings.builder(CLIENT_SETTINGS).codecRegistry(newCodecRegistry).build())));
    }

    @Test
    @DisplayName("should behave correctly when using withReadPreference")
    void shouldBehaveCorrectlyWithReadPreference() {
        ReadPreference newReadPreference = ReadPreference.secondaryPreferred();
        MongoClusterImpl mongoCluster = (MongoClusterImpl) createMongoCluster().withReadPreference(newReadPreference);

        assertEquals(newReadPreference, mongoCluster.getReadPreference());
        assertThat(mongoCluster, isTheSameAs(createMongoCluster(
                MongoClientSettings.builder(CLIENT_SETTINGS).readPreference(newReadPreference).build())));
    }

    @Test
    @DisplayName("should behave correctly when using withWriteConcern")
    void shouldBehaveCorrectlyWithWriteConcern() {
        WriteConcern newWriteConcern = WriteConcern.MAJORITY;
        MongoClusterImpl mongoCluster = (MongoClusterImpl) createMongoCluster().withWriteConcern(newWriteConcern);

        assertEquals(newWriteConcern, mongoCluster.getWriteConcern());
        assertThat(mongoCluster, isTheSameAs(createMongoCluster(
                MongoClientSettings.builder(CLIENT_SETTINGS).writeConcern(newWriteConcern).build())));
    }

    @Test
    @DisplayName("should behave correctly when using withReadConcern")
    void shouldBehaveCorrectlyWithReadConcern() {
        ReadConcern newReadConcern = ReadConcern.MAJORITY;
        MongoClusterImpl mongoCluster = (MongoClusterImpl) createMongoCluster().withReadConcern(newReadConcern);

        assertEquals(newReadConcern, mongoCluster.getReadConcern());
        assertThat(mongoCluster, isTheSameAs(createMongoCluster(
                MongoClientSettings.builder(CLIENT_SETTINGS).readConcern(newReadConcern).build())));
    }

    @Test
    @DisplayName("should behave correctly when using withTimeout")
    void shouldBehaveCorrectlyWithTimeout() {
        MongoClusterImpl mongoCluster = (MongoClusterImpl) createMongoCluster().withTimeout(10_000, TimeUnit.MILLISECONDS);

        assertEquals(10_000L, mongoCluster.getTimeout(TimeUnit.MILLISECONDS));
        assertThat(mongoCluster, isTheSameAs(createMongoCluster(MongoClientSettings.builder(CLIENT_SETTINGS)
                .timeout(10_000, TimeUnit.MILLISECONDS).build())));

        assertThrows(IllegalArgumentException.class, () ->
                createMongoCluster().withTimeout(500, TimeUnit.NANOSECONDS));
    }

    @Test
    @DisplayName("should use ListDatabasesIterableImpl correctly")
    void shouldUseListDatabasesIterableImplCorrectly() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null));
            MongoClusterImpl mongoCluster = createMongoCluster(executor);

            // listDatabases()
            ListDatabasesIterableImpl<?> listDatabasesIterable;
            if (session != null) {
                listDatabasesIterable = (ListDatabasesIterableImpl<?>) mongoCluster.listDatabases(session);
            } else {
                listDatabasesIterable = (ListDatabasesIterableImpl<?>) mongoCluster.listDatabases();
            }
            assertThat(listDatabasesIterable, isTheSameAs(new ListDatabasesIterableImpl<>(session, Document.class,
                    CLIENT_SETTINGS.getCodecRegistry(), primary(), executor, true, TIMEOUT_SETTINGS)));

            // listDatabases(BsonDocument.class)
            if (session != null) {
                listDatabasesIterable = (ListDatabasesIterableImpl<?>) mongoCluster.listDatabases(session, BsonDocument.class);
            } else {
                listDatabasesIterable = (ListDatabasesIterableImpl<?>) mongoCluster.listDatabases(BsonDocument.class);
            }
            assertThat(listDatabasesIterable, isTheSameAs(new ListDatabasesIterableImpl<>(session, BsonDocument.class,
                    CLIENT_SETTINGS.getCodecRegistry(), primary(), executor, true, TIMEOUT_SETTINGS)));

            // listDatabaseNames()
            MongoIterable<String> listDatabaseNamesIterable;
            if (session != null) {
                listDatabaseNamesIterable = mongoCluster.listDatabaseNames(session);
            } else {
                listDatabaseNamesIterable = mongoCluster.listDatabaseNames();
            }
            assertThat(((MappingIterable<?, ?>) listDatabaseNamesIterable).getMapped(),
                    isTheSameAs(new ListDatabasesIterableImpl<>(session, BsonDocument.class,
                            CLIENT_SETTINGS.getCodecRegistry(), primary(), executor, true, TIMEOUT_SETTINGS)
                            .nameOnly(true)));
        }
    }

    @Test
    @DisplayName("should create ChangeStreamIterable correctly")
    void shouldCreateChangeStreamIterableCorrectly() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
            MongoNamespace ns = new MongoNamespace("admin", "_ignored");
            MongoClientSettings settings = MongoClientSettings.builder()
                    .readPreference(secondary())
                    .readConcern(ReadConcern.MAJORITY)
                    .codecRegistry(getDefaultCodecRegistry())
                    .build();
            MongoClusterImpl mongoCluster = createMongoCluster(settings, executor);

            // watch()
            ChangeStreamIterableImpl<?> changeStreamIterable;
            if (session != null) {
                changeStreamIterable = (ChangeStreamIterableImpl<?>) mongoCluster.watch(session);
            } else {
                changeStreamIterable = (ChangeStreamIterableImpl<?>) mongoCluster.watch();
            }
            assertThat(changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl<>(session, ns,
                    settings.getCodecRegistry(), settings.getReadPreference(), settings.getReadConcern(),
                    executor, Collections.emptyList(), Document.class, ChangeStreamLevel.CLIENT, true,
                    TIMEOUT_SETTINGS), Collections.singletonList("codec")));

            // watch(pipeline)
            if (session != null) {
                changeStreamIterable = (ChangeStreamIterableImpl<?>) mongoCluster.watch(session,
                        Collections.singletonList(new Document("$match", 1)));
            } else {
                changeStreamIterable = (ChangeStreamIterableImpl<?>) mongoCluster.watch(
                        Collections.singletonList(new Document("$match", 1)));
            }
            assertThat(changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl<>(session, ns,
                    settings.getCodecRegistry(), settings.getReadPreference(), settings.getReadConcern(),
                    executor, Collections.singletonList(new Document("$match", 1)), Document.class,
                    ChangeStreamLevel.CLIENT, true, TIMEOUT_SETTINGS), Collections.singletonList("codec")));

            // watch(pipeline, BsonDocument.class)
            if (session != null) {
                changeStreamIterable = (ChangeStreamIterableImpl<?>) mongoCluster.watch(session,
                        Collections.singletonList(new Document("$match", 1)), BsonDocument.class);
            } else {
                changeStreamIterable = (ChangeStreamIterableImpl<?>) mongoCluster.watch(
                        Collections.singletonList(new Document("$match", 1)), BsonDocument.class);
            }
            assertThat(changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl<>(session, ns,
                    settings.getCodecRegistry(), settings.getReadPreference(), settings.getReadConcern(),
                    executor, Collections.singletonList(new Document("$match", 1)), BsonDocument.class,
                    ChangeStreamLevel.CLIENT, true, TIMEOUT_SETTINGS), Collections.singletonList("codec")));
        }
    }

    @Test
    @DisplayName("should validate the ChangeStreamIterable pipeline data correctly")
    void shouldValidateChangeStreamPipeline() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
        MongoClusterImpl mongoCluster = createMongoCluster(executor);

        assertThrows(IllegalArgumentException.class, () -> mongoCluster.watch((Class<?>) null));
        assertThrows(IllegalArgumentException.class, () ->
                mongoCluster.watch(Collections.singletonList(null)).into(new java.util.ArrayList<>()));
    }

    private MongoClusterImpl createMongoCluster() {
        return createMongoCluster(CLIENT_SETTINGS);
    }

    private MongoClusterImpl createMongoCluster(final MongoClientSettings settings) {
        return createMongoCluster(settings, operationExecutor);
    }

    private MongoClusterImpl createMongoCluster(final OperationExecutor executor) {
        return createMongoCluster(CLIENT_SETTINGS, executor);
    }

    private MongoClusterImpl createMongoCluster(final MongoClientSettings settings, final OperationExecutor executor) {
        return new MongoClusterImpl(null, cluster, settings.getCodecRegistry(), null, null,
                originator, executor, settings.getReadConcern(), settings.getReadPreference(),
                settings.getRetryReads(), settings.getRetryWrites(), null, serverSessionPool,
                TimeoutSettings.create(settings), settings.getUuidRepresentation(),
                settings.getWriteConcern(), TracingManager.NO_OP);
    }
}
