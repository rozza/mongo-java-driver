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
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.client.model.IndexOptionDefaults;
import com.mongodb.client.model.ValidationAction;
import com.mongodb.client.model.ValidationLevel;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.operation.CommandReadOperation;
import com.mongodb.internal.operation.CreateCollectionOperation;
import com.mongodb.internal.operation.CreateViewOperation;
import com.mongodb.internal.operation.DropDatabaseOperation;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.UuidCodec;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.CustomMatchers.isTheSameAs;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.ReadPreference.primaryPreferred;
import static com.mongodb.ReadPreference.secondary;
import static org.bson.UuidRepresentation.C_SHARP_LEGACY;
import static org.bson.UuidRepresentation.JAVA_LEGACY;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class MongoDatabaseTest {

    private final String name = "databaseName";
    private final CodecRegistry codecRegistry = MongoClientSettings.getDefaultCodecRegistry();
    private final WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
    private final ReadConcern readConcern = ReadConcern.DEFAULT;
    private final Collation collation = Collation.builder().locale("en").build();

    @Test
    @DisplayName("should throw IllegalArgumentException if name is invalid")
    void shouldThrowIfNameInvalid() {
        assertThrows(IllegalArgumentException.class, () ->
                new MongoDatabaseImpl("a.b", codecRegistry, secondary(), writeConcern, false, false, readConcern,
                        JAVA_LEGACY, null, TIMEOUT_SETTINGS, new TestOperationExecutor(Collections.emptyList())));
    }

    @Test
    @DisplayName("should throw IllegalArgumentException from getCollection if collectionName is invalid")
    void shouldThrowIfCollectionNameInvalid() {
        MongoDatabaseImpl database = new MongoDatabaseImpl(name, codecRegistry, secondary(), writeConcern, false,
                false, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, new TestOperationExecutor(Collections.emptyList()));
        assertThrows(IllegalArgumentException.class, () -> database.getCollection(""));
    }

    @Test
    @DisplayName("should return the correct name from getName")
    void shouldReturnCorrectName() {
        MongoDatabaseImpl database = new MongoDatabaseImpl(name, codecRegistry, secondary(), writeConcern, false,
                false, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, new TestOperationExecutor(Collections.emptyList()));
        assertEquals(name, database.getName());
    }

    @Test
    @DisplayName("should behave correctly when using withCodecRegistry")
    void shouldBehaveCorrectlyWithCodecRegistry() {
        CodecRegistry newCodecRegistry = fromProviders(new ValueCodecProvider());
        TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());

        MongoDatabaseImpl database = (MongoDatabaseImpl) new MongoDatabaseImpl(name, codecRegistry, secondary(),
                writeConcern, false, true, readConcern, C_SHARP_LEGACY, null, TIMEOUT_SETTINGS, executor)
                .withCodecRegistry(newCodecRegistry);

        assertEquals(C_SHARP_LEGACY, ((UuidCodec) database.getCodecRegistry().get(java.util.UUID.class)).getUuidRepresentation());
        assertThat(database, isTheSameAs(new MongoDatabaseImpl(name, database.getCodecRegistry(), secondary(),
                writeConcern, false, true, readConcern, C_SHARP_LEGACY, null, TIMEOUT_SETTINGS, executor)));
    }

    @Test
    @DisplayName("should behave correctly when using withReadPreference")
    void shouldBehaveCorrectlyWithReadPreference() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
        MongoDatabaseImpl database = (MongoDatabaseImpl) new MongoDatabaseImpl(name, codecRegistry, secondary(),
                writeConcern, false, false, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor)
                .withReadPreference(primary());

        assertEquals(primary(), database.getReadPreference());
        assertThat(database, isTheSameAs(new MongoDatabaseImpl(name, codecRegistry, primary(), writeConcern, false,
                false, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor)));
    }

    @Test
    @DisplayName("should behave correctly when using withWriteConcern")
    void shouldBehaveCorrectlyWithWriteConcern() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
        MongoDatabaseImpl database = (MongoDatabaseImpl) new MongoDatabaseImpl(name, codecRegistry, secondary(),
                writeConcern, false, false, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor)
                .withWriteConcern(WriteConcern.MAJORITY);

        assertEquals(WriteConcern.MAJORITY, database.getWriteConcern());
        assertThat(database, isTheSameAs(new MongoDatabaseImpl(name, codecRegistry, secondary(),
                WriteConcern.MAJORITY, false, false, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor)));
    }

    @Test
    @DisplayName("should behave correctly when using withReadConcern")
    void shouldBehaveCorrectlyWithReadConcern() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
        MongoDatabaseImpl database = (MongoDatabaseImpl) new MongoDatabaseImpl(name, codecRegistry, secondary(),
                writeConcern, false, false, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor)
                .withReadConcern(ReadConcern.MAJORITY);

        assertEquals(ReadConcern.MAJORITY, database.getReadConcern());
        assertThat(database, isTheSameAs(new MongoDatabaseImpl(name, codecRegistry, secondary(), writeConcern, false,
                false, ReadConcern.MAJORITY, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor)));
    }

    @Test
    @DisplayName("should behave correctly when using withTimeout")
    void shouldBehaveCorrectlyWithTimeout() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
        MongoDatabaseImpl database = new MongoDatabaseImpl(name, codecRegistry, secondary(), writeConcern, false,
                false, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor);

        MongoDatabaseImpl newDatabase = (MongoDatabaseImpl) database.withTimeout(10_000, TimeUnit.MILLISECONDS);
        assertEquals(10_000L, newDatabase.getTimeout(TimeUnit.MILLISECONDS));
        assertThat(newDatabase, isTheSameAs(new MongoDatabaseImpl(name, codecRegistry, secondary(), writeConcern,
                false, false, readConcern, JAVA_LEGACY, null,
                TIMEOUT_SETTINGS.withTimeout(10_000L, TimeUnit.MILLISECONDS), executor)));

        assertThrows(IllegalArgumentException.class, () -> database.withTimeout(500, TimeUnit.NANOSECONDS));
    }

    @Test
    @DisplayName("should be able to executeCommand correctly")
    void shouldExecuteCommandCorrectly() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            BsonDocument command = new BsonDocument("command", new BsonInt32(1));
            TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null, null, null));
            MongoDatabaseImpl database = new MongoDatabaseImpl(name, codecRegistry, secondary(), writeConcern, false,
                    false, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor);

            if (session != null) {
                database.runCommand(session, command);
            } else {
                database.runCommand(command);
            }
            executor.getReadOperation();
            assertEquals(session, executor.getClientSession());
            assertEquals(primary(), executor.getReadPreference());

            if (session != null) {
                database.runCommand(session, command, primaryPreferred());
            } else {
                database.runCommand(command, primaryPreferred());
            }
            executor.getReadOperation();
            assertEquals(session, executor.getClientSession());
            assertEquals(primaryPreferred(), executor.getReadPreference());

            if (session != null) {
                database.runCommand(session, command, BsonDocument.class);
            } else {
                database.runCommand(command, BsonDocument.class);
            }
            executor.getReadOperation();
            assertEquals(session, executor.getClientSession());
            assertEquals(primary(), executor.getReadPreference());

            if (session != null) {
                database.runCommand(session, command, primaryPreferred(), BsonDocument.class);
            } else {
                database.runCommand(command, primaryPreferred(), BsonDocument.class);
            }
            executor.getReadOperation();
            assertEquals(session, executor.getClientSession());
            assertEquals(primaryPreferred(), executor.getReadPreference());
        }
    }

    @Test
    @DisplayName("should use DropDatabaseOperation correctly")
    void shouldUseDropDatabaseOperation() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            TestOperationExecutor executor = new TestOperationExecutor(Collections.singletonList(null));
            MongoDatabaseImpl database = new MongoDatabaseImpl(name, codecRegistry, secondary(), writeConcern, false,
                    false, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor);

            if (session != null) {
                database.drop(session);
            } else {
                database.drop();
            }
            DropDatabaseOperation operation = (DropDatabaseOperation) executor.getWriteOperation();
            assertThat(operation, isTheSameAs(new DropDatabaseOperation(name, writeConcern)));
            assertEquals(session, executor.getClientSession());
        }
    }

    @Test
    @DisplayName("should use ListCollectionsOperation correctly")
    void shouldUseListCollectionsOperation() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null, null));
            MongoDatabaseImpl database = new MongoDatabaseImpl(name, codecRegistry, secondary(), writeConcern, false,
                    false, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor);

            ListCollectionsIterableImpl<?> listCollectionIterable;
            if (session != null) {
                listCollectionIterable = (ListCollectionsIterableImpl<?>) database.listCollections(session);
            } else {
                listCollectionIterable = (ListCollectionsIterableImpl<?>) database.listCollections();
            }
            assertThat(listCollectionIterable, isTheSameAs(new ListCollectionsIterableImpl<>(session, name, false,
                    Document.class, codecRegistry, primary(), executor, false, TIMEOUT_SETTINGS)));

            if (session != null) {
                listCollectionIterable = (ListCollectionsIterableImpl<?>) database.listCollections(session, BsonDocument.class);
            } else {
                listCollectionIterable = (ListCollectionsIterableImpl<?>) database.listCollections(BsonDocument.class);
            }
            assertThat(listCollectionIterable, isTheSameAs(new ListCollectionsIterableImpl<>(session, name, false,
                    BsonDocument.class, codecRegistry, primary(), executor, false, TIMEOUT_SETTINGS)));

            // listCollectionNames
            Object listCollectionNamesIterable;
            if (session != null) {
                listCollectionNamesIterable = database.listCollectionNames(session);
            } else {
                listCollectionNamesIterable = database.listCollectionNames();
            }
            assertThat(((ListCollectionNamesIterableImpl) listCollectionNamesIterable).getWrapped(),
                    isTheSameAs(new ListCollectionsIterableImpl<>(session, name, true, BsonDocument.class,
                            codecRegistry, primary(), executor, false, TIMEOUT_SETTINGS)));
        }
    }

    @Test
    @DisplayName("should use CreateCollectionOperation correctly")
    void shouldUseCreateCollectionOperation() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            String collectionName = "collectionName";
            TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null));
            MongoDatabaseImpl database = new MongoDatabaseImpl(name, codecRegistry, secondary(), writeConcern, false,
                    false, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor);

            if (session != null) {
                database.createCollection(session, collectionName);
            } else {
                database.createCollection(collectionName);
            }
            CreateCollectionOperation operation = (CreateCollectionOperation) executor.getWriteOperation();
            assertThat(operation, isTheSameAs(new CreateCollectionOperation(name, collectionName, writeConcern)));
            assertEquals(session, executor.getClientSession());

            CreateCollectionOptions options = new CreateCollectionOptions()
                    .capped(true)
                    .maxDocuments(100)
                    .sizeInBytes(1000)
                    .storageEngineOptions(BsonDocument.parse("{ wiredTiger : {}}"))
                    .indexOptionDefaults(new IndexOptionDefaults().storageEngine(BsonDocument.parse("{ mmapv1 : {}}")))
                    .validationOptions(new ValidationOptions().validator(BsonDocument.parse("{level: {$gte: 10}}"))
                            .validationLevel(ValidationLevel.MODERATE)
                            .validationAction(ValidationAction.WARN))
                    .collation(collation);

            if (session != null) {
                database.createCollection(session, collectionName, options);
            } else {
                database.createCollection(collectionName, options);
            }
            operation = (CreateCollectionOperation) executor.getWriteOperation();
            assertThat(operation, isTheSameAs(new CreateCollectionOperation(name, collectionName, writeConcern)
                    .collation(collation)
                    .capped(true)
                    .maxDocuments(100)
                    .sizeInBytes(1000)
                    .storageEngineOptions(BsonDocument.parse("{ wiredTiger : {}}"))
                    .indexOptionDefaults(BsonDocument.parse("{ storageEngine : { mmapv1 : {}}}"))
                    .validator(BsonDocument.parse("{level: {$gte: 10}}"))
                    .validationLevel(ValidationLevel.MODERATE)
                    .validationAction(ValidationAction.WARN)));
            assertEquals(session, executor.getClientSession());
        }
    }

    @Test
    @DisplayName("should use CreateViewOperation correctly")
    void shouldUseCreateViewOperation() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            String viewName = "view1";
            String viewOn = "col1";
            java.util.List<Document> pipeline = Collections.singletonList(new Document("$match", new Document("x", true)));
            WriteConcern wc = WriteConcern.JOURNALED;
            TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null));
            MongoDatabaseImpl database = new MongoDatabaseImpl(name, codecRegistry, secondary(), wc, false, false,
                    readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor);

            if (session != null) {
                database.createView(session, viewName, viewOn, pipeline);
            } else {
                database.createView(viewName, viewOn, pipeline);
            }
            CreateViewOperation operation = (CreateViewOperation) executor.getWriteOperation();
            assertThat(operation, isTheSameAs(new CreateViewOperation(name, viewName, viewOn,
                    Collections.singletonList(new BsonDocument("$match", new BsonDocument("x", BsonBoolean.TRUE))), wc)));
            assertEquals(session, executor.getClientSession());

            if (session != null) {
                database.createView(session, viewName, viewOn, pipeline, new CreateViewOptions().collation(collation));
            } else {
                database.createView(viewName, viewOn, pipeline, new CreateViewOptions().collation(collation));
            }
            operation = (CreateViewOperation) executor.getWriteOperation();
            assertThat(operation, isTheSameAs(new CreateViewOperation(name, viewName, viewOn,
                    Collections.singletonList(new BsonDocument("$match", new BsonDocument("x", BsonBoolean.TRUE))), wc)
                    .collation(collation)));
            assertEquals(session, executor.getClientSession());
        }
    }

    @Test
    @DisplayName("should validate the createView pipeline data correctly")
    void shouldValidateCreateViewPipeline() {
        MongoDatabaseImpl database = new MongoDatabaseImpl(name, codecRegistry, secondary(), writeConcern, false,
                false, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, mock(OperationExecutor.class));

        assertThrows(IllegalArgumentException.class, () -> database.createView("view1", "col1", null));
        assertThrows(IllegalArgumentException.class, () ->
                database.createView("view1", "col1", Collections.singletonList(null)));
    }

    @Test
    @DisplayName("should create ChangeStreamIterable correctly")
    void shouldCreateChangeStreamIterable() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
            MongoNamespace ns = new MongoNamespace(name, "_ignored");
            MongoDatabaseImpl database = new MongoDatabaseImpl(name, codecRegistry, secondary(), writeConcern, false,
                    false, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor);

            ChangeStreamIterableImpl<?> changeStreamIterable;
            if (session != null) {
                changeStreamIterable = (ChangeStreamIterableImpl<?>) database.watch(session);
            } else {
                changeStreamIterable = (ChangeStreamIterableImpl<?>) database.watch();
            }
            assertThat(changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl<>(session, ns, codecRegistry,
                    secondary(), readConcern, executor, Collections.emptyList(), Document.class,
                    ChangeStreamLevel.DATABASE, false, TIMEOUT_SETTINGS), Collections.singletonList("codec")));

            if (session != null) {
                changeStreamIterable = (ChangeStreamIterableImpl<?>) database.watch(session,
                        Collections.singletonList(new Document("$match", 1)));
            } else {
                changeStreamIterable = (ChangeStreamIterableImpl<?>) database.watch(
                        Collections.singletonList(new Document("$match", 1)));
            }
            assertThat(changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl<>(session, ns, codecRegistry,
                    secondary(), readConcern, executor, Collections.singletonList(new Document("$match", 1)),
                    Document.class, ChangeStreamLevel.DATABASE, false, TIMEOUT_SETTINGS),
                    Collections.singletonList("codec")));

            if (session != null) {
                changeStreamIterable = (ChangeStreamIterableImpl<?>) database.watch(session,
                        Collections.singletonList(new Document("$match", 1)), BsonDocument.class);
            } else {
                changeStreamIterable = (ChangeStreamIterableImpl<?>) database.watch(
                        Collections.singletonList(new Document("$match", 1)), BsonDocument.class);
            }
            assertThat(changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl<>(session, ns, codecRegistry,
                    secondary(), readConcern, executor, Collections.singletonList(new Document("$match", 1)),
                    BsonDocument.class, ChangeStreamLevel.DATABASE, false, TIMEOUT_SETTINGS),
                    Collections.singletonList("codec")));
        }
    }

    @Test
    @DisplayName("should validate the ChangeStreamIterable pipeline data correctly")
    void shouldValidateChangeStreamPipeline() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
        MongoDatabaseImpl database = new MongoDatabaseImpl(name, codecRegistry, secondary(), writeConcern, false,
                false, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor);

        assertThrows(IllegalArgumentException.class, () -> database.watch((Class<?>) null));
        assertThrows(IllegalArgumentException.class, () ->
                database.watch(Collections.singletonList(null)).into(new java.util.ArrayList<>()));
    }

    @Test
    @DisplayName("should create AggregateIterable correctly")
    void shouldCreateAggregateIterable() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
            MongoDatabaseImpl database = new MongoDatabaseImpl(name, codecRegistry, secondary(), writeConcern, false,
                    false, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor);

            AggregateIterableImpl<?, ?> aggregateIterable;
            if (session != null) {
                aggregateIterable = (AggregateIterableImpl<?, ?>) database.aggregate(session, Collections.emptyList());
            } else {
                aggregateIterable = (AggregateIterableImpl<?, ?>) database.aggregate(Collections.emptyList());
            }
            assertThat(aggregateIterable, isTheSameAs(new AggregateIterableImpl<>(session, name, Document.class,
                    Document.class, codecRegistry, secondary(), readConcern, writeConcern, executor,
                    Collections.emptyList(), AggregationLevel.DATABASE, false, TIMEOUT_SETTINGS),
                    Collections.singletonList("codec")));

            if (session != null) {
                aggregateIterable = (AggregateIterableImpl<?, ?>) database.aggregate(session,
                        Collections.singletonList(new Document("$match", 1)), BsonDocument.class);
            } else {
                aggregateIterable = (AggregateIterableImpl<?, ?>) database.aggregate(
                        Collections.singletonList(new Document("$match", 1)), BsonDocument.class);
            }
            assertThat(aggregateIterable, isTheSameAs(new AggregateIterableImpl<>(session, name, Document.class,
                    BsonDocument.class, codecRegistry, secondary(), readConcern, writeConcern, executor,
                    Collections.singletonList(new Document("$match", 1)), AggregationLevel.DATABASE, false,
                    TIMEOUT_SETTINGS), Collections.singletonList("codec")));
        }
    }

    @Test
    @DisplayName("should validate the AggregationIterable pipeline data correctly")
    void shouldValidateAggregationPipeline() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
        MongoDatabaseImpl database = new MongoDatabaseImpl(name, codecRegistry, secondary(), writeConcern, false,
                false, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor);

        assertThrows(IllegalArgumentException.class, () -> database.aggregate(null, Collections.emptyList()));
        assertThrows(IllegalArgumentException.class, () -> database.aggregate((java.util.List) null));
        assertThrows(IllegalArgumentException.class, () ->
                database.aggregate(Collections.singletonList(null)).into(new java.util.ArrayList<>()));
    }

    @Test
    @DisplayName("should pass the correct options to getCollection")
    void shouldPassCorrectOptionsToGetCollection() {
        CodecRegistry cr = fromProviders(Arrays.asList(new ValueCodecProvider(), new DocumentCodecProvider(),
                new BsonValueCodecProvider()));
        MongoDatabaseImpl database = new MongoDatabaseImpl("databaseName", cr, secondary(), WriteConcern.MAJORITY,
                true, true, ReadConcern.MAJORITY, JAVA_LEGACY, null, TIMEOUT_SETTINGS,
                new TestOperationExecutor(Collections.emptyList()));

        MongoCollectionImpl<?> collection = (MongoCollectionImpl<?>) database.getCollection("collectionName");
        assertThat(collection, isTheSameAs(new MongoCollectionImpl<>(new MongoNamespace("databaseName", "collectionName"),
                Document.class, cr, secondary(), WriteConcern.MAJORITY, true, true, ReadConcern.MAJORITY,
                JAVA_LEGACY, null, TIMEOUT_SETTINGS, new TestOperationExecutor(Collections.emptyList()))));
    }

    @Test
    @DisplayName("should validate the client session correctly")
    void shouldValidateClientSession() {
        MongoDatabaseImpl database = new MongoDatabaseImpl(name, codecRegistry, secondary(), writeConcern, false,
                false, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, mock(OperationExecutor.class));

        assertThrows(IllegalArgumentException.class, () -> database.createCollection(null, "newColl"));
        assertThrows(IllegalArgumentException.class, () ->
                database.createView(null, "newView", Collections.singletonList(Document.parse("{$match: {}}"))));
        assertThrows(IllegalArgumentException.class, () -> database.drop(null));
        assertThrows(IllegalArgumentException.class, () -> database.listCollectionNames(null));
        assertThrows(IllegalArgumentException.class, () -> database.listCollections((ClientSession) null));
        assertThrows(IllegalArgumentException.class, () -> database.runCommand(null, Document.parse("{}")));
    }
}
