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

import com.mongodb.client.internal.TestOperationExecutor;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.model.DBCreateViewOptions;
import com.mongodb.client.model.ValidationAction;
import com.mongodb.client.model.ValidationLevel;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.CreateCollectionOperation;
import com.mongodb.internal.operation.CreateViewOperation;
import com.mongodb.internal.operation.ListCollectionsOperation;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static com.mongodb.CustomMatchers.isTheSameAs;
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DBUnitTest {

    private static final TimeoutSettings TIMEOUT_SETTINGS =
            new TimeoutSettings(30_000, 10_000, 0, null, java.util.concurrent.TimeUnit.SECONDS.toMillis(5));

    private static MongoClient createMongoClient() {
        MongoClient mongoClient = mock(MongoClient.class);
        when(mongoClient.getCodecRegistry()).thenReturn(getDefaultCodecRegistry());
        when(mongoClient.getMongoClientOptions()).thenReturn(MongoClientOptions.builder().build());
        when(mongoClient.getTimeoutSettings()).thenReturn(TIMEOUT_SETTINGS);
        when(mongoClient.getReadPreference()).thenReturn(ReadPreference.primary());
        when(mongoClient.getWriteConcern()).thenReturn(WriteConcern.ACKNOWLEDGED);
        when(mongoClient.getReadConcern()).thenReturn(ReadConcern.DEFAULT);
        return mongoClient;
    }

    private static MongoClient createStubbedMongoClient() {
        MongoClient mongoClient = mock(MongoClient.class);
        when(mongoClient.getMongoClientOptions()).thenReturn(MongoClientOptions.builder().build());
        when(mongoClient.getCodecRegistry()).thenReturn(getDefaultCodecRegistry());
        when(mongoClient.getTimeoutSettings()).thenReturn(TIMEOUT_SETTINGS);
        return mongoClient;
    }

    @Test
    @DisplayName("should throw IllegalArgumentException if name is invalid")
    void shouldThrowIllegalArgumentExceptionIfNameIsInvalid() {
        assertThrows(IllegalArgumentException.class,
                () -> new DB(createMongoClient(), "a.b", new TestOperationExecutor(new ArrayList<>())));
    }

    @Test
    @DisplayName("should get and set read concern")
    void shouldGetAndSetReadConcern() {
        DB db = new DB(createMongoClient(), "test", new TestOperationExecutor(new ArrayList<>()));

        assertEquals(ReadConcern.DEFAULT, db.getReadConcern());

        db.setReadConcern(ReadConcern.MAJORITY);
        assertEquals(ReadConcern.MAJORITY, db.getReadConcern());

        db.setReadConcern(null);
        assertEquals(ReadConcern.DEFAULT, db.getReadConcern());
    }

    @Test
    @DisplayName("should execute CreateCollectionOperation")
    void shouldExecuteCreateCollectionOperation() {
        MongoClient mongo = createStubbedMongoClient();
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(1L, 2L, 3L));
        DB db = new DB(mongo, "test", executor);
        db.setReadConcern(ReadConcern.MAJORITY);
        db.setWriteConcern(WriteConcern.MAJORITY);

        // Test with empty options
        db.createCollection("ctest", new BasicDBObject());
        CreateCollectionOperation operation = (CreateCollectionOperation) executor.getWriteOperation();
        assertThat(operation, isTheSameAs(new CreateCollectionOperation("test", "ctest", db.getWriteConcern())));
        assertEquals(ReadConcern.MAJORITY, executor.getReadConcern());

        // Test with full options
        BasicDBObject options = new BasicDBObject()
                .append("size", 100000)
                .append("max", 2000)
                .append("capped", true)
                .append("autoIndexId", true)
                .append("storageEngine", BasicDBObject.parse("{ wiredTiger: {}}"))
                .append("indexOptionDefaults", BasicDBObject.parse("{storageEngine: { mmapv1: {}}}"))
                .append("validator", BasicDBObject.parse("{level : { $gte: 10 } }"))
                .append("validationLevel", ValidationLevel.MODERATE.getValue())
                .append("validationAction", ValidationAction.WARN.getValue());

        db.createCollection("ctest", options);
        operation = (CreateCollectionOperation) executor.getWriteOperation();

        assertThat(operation, isTheSameAs(new CreateCollectionOperation("test", "ctest", db.getWriteConcern())
                .sizeInBytes(100000)
                .maxDocuments(2000)
                .capped(true)
                .autoIndex(true)
                .storageEngineOptions(BsonDocument.parse("{ wiredTiger: {}}"))
                .indexOptionDefaults(BsonDocument.parse("{storageEngine: { mmapv1: {}}}"))
                .validator(BsonDocument.parse("{level : { $gte: 10 } }"))
                .validationLevel(ValidationLevel.MODERATE)
                .validationAction(ValidationAction.WARN)));
        assertEquals(ReadConcern.MAJORITY, executor.getReadConcern());

        // Test with collation
        Collation collation = Collation.builder()
                .locale("en")
                .caseLevel(true)
                .collationCaseFirst(CollationCaseFirst.OFF)
                .collationStrength(CollationStrength.IDENTICAL)
                .numericOrdering(true)
                .collationAlternate(CollationAlternate.SHIFTED)
                .collationMaxVariable(CollationMaxVariable.SPACE)
                .backwards(true)
                .build();

        db.createCollection("ctest", new BasicDBObject("collation", BasicDBObject.parse(collation.asDocument().toJson())));
        operation = (CreateCollectionOperation) executor.getWriteOperation();

        assertThat(operation, isTheSameAs(new CreateCollectionOperation("test", "ctest", db.getWriteConcern())
                .collation(collation)));
        assertEquals(ReadConcern.MAJORITY, executor.getReadConcern());
    }

    @Test
    @DisplayName("should execute CreateViewOperation")
    void shouldExecuteCreateViewOperation() {
        MongoClient mongo = mock(MongoClient.class);
        when(mongo.getCodecRegistry()).thenReturn(MongoClient.getDefaultCodecRegistry());
        when(mongo.getMongoClientOptions()).thenReturn(MongoClientOptions.builder().build());
        when(mongo.getTimeoutSettings()).thenReturn(TIMEOUT_SETTINGS);

        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(1L, 2L, 3L));

        String databaseName = "test";
        String viewName = "view1";
        String viewOn = "collection1";
        List<BasicDBObject> pipeline = Collections.singletonList(new BasicDBObject("$match", new BasicDBObject("x", true)));
        WriteConcern writeConcern = WriteConcern.JOURNALED;
        Collation collation = Collation.builder().locale("en").build();

        DB db = new DB(mongo, databaseName, executor);
        db.setWriteConcern(writeConcern);
        db.setReadConcern(ReadConcern.MAJORITY);

        // Without collation
        db.createView(viewName, viewOn, pipeline);
        CreateViewOperation operation = (CreateViewOperation) executor.getWriteOperation();
        assertThat(operation, isTheSameAs(new CreateViewOperation(databaseName, viewName, viewOn,
                Collections.singletonList(new BsonDocument("$match", new BsonDocument("x", BsonBoolean.TRUE))), writeConcern)));
        assertEquals(ReadConcern.MAJORITY, executor.getReadConcern());

        // With collation
        db.createView(viewName, viewOn, pipeline, new DBCreateViewOptions().collation(collation));
        operation = (CreateViewOperation) executor.getWriteOperation();
        assertThat(operation, isTheSameAs(new CreateViewOperation(databaseName, viewName, viewOn,
                Collections.singletonList(new BsonDocument("$match", new BsonDocument("x", BsonBoolean.TRUE))), writeConcern)
                .collation(collation)));
        assertEquals(ReadConcern.MAJORITY, executor.getReadConcern());
    }

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("should execute ListCollectionsOperation")
    void shouldExecuteListCollectionsOperation() {
        MongoClient mongo = createStubbedMongoClient();
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(mock(BatchCursor.class), mock(BatchCursor.class)));

        String databaseName = "test";
        DB db = new DB(mongo, databaseName, executor);

        db.getCollectionNames();
        ListCollectionsOperation<?> operation = (ListCollectionsOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new ListCollectionsOperation<>(databaseName,
                new DBObjectCodec(getDefaultCodecRegistry()))
                .nameOnly(true)));

        db.collectionExists("someCollection");
        operation = (ListCollectionsOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new ListCollectionsOperation<>(databaseName,
                new DBObjectCodec(getDefaultCodecRegistry()))
                .nameOnly(true)));
    }

    @ParameterizedTest
    @MethodSource("obedientCommandArgs")
    @DisplayName("should use provided read preference for obedient commands")
    void shouldUseProvidedReadPreferenceForObedientCommands(final BasicDBObject cmd) {
        MongoClient mongo = createStubbedMongoClient();
        when(mongo.getCodecRegistry()).thenReturn(getDefaultCodecRegistry());
        TestOperationExecutor executor = new TestOperationExecutor(
                Collections.singletonList(new BsonDocument("ok", new BsonDouble(1.0))));
        DB database = new DB(mongo, "test", executor);
        database.setReadPreference(ReadPreference.secondary());
        database.setReadConcern(ReadConcern.MAJORITY);

        database.command(cmd);

        assertEquals(ReadPreference.secondary(), executor.getReadPreference());
        assertEquals(ReadConcern.MAJORITY, executor.getReadConcern());
    }

    private static Stream<BasicDBObject> obedientCommandArgs() {
        return Stream.of(
                new BasicDBObject("listCollections", 1),
                new BasicDBObject("dbStats", 1),
                new BasicDBObject("distinct", 1),
                new BasicDBObject("geoNear", 1),
                new BasicDBObject("geoSearch", 1),
                new BasicDBObject("group", 1),
                new BasicDBObject("listCollections", 1),
                new BasicDBObject("listIndexes", 1),
                new BasicDBObject("parallelCollectionScan", 1),
                new BasicDBObject("text", 1)
        );
    }

    @Test
    @DisplayName("should use primary read preference for non obedient commands")
    void shouldUsePrimaryReadPreferenceForNonObedientCommands() {
        MongoClient mongo = createStubbedMongoClient();
        when(mongo.getCodecRegistry()).thenReturn(getDefaultCodecRegistry());
        TestOperationExecutor executor = new TestOperationExecutor(
                Collections.singletonList(new BsonDocument("ok", new BsonDouble(1.0))));
        DB database = new DB(mongo, "test", executor);
        database.setReadPreference(ReadPreference.secondary());
        database.setReadConcern(ReadConcern.MAJORITY);

        database.command(new BasicDBObject("command", 1));

        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        assertEquals(ReadConcern.MAJORITY, executor.getReadConcern());
    }
}
