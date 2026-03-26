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
import com.mongodb.client.model.DBCollectionCountOptions;
import com.mongodb.client.model.DBCollectionDistinctOptions;
import com.mongodb.client.model.DBCollectionFindAndModifyOptions;
import com.mongodb.client.model.DBCollectionFindOptions;
import com.mongodb.client.model.DBCollectionRemoveOptions;
import com.mongodb.client.model.DBCollectionUpdateOptions;
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.IndexRequest;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.operation.AggregateOperation;
import com.mongodb.internal.operation.AggregateToCollectionOperation;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.CountOperation;
import com.mongodb.internal.operation.CreateIndexesOperation;
import com.mongodb.internal.operation.DistinctOperation;
import com.mongodb.internal.operation.FindAndDeleteOperation;
import com.mongodb.internal.operation.FindAndReplaceOperation;
import com.mongodb.internal.operation.FindAndUpdateOperation;
import com.mongodb.internal.operation.FindOperation;
import com.mongodb.internal.operation.MapReduceBatchCursor;
import com.mongodb.internal.operation.MapReduceStatistics;
import com.mongodb.internal.operation.MapReduceToCollectionOperation;
import com.mongodb.internal.operation.MapReduceWithInlineResultsOperation;
import com.mongodb.internal.operation.MixedBulkWriteOperation;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonInt32;
import org.bson.BsonJavaScript;
import org.bson.UuidRepresentation;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.BsonValueCodec;
import org.bson.codecs.UuidCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.mongodb.CustomMatchers.isTheSameAs;
import static com.mongodb.Fixture.getMongoClient;
import static com.mongodb.LegacyMixedBulkWriteOperation.createBulkWriteOperationForDelete;
import static com.mongodb.LegacyMixedBulkWriteOperation.createBulkWriteOperationForUpdate;
import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("deprecation")
class DBCollectionSpecificationTest {

    private static final DBObjectCodec DEFAULT_DBOBJECT_CODEC_FACTORY = new DBObjectCodec(
            MongoClient.getDefaultCodecRegistry(),
            DBObjectCodec.getDefaultBsonTypeClassMap(),
            new DBCollectionObjectFactory());

    private static final Collation COLLATION = Collation.builder()
            .locale("en")
            .caseLevel(true)
            .collationCaseFirst(CollationCaseFirst.OFF)
            .collationStrength(CollationStrength.IDENTICAL)
            .numericOrdering(true)
            .collationAlternate(CollationAlternate.SHIFTED)
            .collationMaxVariable(CollationMaxVariable.SPACE)
            .backwards(true)
            .build();

    private static final Collation FRENCH_COLLATION = Collation.builder().locale("fr").build();

    @Test
    @DisplayName("should throw IllegalArgumentException if name is invalid")
    void shouldThrowIllegalArgumentExceptionIfNameIsInvalid() {
        assertThrows(IllegalArgumentException.class, () ->
                new DB(getMongoClient(), "myDatabase", new TestOperationExecutor(Collections.emptyList())).getCollection(""));
    }

    @Test
    @DisplayName("should use MongoClient CodecRegistry")
    void shouldUseMongoClientCodecRegistry() {
        MongoClient mongoClient = org.mockito.Mockito.mock(MongoClient.class);
        org.mockito.Mockito.when(mongoClient.getCodecRegistry())
                .thenReturn(fromCodecs(new UuidCodec(UuidRepresentation.STANDARD)));
        org.mockito.Mockito.when(mongoClient.getReadConcern()).thenReturn(ReadConcern.DEFAULT);
        org.mockito.Mockito.when(mongoClient.getWriteConcern()).thenReturn(WriteConcern.ACKNOWLEDGED);
        org.mockito.Mockito.when(mongoClient.getMongoClientOptions()).thenReturn(MongoClientOptions.builder().build());

        TestOperationExecutor executor = new TestOperationExecutor(
                Collections.singletonList(WriteConcernResult.unacknowledged()));
        DB db = new DB(mongoClient, "myDatabase", executor);
        DBCollection collection = db.getCollection("test");
        UUID uuid = UUID.fromString("01020304-0506-0708-090a-0b0c0d0e0f10");

        collection.insert(new BasicDBObject("_id", uuid));
        LegacyMixedBulkWriteOperation operation = (LegacyMixedBulkWriteOperation) executor.getWriteOperation();

        assertEquals(new BsonBinary(uuid, UuidRepresentation.STANDARD),
                ((InsertRequest) operation.getWriteRequests().get(0)).getDocument().getBinary("_id"));
    }

    @Test
    @DisplayName("should get and set read concern")
    void shouldGetAndSetReadConcern() {
        DB db = new DB(getMongoClient(), "myDatabase", new TestOperationExecutor(Collections.emptyList()));
        db.setReadConcern(ReadConcern.MAJORITY);
        DBCollection collection = db.getCollection("test");

        assertEquals(ReadConcern.MAJORITY, collection.getReadConcern());

        collection.setReadConcern(ReadConcern.LOCAL);
        assertEquals(ReadConcern.LOCAL, collection.getReadConcern());

        collection.setReadConcern(null);
        assertEquals(ReadConcern.MAJORITY, collection.getReadConcern());
    }

    @Test
    @DisplayName("should use CreateIndexOperation properly")
    void shouldUseCreateIndexOperationProperly() {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null, null));
        DBCollection collection = new DB(getMongoClient(), "myDatabase", executor).getCollection("test");
        BasicDBObject keys = new BasicDBObject("a", 1);

        collection.createIndex(keys);
        CreateIndexesOperation op = (CreateIndexesOperation) executor.getWriteOperation();
        assertThat(op.getRequests().get(0), isTheSameAs(new IndexRequest(new BsonDocument("a", new BsonInt32(1)))));

        String storageEngine = "{ wiredTiger: { configString: \"block_compressor=zlib\" }}";
        String partialFilterExpression = "{ a: { $gte: 10 } }";
        collection.createIndex(keys, new BasicDBObject("background", true)
                .append("unique", true)
                .append("sparse", true)
                .append("name", "aIndex")
                .append("expireAfterSeconds", 100)
                .append("v", 1)
                .append("weights", new BasicDBObject("a", 1000))
                .append("default_language", "es")
                .append("language_override", "language")
                .append("textIndexVersion", 1)
                .append("2dsphereIndexVersion", 1)
                .append("bits", 1)
                .append("min", -180.0)
                .append("max", 180.0)
                .append("dropDups", true)
                .append("storageEngine", BasicDBObject.parse(storageEngine))
                .append("partialFilterExpression", BasicDBObject.parse(partialFilterExpression))
                .append("collation", BasicDBObject.parse(COLLATION.asDocument().toJson())));

        CreateIndexesOperation op2 = (CreateIndexesOperation) executor.getWriteOperation();
        assertThat(op2.getRequests().get(0), isTheSameAs(new IndexRequest(new BsonDocument("a", new BsonInt32(1)))
                .background(true)
                .unique(true)
                .sparse(true)
                .name("aIndex")
                .expireAfter(100L, TimeUnit.SECONDS)
                .version(1)
                .weights(new BsonDocument("a", new BsonInt32(1000)))
                .defaultLanguage("es")
                .languageOverride("language")
                .textVersion(1)
                .sphereVersion(1)
                .bits(1)
                .min(-180.0)
                .max(180.0)
                .dropDups(true)
                .storageEngine(BsonDocument.parse(storageEngine))
                .partialFilterExpression(BsonDocument.parse(partialFilterExpression))
                .collation(COLLATION)));
    }

    @ParameterizedTest(name = "value={0}, expectedValue={1}")
    @MethodSource("booleanIndexOptionsProvider")
    @DisplayName("should support boolean index options that are numbers")
    void shouldSupportBooleanIndexOptionsThatAreNumbers(final Object value, final boolean expectedValue) {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null));
        DBCollection collection = new DB(getMongoClient(), "myDatabase", executor).getCollection("test");
        BasicDBObject options = new BasicDBObject("sparse", value);

        collection.createIndex(new BasicDBObject("y", 1), options);

        CreateIndexesOperation operation = (CreateIndexesOperation) executor.getWriteOperation();
        assertEquals(expectedValue, operation.getRequests().get(0).isSparse());
    }

    static Stream<Arguments> booleanIndexOptionsProvider() {
        return Stream.of(
                Arguments.of(0, false),
                Arguments.of(0F, false),
                Arguments.of(0D, false),
                Arguments.of(1, true),
                Arguments.of(-1, true),
                Arguments.of(4L, true),
                Arguments.of(4.3F, true),
                Arguments.of(4.0D, true)
        );
    }

    @ParameterizedTest(name = "integerValue={0}")
    @MethodSource("integerIndexOptionsProvider")
    @DisplayName("should support integer index options that are numbers")
    void shouldSupportIntegerIndexOptionsThatAreNumbers(final Number integerValue) {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null));
        DBCollection collection = new DB(getMongoClient(), "myDatabase", executor).getCollection("test");
        BasicDBObject options = new BasicDBObject("expireAfterSeconds", integerValue);

        collection.createIndex(new BasicDBObject("y", 1), options);

        CreateIndexesOperation operation = (CreateIndexesOperation) executor.getWriteOperation();
        assertEquals(integerValue.longValue(), operation.getRequests().get(0).getExpireAfter(TimeUnit.SECONDS));
    }

    static Stream<Arguments> integerIndexOptionsProvider() {
        return Stream.of(
                Arguments.of(4),
                Arguments.of(4L),
                Arguments.of(4.0)
        );
    }

    @ParameterizedTest(name = "doubleValue={0}")
    @MethodSource("doubleIndexOptionsProvider")
    @DisplayName("should support double index options that are numbers")
    void shouldSupportDoubleIndexOptionsThatAreNumbers(final Number doubleValue) {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null));
        DBCollection collection = new DB(getMongoClient(), "myDatabase", executor).getCollection("test");
        BasicDBObject options = new BasicDBObject("max", doubleValue);

        collection.createIndex(new BasicDBObject("y", "2d"), options);

        CreateIndexesOperation operation = (CreateIndexesOperation) executor.getWriteOperation();
        assertEquals(doubleValue.doubleValue(), operation.getRequests().get(0).getMax());
    }

    static Stream<Arguments> doubleIndexOptionsProvider() {
        return Stream.of(
                Arguments.of(4),
                Arguments.of(4L),
                Arguments.of(4.0)
        );
    }

    @Test
    @DisplayName("should throw IllegalArgumentException for unsupported option value type")
    void shouldThrowIllegalArgumentExceptionForUnsupportedOptionValueType() {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null));
        DBCollection collection = new DB(getMongoClient(), "myDatabase", executor).getCollection("test");
        BasicDBObject options = new BasicDBObject("sparse", "true");

        assertThrows(IllegalArgumentException.class, () ->
                collection.createIndex(new BasicDBObject("y", "1"), options));
    }

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("find should create the correct FindOperation")
    void findShouldCreateCorrectFindOperation() {
        BatchCursor<DBObject> cursor = org.mockito.Mockito.mock(BatchCursor.class);
        org.mockito.Mockito.when(cursor.hasNext()).thenReturn(false);
        org.mockito.Mockito.when(cursor.getServerCursor()).thenReturn(new ServerCursor(12L, new ServerAddress()));

        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(cursor, cursor, cursor));
        DB db = new DB(getMongoClient(), "myDatabase", executor);
        DBCollection collection = db.getCollection("test");

        collection.find().iterator().hasNext();
        assertThat(executor.getReadOperation(), isTheSameAs(new FindOperation<>(collection.getNamespace(),
                collection.getObjectCodec())
                .filter(new BsonDocument())
                .retryReads(true)));

        // Inherits from DB
        db.setReadConcern(ReadConcern.MAJORITY);
        collection.find().iterator().hasNext();
        assertThat(executor.getReadOperation(), isTheSameAs(new FindOperation<>(collection.getNamespace(),
                collection.getObjectCodec())
                .filter(new BsonDocument())
                .retryReads(true)));

        collection.setReadConcern(ReadConcern.LOCAL);
        collection.find(new BasicDBObject(), new DBCollectionFindOptions().collation(COLLATION)).iterator().hasNext();
        assertThat(executor.getReadOperation(), isTheSameAs(new FindOperation<>(collection.getNamespace(),
                collection.getObjectCodec())
                .filter(new BsonDocument())
                .collation(COLLATION)
                .retryReads(true)));
    }

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("findOne should create the correct FindOperation")
    void findOneShouldCreateCorrectFindOperation() {
        DBObject dbObject = new BasicDBObject("_id", 1);
        BatchCursor<DBObject> cursor = org.mockito.Mockito.mock(BatchCursor.class);
        org.mockito.Mockito.when(cursor.next()).thenReturn(new java.util.ArrayList<>(Collections.singletonList(dbObject)));
        org.mockito.Mockito.when(cursor.hasNext()).thenReturn(true);
        org.mockito.Mockito.when(cursor.getServerCursor()).thenReturn(new ServerCursor(12L, new ServerAddress()));

        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(cursor, cursor, cursor));
        DB db = new DB(getMongoClient(), "myDatabase", executor);
        DBCollection collection = db.getCollection("test");

        collection.findOne();
        assertThat(executor.getReadOperation(), isTheSameAs(new FindOperation<>(collection.getNamespace(),
                collection.getObjectCodec())
                .filter(new BsonDocument())
                .limit(-1)
                .retryReads(true)));

        // Inherits from DB
        db.setReadConcern(ReadConcern.MAJORITY);
        collection.findOne();
        assertThat(executor.getReadOperation(), isTheSameAs(new FindOperation<>(collection.getNamespace(),
                collection.getObjectCodec())
                .filter(new BsonDocument())
                .limit(-1)
                .retryReads(true)));

        collection.setReadConcern(ReadConcern.LOCAL);
        collection.findOne(new BasicDBObject(), new DBCollectionFindOptions().collation(COLLATION));
        assertThat(executor.getReadOperation(), isTheSameAs(new FindOperation<>(collection.getNamespace(),
                collection.getObjectCodec())
                .filter(new BsonDocument())
                .limit(-1)
                .collation(COLLATION)
                .retryReads(true)));
    }

    @Test
    @DisplayName("findAndRemove should create the correct FindAndDeleteOperation")
    void findAndRemoveShouldCreateCorrectOperation() {
        BasicDBObject query = new BasicDBObject();
        BasicDBObject cannedResult = BasicDBObject.parse("{value: {}}");
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(cannedResult, cannedResult, cannedResult));
        DB db = new DB(getMongoClient(), "myDatabase", executor);
        boolean retryWrites = db.getMongoClient().getMongoClientOptions().getRetryWrites();
        DBCollection collection = db.getCollection("test");

        collection.findAndRemove(query);

        assertThat(executor.getWriteOperation(), isTheSameAs(new FindAndDeleteOperation<>(collection.getNamespace(),
                WriteConcern.ACKNOWLEDGED, retryWrites, collection.getObjectCodec())
                .filter(new BsonDocument())));
    }

    @ParameterizedTest(name = "arrayFilters={0}")
    @MethodSource("arrayFiltersProvider")
    @DisplayName("findAndModify should create the correct FindAndUpdateOperation")
    void findAndModifyShouldCreateCorrectFindAndUpdateOperation(final List<BasicDBObject> dbObjectArrayFilters,
                                                                  final List<BsonDocument> bsonArrayFilters) {
        BasicDBObject query = new BasicDBObject();
        String updateJson = "{$set: {a : 1}}";
        BasicDBObject update = BasicDBObject.parse(updateJson);
        BsonDocument bsonUpdate = BsonDocument.parse(updateJson);
        BasicDBObject cannedResult = BasicDBObject.parse("{value: {}}");
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(cannedResult, cannedResult, cannedResult));
        DB db = new DB(getMongoClient(), "myDatabase", executor);
        boolean retryWrites = db.getMongoClient().getMongoClientOptions().getRetryWrites();
        DBCollection collection = db.getCollection("test");

        collection.findAndModify(query, update);
        assertThat(executor.getWriteOperation(), isTheSameAs(new FindAndUpdateOperation<>(collection.getNamespace(),
                WriteConcern.ACKNOWLEDGED, retryWrites, collection.getObjectCodec(), bsonUpdate)
                .filter(new BsonDocument())));

        // With options
        collection.findAndModify(query, new DBCollectionFindAndModifyOptions().update(update).collation(COLLATION)
                .arrayFilters(dbObjectArrayFilters).writeConcern(WriteConcern.W3));
        assertThat(executor.getWriteOperation(), isTheSameAs(new FindAndUpdateOperation<>(collection.getNamespace(),
                WriteConcern.W3, retryWrites, collection.getObjectCodec(), bsonUpdate)
                .filter(new BsonDocument())
                .collation(COLLATION)
                .arrayFilters(bsonArrayFilters)));
    }

    @Test
    @DisplayName("findAndModify should create the correct FindAndReplaceOperation")
    void findAndModifyShouldCreateCorrectFindAndReplaceOperation() {
        BasicDBObject query = new BasicDBObject();
        String replacementJson = "{a : 1}";
        BasicDBObject replace = BasicDBObject.parse(replacementJson);
        BsonDocument bsonReplace = BsonDocument.parse(replacementJson);
        BasicDBObject cannedResult = BasicDBObject.parse("{value: {}}");
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(cannedResult, cannedResult, cannedResult));
        DB db = new DB(getMongoClient(), "myDatabase", executor);
        boolean retryWrites = db.getMongoClient().getMongoClientOptions().getRetryWrites();
        DBCollection collection = db.getCollection("test");

        collection.findAndModify(query, replace);
        assertThat(executor.getWriteOperation(), isTheSameAs(new FindAndReplaceOperation<>(collection.getNamespace(),
                WriteConcern.ACKNOWLEDGED, retryWrites, collection.getObjectCodec(), bsonReplace)
                .filter(new BsonDocument())));

        // With options
        collection.findAndModify(query, new DBCollectionFindAndModifyOptions().update(replace).collation(COLLATION)
                .writeConcern(WriteConcern.W3));
        assertThat(executor.getWriteOperation(), isTheSameAs(new FindAndReplaceOperation<>(collection.getNamespace(),
                WriteConcern.W3, retryWrites, collection.getObjectCodec(), bsonReplace)
                .filter(new BsonDocument())
                .collation(COLLATION)));
    }

    @Test
    @DisplayName("count should create the correct CountOperation")
    void countShouldCreateCorrectCountOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(42L, 42L, 42L));
        DB db = new DB(getMongoClient(), "myDatabase", executor);
        DBCollection collection = db.getCollection("test");

        collection.count();
        assertThat(executor.getReadOperation(), isTheSameAs(new CountOperation(collection.getNamespace())
                .filter(new BsonDocument()).retryReads(true)));
        executor.getReadConcern(); // consume the read concern from the first operation

        // Inherits from DB
        db.setReadConcern(ReadConcern.MAJORITY);
        collection.count();
        assertThat(executor.getReadOperation(), isTheSameAs(new CountOperation(collection.getNamespace())
                .filter(new BsonDocument()).retryReads(true)));
        assertEquals(ReadConcern.MAJORITY, executor.getReadConcern());

        collection.setReadConcern(ReadConcern.LOCAL);
        collection.count(new BasicDBObject(), new DBCollectionCountOptions().collation(COLLATION));
        assertThat(executor.getReadOperation(), isTheSameAs(new CountOperation(collection.getNamespace())
                .filter(new BsonDocument()).retryReads(true)
                .collation(COLLATION)));
        assertEquals(ReadConcern.LOCAL, executor.getReadConcern());
    }

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("distinct should create the correct DistinctOperation")
    void distinctShouldCreateCorrectDistinctOperation() {
        BatchCursor<BsonInt32> cursor = org.mockito.Mockito.mock(BatchCursor.class);
        final int[] count = {0};
        org.mockito.Mockito.when(cursor.next()).thenAnswer(invocation -> {
            count[0]++;
            return Arrays.asList(new BsonInt32(1), new BsonInt32(2));
        });
        org.mockito.Mockito.when(cursor.hasNext()).thenAnswer(invocation -> count[0] == 0);

        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(cursor, cursor, cursor));
        DB db = new DB(getMongoClient(), "myDatabase", executor);
        DBCollection collection = db.getCollection("test");

        List<Object> distinctFieldValues = collection.distinct("field1");
        assertEquals(Arrays.asList(1, 2), distinctFieldValues);
        assertThat(executor.getReadOperation(), isTheSameAs(new DistinctOperation(collection.getNamespace(), "field1",
                new BsonValueCodec()).filter(new BsonDocument()).retryReads(true)));
        assertEquals(ReadConcern.DEFAULT, executor.getReadConcern());

        // Inherits from DB
        db.setReadConcern(ReadConcern.MAJORITY);
        collection.distinct("field1");
        assertThat(executor.getReadOperation(), isTheSameAs(new DistinctOperation(collection.getNamespace(), "field1",
                new BsonValueCodec())
                .filter(new BsonDocument()).retryReads(true)));
        assertEquals(ReadConcern.MAJORITY, executor.getReadConcern());

        collection.setReadConcern(ReadConcern.LOCAL);
        collection.distinct("field1", new DBCollectionDistinctOptions().collation(COLLATION));
        assertThat(executor.getReadOperation(), isTheSameAs(new DistinctOperation(collection.getNamespace(), "field1",
                new BsonValueCodec()).collation(COLLATION).retryReads(true)));
        assertEquals(ReadConcern.LOCAL, executor.getReadConcern());
    }

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("mapReduce should create the correct MapReduceInlineResultsOperation")
    void mapReduceShouldCreateCorrectInlineResultsOperation() {
        MapReduceBatchCursor<DBObject> cursor = org.mockito.Mockito.mock(MapReduceBatchCursor.class);
        org.mockito.Mockito.when(cursor.hasNext()).thenReturn(false);

        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(cursor, cursor, cursor));
        DB db = new DB(getMongoClient(), "myDatabase", executor);
        DBCollection collection = db.getCollection("test");

        collection.mapReduce("map", "reduce", null, MapReduceCommand.OutputType.INLINE, new BasicDBObject());
        assertThat(executor.getReadOperation(), isTheSameAs(
                new MapReduceWithInlineResultsOperation<>(collection.getNamespace(), new BsonJavaScript("map"),
                        new BsonJavaScript("reduce"), collection.getDefaultDBObjectCodec())
                        .verbose(true)
                        .filter(new BsonDocument())));
        assertEquals(ReadConcern.DEFAULT, executor.getReadConcern());

        // Inherits from DB
        db.setReadConcern(ReadConcern.LOCAL);
        collection.mapReduce("map", "reduce", null, MapReduceCommand.OutputType.INLINE, new BasicDBObject());
        assertThat(executor.getReadOperation(), isTheSameAs(
                new MapReduceWithInlineResultsOperation<>(collection.getNamespace(), new BsonJavaScript("map"),
                        new BsonJavaScript("reduce"), collection.getDefaultDBObjectCodec())
                        .verbose(true)
                        .filter(new BsonDocument())));
        assertEquals(ReadConcern.LOCAL, executor.getReadConcern());

        collection.setReadConcern(ReadConcern.MAJORITY);
        MapReduceCommand mapReduceCommand = new MapReduceCommand(collection, "map", "reduce", null,
                MapReduceCommand.OutputType.INLINE, new BasicDBObject());
        mapReduceCommand.setCollation(COLLATION);
        collection.mapReduce(mapReduceCommand);
        assertThat(executor.getReadOperation(), isTheSameAs(
                new MapReduceWithInlineResultsOperation<>(collection.getNamespace(), new BsonJavaScript("map"),
                        new BsonJavaScript("reduce"), collection.getDefaultDBObjectCodec())
                        .verbose(true)
                        .filter(new BsonDocument())
                        .collation(COLLATION)));
        assertEquals(ReadConcern.MAJORITY, executor.getReadConcern());
    }

    @Test
    @DisplayName("mapReduce should create the correct MapReduceToCollectionOperation")
    void mapReduceShouldCreateCorrectToCollectionOperation() {
        MapReduceStatistics stats = org.mockito.Mockito.mock(MapReduceStatistics.class);

        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(stats, stats, stats));
        DB db = new DB(getMongoClient(), "myDatabase", executor);
        DBCollection collection = db.getCollection("test");

        collection.mapReduce("map", "reduce", "myColl", MapReduceCommand.OutputType.REPLACE, new BasicDBObject());
        assertThat(executor.getWriteOperation(), isTheSameAs(
                new MapReduceToCollectionOperation(collection.getNamespace(), new BsonJavaScript("map"),
                        new BsonJavaScript("reduce"), "myColl", collection.getWriteConcern())
                        .verbose(true)
                        .filter(new BsonDocument())));

        // Inherits from DB
        collection.mapReduce("map", "reduce", "myColl", MapReduceCommand.OutputType.REPLACE, new BasicDBObject());
        assertThat(executor.getWriteOperation(), isTheSameAs(
                new MapReduceToCollectionOperation(collection.getNamespace(), new BsonJavaScript("map"),
                        new BsonJavaScript("reduce"), "myColl", collection.getWriteConcern())
                        .verbose(true)
                        .filter(new BsonDocument())));

        MapReduceCommand mapReduceCommand = new MapReduceCommand(collection, "map", "reduce", "myColl",
                MapReduceCommand.OutputType.REPLACE, new BasicDBObject());
        mapReduceCommand.setCollation(COLLATION);
        collection.mapReduce(mapReduceCommand);
        assertThat(executor.getWriteOperation(), isTheSameAs(
                new MapReduceToCollectionOperation(collection.getNamespace(), new BsonJavaScript("map"),
                        new BsonJavaScript("reduce"), "myColl", collection.getWriteConcern())
                        .verbose(true)
                        .filter(new BsonDocument())
                        .collation(COLLATION)));
    }

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("aggregate should create the correct AggregateOperation")
    void aggregateShouldCreateCorrectAggregateOperation() {
        MapReduceBatchCursor<DBObject> cursor = org.mockito.Mockito.mock(MapReduceBatchCursor.class);
        org.mockito.Mockito.when(cursor.hasNext()).thenReturn(false);

        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(cursor, cursor, cursor));
        DB db = new DB(getMongoClient(), "myDatabase", executor);
        DBCollection collection = db.getCollection("test");
        List<DBObject> pipeline = Collections.singletonList(BasicDBObject.parse("{$match: {}}"));
        List<BsonDocument> bsonPipeline = Collections.singletonList(BsonDocument.parse("{$match: {}}"));

        collection.aggregate(pipeline, AggregationOptions.builder().build());
        assertThat(executor.getReadOperation(), isTheSameAs(new AggregateOperation<>(collection.getNamespace(),
                bsonPipeline, collection.getDefaultDBObjectCodec()).retryReads(true)));
        assertEquals(ReadConcern.DEFAULT, executor.getReadConcern());

        // Inherits from DB
        db.setReadConcern(ReadConcern.MAJORITY);
        collection.aggregate(pipeline, AggregationOptions.builder().build());
        assertThat(executor.getReadOperation(), isTheSameAs(new AggregateOperation<>(collection.getNamespace(),
                bsonPipeline, collection.getDefaultDBObjectCodec()).retryReads(true)));
        assertEquals(ReadConcern.MAJORITY, executor.getReadConcern());

        collection.setReadConcern(ReadConcern.LOCAL);
        collection.aggregate(pipeline, AggregationOptions.builder().collation(COLLATION).build());
        assertThat(executor.getReadOperation(), isTheSameAs(new AggregateOperation<>(collection.getNamespace(),
                bsonPipeline, collection.getDefaultDBObjectCodec()).collation(COLLATION).retryReads(true)));
        assertEquals(ReadConcern.LOCAL, executor.getReadConcern());
    }

    @Test
    @DisplayName("aggregate should create the correct AggregateToCollectionOperation")
    void aggregateShouldCreateCorrectAggregateToCollectionOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null, null));
        DB db = new DB(getMongoClient(), "myDatabase", executor);
        DBCollection collection = db.getCollection("test");
        List<DBObject> pipeline = Arrays.asList(BasicDBObject.parse("{$match: {}}"), BasicDBObject.parse("{$out: \"myColl\"}"));
        List<BsonDocument> bsonPipeline = Arrays.asList(BsonDocument.parse("{$match: {}}"), BsonDocument.parse("{$out: \"myColl\"}"));

        collection.aggregate(pipeline, AggregationOptions.builder().build());
        assertThat(executor.getReadOperation(), isTheSameAs(new AggregateToCollectionOperation(collection.getNamespace(),
                bsonPipeline, collection.getReadConcern(), collection.getWriteConcern())));

        // Inherits from DB
        collection.aggregate(pipeline, AggregationOptions.builder().build());
        assertThat(executor.getReadOperation(), isTheSameAs(new AggregateToCollectionOperation(collection.getNamespace(),
                bsonPipeline, collection.getReadConcern(), collection.getWriteConcern())));

        collection.aggregate(pipeline, AggregationOptions.builder().collation(COLLATION).build());
        assertThat(executor.getReadOperation(), isTheSameAs(new AggregateToCollectionOperation(collection.getNamespace(),
                bsonPipeline, collection.getReadConcern(), collection.getWriteConcern()).collation(COLLATION)));
    }

    @Test
    @DisplayName("explainAggregate should create the correct AggregateOperation")
    void explainAggregateShouldCreateCorrectAggregateOperation() {
        BsonDocument result = BsonDocument.parse("{ok: 1}");
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(result, result, result));
        DB db = new DB(getMongoClient(), "myDatabase", executor);
        DBCollection collection = db.getCollection("test");
        AggregationOptions options = AggregationOptions.builder().collation(COLLATION).build();
        List<DBObject> pipeline = Collections.singletonList(BasicDBObject.parse("{$match: {}}"));
        List<BsonDocument> bsonPipeline = Collections.singletonList(BsonDocument.parse("{$match: {}}"));

        collection.explainAggregate(pipeline, options);
        assertThat(executor.getReadOperation(), isTheSameAs(new AggregateOperation<>(collection.getNamespace(),
                bsonPipeline, collection.getDefaultDBObjectCodec()).retryReads(true).collation(COLLATION)
                .asExplainableOperation(ExplainVerbosity.QUERY_PLANNER, new BsonDocumentCodec())));

        // Inherits from DB
        db.setReadConcern(ReadConcern.MAJORITY);
        collection.explainAggregate(pipeline, options);
        assertThat(executor.getReadOperation(), isTheSameAs(new AggregateOperation<>(collection.getNamespace(),
                bsonPipeline, collection.getDefaultDBObjectCodec()).retryReads(true).collation(COLLATION)
                .asExplainableOperation(ExplainVerbosity.QUERY_PLANNER, new BsonDocumentCodec())));

        collection.setReadConcern(ReadConcern.LOCAL);
        collection.explainAggregate(pipeline, options);
        assertThat(executor.getReadOperation(), isTheSameAs(new AggregateOperation<>(collection.getNamespace(),
                bsonPipeline, collection.getDefaultDBObjectCodec()).retryReads(true).collation(COLLATION)
                .asExplainableOperation(ExplainVerbosity.QUERY_PLANNER, new BsonDocumentCodec())));
    }

    @ParameterizedTest(name = "arrayFilters={0}")
    @MethodSource("arrayFiltersProvider")
    @DisplayName("update should create the correct UpdateOperation")
    void updateShouldCreateCorrectUpdateOperation(final List<BasicDBObject> dbObjectArrayFilters,
                                                    final List<BsonDocument> bsonArrayFilters) {
        WriteConcernResult result = org.mockito.Mockito.mock(WriteConcernResult.class);
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(result, result, result));
        DB db = new DB(getMongoClient(), "myDatabase", executor);
        boolean retryWrites = db.getMongoClient().getMongoClientOptions().getRetryWrites();
        DBCollection collection = db.getCollection("test");
        String query = "{a: 1}";
        String update = "{$set: {a: 2}}";

        UpdateRequest updateRequest = new UpdateRequest(BsonDocument.parse(query), BsonDocument.parse(update),
                com.mongodb.internal.bulk.WriteRequest.Type.UPDATE).multi(false);
        collection.update(BasicDBObject.parse(query), BasicDBObject.parse(update));
        assertThat(executor.getWriteOperation(), isTheSameAs(createBulkWriteOperationForUpdate(collection.getNamespace(),
                true, WriteConcern.ACKNOWLEDGED, retryWrites, asList(updateRequest))));

        // Inherits from DB
        db.setWriteConcern(WriteConcern.W3);
        collection.update(BasicDBObject.parse(query), BasicDBObject.parse(update));
        assertThat(executor.getWriteOperation(), isTheSameAs(createBulkWriteOperationForUpdate(collection.getNamespace(),
                true, WriteConcern.W3, retryWrites, asList(updateRequest))));

        collection.setWriteConcern(WriteConcern.W1);
        updateRequest.collation(COLLATION);
        collection.update(BasicDBObject.parse(query), BasicDBObject.parse(update),
                new DBCollectionUpdateOptions().collation(COLLATION).arrayFilters(dbObjectArrayFilters));
        assertThat(executor.getWriteOperation(), isTheSameAs(createBulkWriteOperationForUpdate(collection.getNamespace(),
                true, WriteConcern.W1, retryWrites, asList(updateRequest.arrayFilters(bsonArrayFilters)))));
    }

    @Test
    @DisplayName("remove should create the correct DeleteOperation")
    void removeShouldCreateCorrectDeleteOperation() {
        WriteConcernResult result = org.mockito.Mockito.mock(WriteConcernResult.class);
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(result, result, result));
        DB db = new DB(getMongoClient(), "myDatabase", executor);
        boolean retryWrites = db.getMongoClient().getMongoClientOptions().getRetryWrites();
        DBCollection collection = db.getCollection("test");
        String query = "{a: 1}";

        DeleteRequest deleteRequest = new DeleteRequest(BsonDocument.parse(query));
        collection.remove(BasicDBObject.parse(query));
        assertThat(executor.getWriteOperation(), isTheSameAs(createBulkWriteOperationForDelete(collection.getNamespace(),
                false, WriteConcern.ACKNOWLEDGED, retryWrites, asList(deleteRequest))));

        // Inherits from DB
        db.setWriteConcern(WriteConcern.W3);
        collection.remove(BasicDBObject.parse(query));
        assertThat(executor.getWriteOperation(), isTheSameAs(createBulkWriteOperationForDelete(collection.getNamespace(),
                false, WriteConcern.W3, retryWrites, asList(deleteRequest))));

        collection.setWriteConcern(WriteConcern.W1);
        deleteRequest.collation(COLLATION);
        collection.remove(BasicDBObject.parse(query), new DBCollectionRemoveOptions().collation(COLLATION));
        assertThat(executor.getWriteOperation(), isTheSameAs(createBulkWriteOperationForDelete(collection.getNamespace(),
                false, WriteConcern.W1, retryWrites, asList(deleteRequest))));
    }

    @ParameterizedTest(name = "ordered={0}")
    @MethodSource("bulkWriteProvider")
    @DisplayName("should create the correct MixedBulkWriteOperation")
    void shouldCreateCorrectMixedBulkWriteOperation(final boolean ordered,
                                                      final List<BasicDBObject> dbObjectArrayFilters,
                                                      final List<BsonDocument> bsonArrayFilters) {
        com.mongodb.bulk.BulkWriteResult result = org.mockito.Mockito.mock(com.mongodb.bulk.BulkWriteResult.class);
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(result, result, result));
        DB db = new DB(getMongoClient(), "myDatabase", executor);
        DBCollection collection = db.getCollection("test");
        String query = "{a: 1}";
        String update = "{$set: {level: 1}}";
        BasicDBObject insertedDocument = new BasicDBObject("_id", 1);
        InsertRequest insertRequest = new InsertRequest(
                new BsonDocumentWrapper<>(insertedDocument, collection.getDefaultDBObjectCodec()));
        UpdateRequest updateRequest = new UpdateRequest(BsonDocument.parse(query), BsonDocument.parse(update),
                com.mongodb.internal.bulk.WriteRequest.Type.UPDATE).multi(false).collation(COLLATION)
                .arrayFilters(bsonArrayFilters);
        DeleteRequest deleteRequest = new DeleteRequest(BsonDocument.parse(query)).multi(false).collation(FRENCH_COLLATION);
        List<com.mongodb.internal.bulk.WriteRequest> writeRequests = asList(insertRequest, updateRequest, deleteRequest);

        // Create bulk operation
        BulkWriteOperation bulkOp = ordered ? collection.initializeOrderedBulkOperation()
                : collection.initializeUnorderedBulkOperation();
        bulkOp.insert(insertedDocument);
        bulkOp.find(BasicDBObject.parse(query)).collation(COLLATION).arrayFilters(dbObjectArrayFilters)
                .updateOne(BasicDBObject.parse(update));
        bulkOp.find(BasicDBObject.parse(query)).collation(FRENCH_COLLATION).removeOne();
        bulkOp.execute();
        assertThat(executor.getWriteOperation(), isTheSameAs(new MixedBulkWriteOperation(collection.getNamespace(),
                writeRequests, ordered, WriteConcern.ACKNOWLEDGED, false)));

        // Inherits from DB
        db.setWriteConcern(WriteConcern.W3);
        bulkOp = ordered ? collection.initializeOrderedBulkOperation()
                : collection.initializeUnorderedBulkOperation();
        bulkOp.insert(insertedDocument);
        bulkOp.find(BasicDBObject.parse(query)).collation(COLLATION).arrayFilters(dbObjectArrayFilters)
                .updateOne(BasicDBObject.parse(update));
        bulkOp.find(BasicDBObject.parse(query)).collation(FRENCH_COLLATION).removeOne();
        bulkOp.execute();
        assertThat(executor.getWriteOperation(), isTheSameAs(new MixedBulkWriteOperation(collection.getNamespace(),
                writeRequests, ordered, WriteConcern.W3, false)));

        collection.setWriteConcern(WriteConcern.W1);
        bulkOp = ordered ? collection.initializeOrderedBulkOperation()
                : collection.initializeUnorderedBulkOperation();
        bulkOp.insert(insertedDocument);
        bulkOp.find(BasicDBObject.parse(query)).collation(COLLATION).arrayFilters(dbObjectArrayFilters)
                .updateOne(BasicDBObject.parse(update));
        bulkOp.find(BasicDBObject.parse(query)).collation(FRENCH_COLLATION).removeOne();
        bulkOp.execute();
        assertThat(executor.getWriteOperation(), isTheSameAs(new MixedBulkWriteOperation(collection.getNamespace(),
                writeRequests, ordered, WriteConcern.W1, false)));
    }

    static Stream<Arguments> arrayFiltersProvider() {
        return Stream.of(
                Arguments.of(null, null),
                Arguments.of(Collections.emptyList(), Collections.emptyList()),
                Arguments.of(Collections.singletonList(new BasicDBObject("i.b", 1)),
                        Collections.singletonList(new BsonDocumentWrapper<>(new BasicDBObject("i.b", 1),
                                DEFAULT_DBOBJECT_CODEC_FACTORY)))
        );
    }

    static Stream<Arguments> bulkWriteProvider() {
        return Stream.of(
                Arguments.of(true, null, null),
                Arguments.of(false, Collections.emptyList(), Collections.emptyList()),
                Arguments.of(true,
                        Collections.singletonList(new BasicDBObject("i.b", 1)),
                        Collections.singletonList(new BsonDocumentWrapper<>(new BasicDBObject("i.b", 1),
                                DEFAULT_DBOBJECT_CODEC_FACTORY)))
        );
    }
}
