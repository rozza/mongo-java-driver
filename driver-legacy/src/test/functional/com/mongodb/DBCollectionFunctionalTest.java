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

import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.model.DBCollectionCountOptions;
import com.mongodb.client.model.DBCollectionFindAndModifyOptions;
import com.mongodb.client.model.DBCollectionRemoveOptions;
import com.mongodb.client.model.DBCollectionUpdateOptions;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@SuppressWarnings("deprecation")
class DBCollectionFunctionalTest extends FunctionalSpecification {

    private Object idOfExistingDocument;

    private static final Collation CASE_INSENSITIVE = Collation.builder()
            .locale("en").collationStrength(CollationStrength.SECONDARY).build();

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        BasicDBObject existingDocument = new BasicDBObject("a", new BasicDBObject())
                .append("b", new BasicDBObject());
        collection.insert(existingDocument);
        idOfExistingDocument = existingDocument.get("_id");
        collection.setObjectClass(BasicDBObject.class);
    }

    @Test
    @DisplayName("should update a document")
    void shouldUpdateADocument() {
        collection.update(new BasicDBObject("_id", 1), new BasicDBObject("$set", new BasicDBObject("x", 1)), true, false);
        assertEquals(new BasicDBObject("_id", 1).append("x", 1), collection.findOne(new BasicDBObject("_id", 1)));

        collection.update(new BasicDBObject("_id", 2), new BasicDBObject("$set", new BasicDBObject("x", 1)));
        assertNull(collection.findOne(new BasicDBObject("_id", 2)));
    }

    @Test
    @DisplayName("should update multiple documents")
    void shouldUpdateMultipleDocuments() {
        collection.insert(Arrays.asList(new BasicDBObject("x", 1), new BasicDBObject("x", 1)));

        collection.update(new BasicDBObject("x", 1), new BasicDBObject("$set", new BasicDBObject("x", 2)), false, true);

        assertEquals(2, collection.count(new BasicDBObject("x", 2)));
    }

    @Test
    @DisplayName("should replace a document")
    void shouldReplaceADocument() {
        collection.update(new BasicDBObject("_id", 1), new BasicDBObject("_id", 1).append("x", 1), true, false);
        assertEquals(new BasicDBObject("_id", 1).append("x", 1), collection.findOne(new BasicDBObject("_id", 1)));

        collection.update(new BasicDBObject("_id", 2), new BasicDBObject("_id", 2).append("x", 1));
        assertNull(collection.findOne(new BasicDBObject("_id", 2)));
    }

    @Test
    @DisplayName("should drop collection that exists")
    void shouldDropCollectionThatExists() {
        collection.insert(new BasicDBObject("name", "myName"));

        collection.drop();

        assertFalse(database.getCollectionNames().contains(getCollectionName()));
    }

    @Test
    @DisplayName("should not error when dropping a collection that does not exist")
    void shouldNotErrorWhenDroppingNonExistentCollection() {
        collection.drop(); // Drop the collection created by setUp()
        assertFalse(database.getCollectionNames().contains(getCollectionName()));

        collection.drop();
        // No exception means test passes
    }

    @Test
    @DisplayName("should use top-level class for findAndModify")
    void shouldUseTopLevelClassForFindAndModify() {
        collection.setObjectClass(ClassA.class);

        DBObject document = collection.findAndModify(null,
                new BasicDBObject("_id", idOfExistingDocument).append("c", 1));

        assertTrue(document instanceof ClassA);
    }

    @Test
    @DisplayName("should use internal classes for findAndModify")
    void shouldUseInternalClassesForFindAndModify() {
        collection.setInternalClass("a", ClassA.class);
        collection.setInternalClass("b", ClassB.class);

        DBObject document = collection.findAndModify(null,
                new BasicDBObject("_id", idOfExistingDocument).append("c", 1));

        assertTrue(document.get("a") instanceof ClassA);
        assertTrue(document.get("b") instanceof ClassB);
    }

    @Test
    @DisplayName("should support index options")
    void shouldSupportIndexOptions() {
        BasicDBObject options = new BasicDBObject("sparse", true)
                .append("background", true)
                .append("expireAfterSeconds", 42);

        collection.createIndex(new BasicDBObject("y", 1), options);

        assertEquals(2, collection.getIndexInfo().size());

        DBObject document = collection.getIndexInfo().get(1);
        assertEquals(42, document.get("expireAfterSeconds"));
        assertEquals(true, document.get("background"));
    }

    @Test
    @DisplayName("drop index should not fail if collection does not exist")
    void dropIndexShouldNotFailIfCollectionDoesNotExist() {
        collection.drop();
        collection.dropIndex("indexOnCollectionThatDoesNotExist");
    }

    @Test
    @DisplayName("drop index should error if index does not exist")
    void dropIndexShouldErrorIfIndexDoesNotExist() {
        assumeFalse(serverVersionAtLeast(8, 3));

        collection.createIndex(new BasicDBObject("x", 1));

        MongoCommandException exception = assertThrows(MongoCommandException.class, () ->
                collection.dropIndex("y_1"));
        assertTrue(exception.getErrorMessage().contains("index not found"));
    }

    @Test
    @DisplayName("should throw Exception if dropping an index with an incorrect type")
    void shouldThrowExceptionIfDroppingIndexWithIncorrectType() {
        assumeFalse(serverVersionAtLeast(8, 3));

        BasicDBObject index = new BasicDBObject("x", 1);
        collection.createIndex(index);

        MongoCommandException exception = assertThrows(MongoCommandException.class, () ->
                collection.dropIndex(new BasicDBObject("x", "2d")));
        assertTrue(exception.getErrorMessage().contains("can't find index"));
    }

    @Test
    @DisplayName("should drop nested index")
    void shouldDropNestedIndex() {
        collection.save(new BasicDBObject("x", new BasicDBObject("y", 1)));
        BasicDBObject index = new BasicDBObject("x.y", 1);
        collection.createIndex(index);
        assertEquals(2, collection.getIndexInfo().size());

        collection.dropIndex(index);

        assertEquals(1, collection.getIndexInfo().size());
    }

    @Test
    @DisplayName("should drop all indexes except the default index on _id")
    void shouldDropAllIndexesExceptDefault() {
        collection.createIndex(new BasicDBObject("x", 1));
        collection.createIndex(new BasicDBObject("x.y", 1));
        assertEquals(3, collection.getIndexInfo().size());

        collection.dropIndexes();

        assertEquals(1, collection.getIndexInfo().size());
    }

    @Test
    @DisplayName("should drop unique index")
    void shouldDropUniqueIndex() {
        BasicDBObject index = new BasicDBObject("x", 1);
        collection.createIndex(index, new BasicDBObject("unique", true));

        collection.dropIndex(index);

        assertEquals(1, collection.getIndexInfo().size());
    }

    @Test
    @DisplayName("should use compound index for min query")
    void shouldUseCompoundIndexForMinQuery() {
        collection.createIndex(new BasicDBObject("a", 1).append("_id", 1));

        DBCursor cursor = collection.find().min(new BasicDBObject("a", 1).append("_id", idOfExistingDocument));

        assertEquals(1, cursor.size());
    }

    @Test
    @DisplayName("should be able to rename a collection")
    void shouldBeAbleToRenameACollection() {
        assertTrue(database.getCollectionNames().contains(getCollectionName()));
        String newCollectionName = "someNewName";

        collection.rename(newCollectionName);

        assertFalse(database.getCollectionNames().contains(getCollectionName()));
        assertNotNull(database.getCollection(newCollectionName));
        assertTrue(database.getCollectionNames().contains(newCollectionName));
    }

    @Test
    @DisplayName("should be able to rename collection to an existing collection name and replace it when drop is true")
    void shouldBeAbleToRenameCollectionToExistingAndReplace() {
        String existingCollectionName = "anExistingCollection";
        String originalCollectionName = "someOriginalCollection";

        DBCollection originalCollection = database.getCollection(originalCollectionName);
        String keyInOriginalCollection = "someKey";
        String valueInOriginalCollection = "someValue";
        originalCollection.insert(new BasicDBObject(keyInOriginalCollection, valueInOriginalCollection));

        DBCollection existingCollection = database.getCollection(existingCollectionName);
        String keyInExistingCollection = "aDifferentDocument";
        existingCollection.insert(new BasicDBObject(keyInExistingCollection, "withADifferentValue"));

        assertTrue(database.getCollectionNames().contains(originalCollectionName));
        assertTrue(database.getCollectionNames().contains(existingCollectionName));

        originalCollection.rename(existingCollectionName, true);

        assertFalse(database.getCollectionNames().contains(originalCollectionName));
        assertTrue(database.getCollectionNames().contains(existingCollectionName));

        DBCollection replacedCollection = database.getCollection(existingCollectionName);
        assertNull(replacedCollection.findOne().get(keyInExistingCollection));
        assertEquals(valueInOriginalCollection, replacedCollection.findOne().get(keyInOriginalCollection).toString());
    }

    @Test
    @DisplayName("should return a list of all the values of a given field without duplicates")
    @SuppressWarnings("unchecked")
    void shouldReturnDistinctValues() {
        collection.drop();
        for (int i = 0; i < 100; i++) {
            collection.save(new BasicDBObject("_id", i).append("x", i % 10));
        }
        assertEquals(100, collection.count());

        List<Object> distinctValuesOfFieldX = collection.distinct("x");

        assertEquals(10, distinctValuesOfFieldX.size());
        assertThat(distinctValuesOfFieldX, contains(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    }

    @Test
    @DisplayName("should query database for values and return a list of all the distinct values of a given field that match the filter")
    @SuppressWarnings("unchecked")
    void shouldReturnDistinctValuesMatchingFilter() {
        collection.drop();
        for (int i = 0; i < 100; i++) {
            collection.save(new BasicDBObject("_id", i).append("x", i % 10).append("isOddNumber", i % 2));
        }
        assertEquals(100, collection.count());

        List<Object> distinctValuesOfFieldX = collection.distinct("x", new BasicDBObject("isOddNumber", 1));

        assertEquals(5, distinctValuesOfFieldX.size());
        assertThat(distinctValuesOfFieldX, contains(1, 3, 5, 7, 9));
    }

    @Test
    @DisplayName("should return distinct values of differing types")
    @SuppressWarnings("unchecked")
    void shouldReturnDistinctValuesOfDifferingTypes() {
        collection.drop();
        List<DBObject> documents = Arrays.asList(
                new BasicDBObject("id", null),
                new BasicDBObject("id", "a"),
                new BasicDBObject("id", 1),
                new BasicDBObject("id", new BasicDBObject("b", "c")),
                new BasicDBObject("id", new BasicDBObject("list", Arrays.asList(2, "d", new BasicDBObject("e", 3))))
        );

        collection.insert(documents);

        List<Object> distinctValues = collection.distinct("id");

        assertEquals(5, distinctValues.size());
        assertThat(distinctValues, containsInAnyOrder(
                null, "a", 1,
                new BasicDBObject("b", "c"),
                new BasicDBObject("list", Arrays.asList(2, "d", new BasicDBObject("e", 3)))));
    }

    @Test
    @DisplayName("should return null when findOne finds nothing")
    void shouldReturnNullWhenFindOneFindsNothing() {
        assertNull(collection.findOne(new BasicDBObject("field", "That Does Not Exist")));
    }

    @Test
    @DisplayName("should return null when findOne finds nothing and a projection field is specified")
    void shouldReturnNullWhenFindOneFindsNothingWithProjection() {
        collection.drop();
        assertNull(collection.findOne(null, new BasicDBObject("_id", true)));
    }

    @ParameterizedTest(name = "criteria={0}")
    @MethodSource("findOneCriteriaProvider")
    @DisplayName("should return result when performing findOne with criteria")
    void shouldReturnResultWhenPerformingFindOne(final Object criteria, final DBObject result) {
        collection.drop();
        collection.insert(new BasicDBObject("_id", 100).append("x", 1).append("y", 2));
        collection.insert(new BasicDBObject("_id", 123).append("x", 2).append("z", 2));

        if (criteria instanceof DBObject) {
            assertEquals(result, collection.findOne((DBObject) criteria));
        } else {
            assertEquals(result, collection.findOne(criteria));
        }
    }

    static Stream<Arguments> findOneCriteriaProvider() {
        return Stream.of(
                Arguments.of(123, new BasicDBObject("_id", 123).append("x", 2).append("z", 2)),
                Arguments.of(new BasicDBObject("x", 1), new BasicDBObject("_id", 100).append("x", 1).append("y", 2))
        );
    }

    @ParameterizedTest(name = "criteria={0}, projection={1}")
    @MethodSource("findOneWithProjectionProvider")
    @DisplayName("should return result when performing findOne with criteria and projection")
    void shouldReturnResultWhenPerformingFindOneWithProjection(final Object criteria, final DBObject projection,
                                                                final DBObject result) {
        collection.drop();
        collection.insert(new BasicDBObject("_id", 100).append("x", 1).append("y", 2));
        collection.insert(new BasicDBObject("_id", 123).append("x", 2).append("z", 2));

        if (criteria instanceof DBObject) {
            assertEquals(result, collection.findOne((DBObject) criteria, projection));
        } else {
            assertEquals(result, collection.findOne(criteria, projection));
        }
    }

    static Stream<Arguments> findOneWithProjectionProvider() {
        return Stream.of(
                Arguments.of(123, new BasicDBObject("x", 1), new BasicDBObject("_id", 123).append("x", 2)),
                Arguments.of(new BasicDBObject("x", 1), new BasicDBObject("y", 1),
                        new BasicDBObject("_id", 100).append("y", 2))
        );
    }

    @ParameterizedTest(name = "criteria={0}, sortBy={1}")
    @MethodSource("sortBeforeSelectingFirstProvider")
    @DisplayName("should sort and filter before selecting first result")
    void shouldSortAndFilterBeforeSelectingFirstResult(final DBObject criteria, final DBObject sortBy,
                                                        final int expectedId) {
        collection.drop();
        collection.insert(new BasicDBObject("_id", 1).append("x", 100).append("y", "abc"));
        collection.insert(new BasicDBObject("_id", 2).append("x", 200).append("y", "abc"));
        collection.insert(new BasicDBObject("_id", 3).append("x", 1).append("y", "abc"));
        collection.insert(new BasicDBObject("_id", 4).append("x", -100).append("y", "xyz"));
        collection.insert(new BasicDBObject("_id", 5).append("x", -50).append("y", "zzz"));
        collection.insert(new BasicDBObject("_id", 6).append("x", 9).append("y", "aaa"));

        assertEquals(expectedId, collection.findOne(criteria, null, sortBy).get("_id"));
    }

    static Stream<Arguments> sortBeforeSelectingFirstProvider() {
        return Stream.of(
                Arguments.of(new BasicDBObject(), new BasicDBObject("x", 1), 4),
                Arguments.of(new BasicDBObject(), new BasicDBObject("x", -1), 2),
                Arguments.of(new BasicDBObject("x", 1), new BasicDBObject("x", 1).append("y", 1), 3),
                Arguments.of(QueryBuilder.start("x").lessThan(2).get(), new BasicDBObject("y", -1), 5)
        );
    }

    @Test
    @DisplayName("should throw WriteConcernException on write concern error for rename")
    void shouldThrowWriteConcernExceptionForRename() {
        assumeTrue(isDiscoverableReplicaSet());
        assertTrue(database.getCollectionNames().contains(getCollectionName()));
        collection.setWriteConcern(new WriteConcern(5));
        try {
            WriteConcernException e = assertThrows(WriteConcernException.class, () ->
                    collection.rename("someOtherNewName"));
            assertEquals(100, e.getErrorCode());
        } finally {
            collection.setWriteConcern(null);
        }
    }

    @Test
    @DisplayName("should throw WriteConcernException on write concern error for drop")
    void shouldThrowWriteConcernExceptionForDrop() {
        assumeTrue(isDiscoverableReplicaSet());
        assertTrue(database.getCollectionNames().contains(getCollectionName()));
        collection.setWriteConcern(new WriteConcern(5));
        try {
            WriteConcernException e = assertThrows(WriteConcernException.class, () ->
                    collection.drop());
            assertEquals(100, e.getErrorCode());
        } finally {
            collection.setWriteConcern(null);
        }
    }

    @Test
    @DisplayName("should throw WriteConcernException on write concern error for createIndex")
    void shouldThrowWriteConcernExceptionForCreateIndex() {
        assumeTrue(isDiscoverableReplicaSet());
        assertTrue(database.getCollectionNames().contains(getCollectionName()));
        collection.setWriteConcern(new WriteConcern(5));
        try {
            WriteConcernException e = assertThrows(WriteConcernException.class, () ->
                    collection.createIndex(new BasicDBObject("somekey", 1)));
            assertEquals(100, e.getErrorCode());
        } finally {
            collection.setWriteConcern(null);
        }
    }

    @Test
    @DisplayName("should throw WriteConcernException on write concern error for dropIndex")
    void shouldThrowWriteConcernExceptionForDropIndex() {
        assumeTrue(isDiscoverableReplicaSet());
        assertTrue(database.getCollectionNames().contains(getCollectionName()));
        collection.createIndex(new BasicDBObject("somekey", 1));
        collection.setWriteConcern(new WriteConcern(5));
        try {
            WriteConcernException e = assertThrows(WriteConcernException.class, () ->
                    collection.dropIndex(new BasicDBObject("somekey", 1)));
            assertEquals(100, e.getErrorCode());
        } finally {
            collection.setWriteConcern(null);
        }
    }

    @Test
    @DisplayName("should support creating an index with collation options")
    void shouldSupportCreatingIndexWithCollationOptions() {
        Collation collation = Collation.builder()
                .locale("en")
                .caseLevel(true)
                .collationCaseFirst(CollationCaseFirst.OFF)
                .collationStrength(CollationStrength.IDENTICAL)
                .numericOrdering(true)
                .normalization(false)
                .collationAlternate(CollationAlternate.SHIFTED)
                .collationMaxVariable(CollationMaxVariable.SPACE)
                .backwards(true)
                .build();

        BasicDBObject options = BasicDBObject.parse("{ collation: { locale: \"en\", caseLevel: true, caseFirst: \"off\", strength: 5,"
                + " numericOrdering: true, alternate: \"shifted\",  maxVariable: \"space\", backwards: true }}");

        collection.drop();
        collection.createIndex(new BasicDBObject("y", 1), new BasicDBObject(options));

        assertEquals(2, collection.getIndexInfo().size());

        BsonDocument indexCollation = new BsonDocumentWrapper<>(
                (DBObject) collection.getIndexInfo().get(1).get("collation"),
                collection.getDefaultDBObjectCodec());
        indexCollation.remove("version");

        assertEquals(collation.asDocument(), indexCollation);
    }

    @Test
    @DisplayName("should find with collation")
    void shouldFindWithCollation() {
        DBObject document = BasicDBObject.parse("{_id: 1, str: \"foo\"}");
        collection.insert(document);

        DBCursor result = collection.find(BasicDBObject.parse("{str: \"FOO\"}"));
        assertFalse(result.hasNext());

        result = collection.find(BasicDBObject.parse("{str: \"FOO\"}")).setCollation(CASE_INSENSITIVE);
        assertTrue(result.hasNext());
        assertEquals(document, result.next());
    }

    @Test
    @DisplayName("should aggregate with collation")
    void shouldAggregateWithCollation() {
        DBObject document = BasicDBObject.parse("{_id: 1, str: \"foo\"}");
        collection.insert(document);

        Cursor result = collection.aggregate(
                Arrays.asList(BasicDBObject.parse("{ $match: { str: \"FOO\"}}")),
                AggregationOptions.builder().build());
        assertFalse(result.hasNext());

        result = collection.aggregate(
                Arrays.asList(BasicDBObject.parse("{ $match: { str: \"FOO\"}}")),
                AggregationOptions.builder().collation(CASE_INSENSITIVE).build());
        assertTrue(result.hasNext());
        assertEquals(document, result.next());
    }

    @Test
    @DisplayName("should count with collation")
    void shouldCountWithCollation() {
        collection.insert(BasicDBObject.parse("{_id: 1, str: \"foo\"}"));

        assertEquals(0L, collection.count(BasicDBObject.parse("{str: \"FOO\"}")));
        assertEquals(1L, collection.count(BasicDBObject.parse("{str: \"FOO\"}"),
                new DBCollectionCountOptions().collation(CASE_INSENSITIVE)));
    }

    @Test
    @DisplayName("should update with collation")
    void shouldUpdateWithCollation() {
        collection.insert(BasicDBObject.parse("{_id: 1, str: \"foo\"}"));

        WriteResult result = collection.update(BasicDBObject.parse("{str: \"FOO\"}"), BasicDBObject.parse("{str: \"bar\"}"));
        assertEquals(0, result.getN());

        result = collection.update(BasicDBObject.parse("{str: \"FOO\"}"), BasicDBObject.parse("{str: \"bar\"}"),
                new DBCollectionUpdateOptions().collation(CASE_INSENSITIVE));
        assertEquals(1, result.getN());
    }

    @Test
    @DisplayName("should remove with collation")
    void shouldRemoveWithCollation() {
        collection.insert(BasicDBObject.parse("{_id: 1, str: \"foo\"}"));

        WriteResult result = collection.remove(BasicDBObject.parse("{str: \"FOO\"}"));
        assertEquals(0, result.getN());

        result = collection.remove(BasicDBObject.parse("{str: \"FOO\"}"),
                new DBCollectionRemoveOptions().collation(CASE_INSENSITIVE));
        assertEquals(1, result.getN());
    }

    @Test
    @DisplayName("should find and modify with collation")
    void shouldFindAndModifyWithCollation() {
        DBObject document = BasicDBObject.parse("{_id: 1, str: \"foo\"}");
        collection.insert(document);

        DBObject result = collection.findAndModify(BasicDBObject.parse("{str: \"FOO\"}"),
                new DBCollectionFindAndModifyOptions().update(BasicDBObject.parse("{_id: 1, str: \"BAR\"}")));
        assertNull(result);

        result = collection.findAndModify(BasicDBObject.parse("{str: \"FOO\"}"),
                new DBCollectionFindAndModifyOptions().update(BasicDBObject.parse("{_id: 1, str: \"BAR\"}"))
                        .collation(CASE_INSENSITIVE));
        assertEquals(document, result);
    }

    @Test
    @DisplayName("should drop compound index by key")
    void shouldDropCompoundIndexByKey() {
        BasicDBObject indexKeys = new BasicDBObject("x", 1).append("y", -1);
        collection.createIndex(indexKeys);

        collection.dropIndex(indexKeys);

        assertEquals(1, collection.getIndexInfo().size());
    }
}
