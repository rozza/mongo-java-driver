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
import com.mongodb.client.model.CollationStrength;
import com.mongodb.internal.operation.BatchCursor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("deprecation")
class DBCursorFunctionalTest extends FunctionalSpecification {

    private static final Map<String, Object> CURSOR_MAP = Collections.singletonMap("a", 1);
    private static final Collation CASE_INSENSITIVE_COLLATION = Collation.builder()
            .locale("en").collationStrength(CollationStrength.SECONDARY).build();

    private DBCursor dbCursor;

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        collection.insert(new BasicDBObject("a", 1));
    }

    @Test
    @DisplayName("should use provided decoder factory")
    void shouldUseProvidedDecoderFactory() {
        DBDecoder decoder = mock(DBDecoder.class);
        DBDecoderFactory factory = mock(DBDecoderFactory.class);
        when(factory.create()).thenReturn(decoder);

        dbCursor = collection.find();
        dbCursor.setDecoderFactory(factory);
        dbCursor.next();

        verify(decoder, times(1)).decode(any(byte[].class), any(DBCollection.class));
    }

    @Test
    @DisplayName("should use provided hints for queries")
    void shouldUseProvidedHintsForQueries() {
        collection.createIndex(new BasicDBObject("a", 1));

        dbCursor = collection.find().hint(new BasicDBObject("a", 1));
        DBObject explainPlan = dbCursor.explain();
        assertEquals(CURSOR_MAP, getKeyPattern(explainPlan));

        dbCursor = collection.find().hint(new BasicDBObject("a", 1));
        explainPlan = dbCursor.explain();
        assertEquals(CURSOR_MAP, getKeyPattern(explainPlan));
    }

    @Test
    @DisplayName("should use provided hint for count")
    void shouldUseProvidedHintForCount() {
        collection.createIndex(new BasicDBObject("a", 1));
        assertEquals(1, collection.find().hint("a_1").count());
        assertEquals(1, collection.find().hint(new BasicDBObject("a", 1)).count());
    }

    @Test
    @DisplayName("should use provided hints for find")
    void shouldUseProvidedHintsForFind() {
        collection.createIndex(new BasicDBObject("a", 1));

        dbCursor = collection.find().hint(new BasicDBObject("a", 1));
        assertNotNull(dbCursor.one());

        dbCursor = collection.find().hint("a_1");
        assertNotNull(dbCursor.one());
    }

    @Test
    @DisplayName("should use provided hints for count")
    void shouldUseProvidedHintsForCountWithMultipleDocuments() {
        collection.insert(new BasicDBObject("a", 2));
        assertEquals(2, collection.find().count());

        collection.createIndex(new BasicDBObject("a", 1));
        assertEquals(1, collection.find(new BasicDBObject("a", 1)).hint("_id_").count());
        assertEquals(2, collection.find().hint("_id_").count());

        collection.createIndex(new BasicDBObject("x", 1), new BasicDBObject("sparse", true));
        assertEquals(0, collection.find(new BasicDBObject("a", 1)).hint("x_1").count());
        assertEquals(2, collection.find().hint("a_1").count());
    }

    @Test
    @DisplayName("should throw with bad hint")
    void shouldThrowWithBadHint() {
        assertThrows(MongoException.class, () ->
                collection.find(new BasicDBObject("a", 1)).hint("BAD HINT").count());

        assertThrows(MongoException.class, () ->
                collection.find(new BasicDBObject("a", 1)).hint("BAD HINT").one());

        assertThrows(MongoException.class, () ->
                collection.find(new BasicDBObject("a", 1)).hint(new BasicDBObject("BAD HINT", 1)).one());
    }

    @Test
    @DisplayName("should return results in the order they are on disk when natural sort applied")
    void shouldReturnResultsInNaturalOrder() {
        collection.insert(new BasicDBObject("name", "Chris"));
        collection.insert(new BasicDBObject("name", "Adam"));
        collection.insert(new BasicDBObject("name", "Bob"));

        dbCursor = collection.find(new BasicDBObject("name", new BasicDBObject("$exists", true)))
                .sort(new BasicDBObject("$natural", 1));

        List<String> names = new ArrayList<>();
        while (dbCursor.hasNext()) {
            names.add((String) dbCursor.next().get("name"));
        }
        assertEquals(Arrays.asList("Chris", "Adam", "Bob"), names);
    }

    @Test
    @DisplayName("should return results in the reverse order they are on disk when natural sort of minus one applied")
    void shouldReturnResultsInReverseNaturalOrder() {
        collection.insert(new BasicDBObject("name", "Chris"));
        collection.insert(new BasicDBObject("name", "Adam"));
        collection.insert(new BasicDBObject("name", "Bob"));

        dbCursor = collection.find(new BasicDBObject("name", new BasicDBObject("$exists", true)))
                .sort(new BasicDBObject("$natural", -1));

        List<String> names = new ArrayList<>();
        while (dbCursor.hasNext()) {
            names.add((String) dbCursor.next().get("name"));
        }
        assertEquals(Arrays.asList("Bob", "Adam", "Chris"), names);
    }

    @Test
    @DisplayName("should sort in reverse order")
    void shouldSortInReverseOrder() {
        for (int i = 1; i <= 10; i++) {
            collection.insert(new BasicDBObject("x", i));
        }

        DBCursor cursor = collection.find().sort(new BasicDBObject("x", -1));
        assertEquals(10, cursor.next().get("x"));
    }

    @Test
    @DisplayName("should sort in order")
    void shouldSortInOrder() {
        for (int i = 80; i <= 89; i++) {
            collection.insert(new BasicDBObject("x", i));
        }

        DBCursor cursor = collection.find(new BasicDBObject("x", new BasicDBObject("$exists", true)))
                .sort(new BasicDBObject("x", 1));
        assertEquals(80, cursor.next().get("x"));
    }

    @Test
    @DisplayName("should sort on two fields")
    void shouldSortOnTwoFields() {
        collection.insert(new BasicDBObject("_id", 1).append("name", "Chris"));
        collection.insert(new BasicDBObject("_id", 2).append("name", "Adam"));
        collection.insert(new BasicDBObject("_id", 3).append("name", "Bob"));
        collection.insert(new BasicDBObject("_id", 5).append("name", "Adam"));
        collection.insert(new BasicDBObject("_id", 4).append("name", "Adam"));

        dbCursor = collection.find(new BasicDBObject("name", new BasicDBObject("$exists", true)))
                .sort(new BasicDBObject("name", 1).append("_id", 1));

        List<List<Object>> results = new ArrayList<>();
        while (dbCursor.hasNext()) {
            DBObject obj = dbCursor.next();
            results.add(Arrays.asList(obj.get("name"), obj.get("_id")));
        }
        assertEquals(Arrays.asList(
                Arrays.asList("Adam", 2), Arrays.asList("Adam", 4), Arrays.asList("Adam", 5),
                Arrays.asList("Bob", 3), Arrays.asList("Chris", 1)),
                results);
    }

    @Test
    @DisplayName("DBCursor options should set the correct read preference")
    void dbCursorOptionsShouldSetCorrectReadPreference() {
        BatchCursor<DBObject> tailableCursor = new BatchCursor<DBObject>() {
            @Override
            public List<DBObject> tryNext() { return null; }

            @Override
            public void close() { }

            @Override
            public boolean hasNext() { return true; }

            @Override
            public List<DBObject> next() { return null; }

            @Override
            public int available() { return 0; }

            @Override
            public void setBatchSize(final int batchSize) { }

            @Override
            public int getBatchSize() { return 0; }

            @Override
            public void remove() { }

            @Override
            public ServerCursor getServerCursor() { return null; }

            @Override
            public ServerAddress getServerAddress() { return null; }
        };

        TestOperationExecutor executor = new TestOperationExecutor(
                Arrays.asList(tailableCursor, tailableCursor, tailableCursor, tailableCursor));
        DBCollection coll = new DBCollection("collectionName", database, executor);

        coll.find().hasNext();
        assertEquals(ReadPreference.primary(), executor.getReadPreference());

        coll.find().cursorType(CursorType.Tailable).tryNext();
        assertEquals(ReadPreference.primary(), executor.getReadPreference());

        coll.find().cursorType(CursorType.Tailable).setReadPreference(ReadPreference.secondaryPreferred()).tryNext();
        assertEquals(ReadPreference.secondaryPreferred(), executor.getReadPreference());
    }

    @Test
    @DisplayName("should support collation")
    void shouldSupportCollation() {
        DBObject document = BasicDBObject.parse("{_id: 1, str: \"foo\"}");
        collection.insert(document);
        dbCursor = collection.find(BasicDBObject.parse("{str: \"FOO\"}")).setCollation(CASE_INSENSITIVE_COLLATION);

        assertEquals(1, dbCursor.count());
        assertEquals(document, dbCursor.one());

        DBCursor iter = collection.find(BasicDBObject.parse("{str: \"FOO\"}")).setCollation(CASE_INSENSITIVE_COLLATION);
        assertEquals(document, iter.next());
    }

    @SuppressWarnings("unchecked")
    static DBObject getKeyPattern(final DBObject explainPlan) {
        DBObject queryPlanner = (DBObject) explainPlan.get("queryPlanner");
        DBObject winningPlan = (DBObject) queryPlanner.get("winningPlan");

        // Try queryPlan.inputStage first
        DBObject queryPlan = (DBObject) winningPlan.get("queryPlan");
        if (queryPlan != null) {
            DBObject inputStage = (DBObject) queryPlan.get("inputStage");
            if (inputStage != null) {
                return (DBObject) inputStage.get("keyPattern");
            }
        }

        // Try winningPlan.inputStage
        DBObject inputStage = (DBObject) winningPlan.get("inputStage");
        if (inputStage != null) {
            return (DBObject) inputStage.get("keyPattern");
        }

        // Try shards
        List<DBObject> shards = (List<DBObject>) winningPlan.get("shards");
        if (shards != null) {
            DBObject shardWinningPlan = (DBObject) shards.get(0).get("winningPlan");
            DBObject shardQueryPlan = (DBObject) shardWinningPlan.get("queryPlan");
            if (shardQueryPlan != null) {
                DBObject shardInputStage = (DBObject) shardQueryPlan.get("inputStage");
                if (shardInputStage != null) {
                    return (DBObject) shardInputStage.get("keyPattern");
                }
            }
            DBObject shardInputStage = (DBObject) shardWinningPlan.get("inputStage");
            if (shardInputStage != null) {
                return (DBObject) shardInputStage.get("keyPattern");
            }
        }

        return null;
    }
}
