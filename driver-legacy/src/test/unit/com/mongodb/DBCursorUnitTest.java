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
import com.mongodb.client.model.DBCollectionFindOptions;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.CountOperation;
import com.mongodb.internal.operation.FindOperation;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.CustomMatchers.isTheSameAs;
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DBCursorUnitTest {

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

    @Test
    @DisplayName("should get and set read preference")
    void shouldGetAndSetReadPreference() {
        DBCollection collection = new DB(createMongoClient(), "myDatabase", new TestOperationExecutor(new ArrayList<>()))
                .getCollection("test");
        collection.setReadPreference(ReadPreference.nearest());
        DBCursor cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.nearest());

        assertEquals(ReadPreference.nearest(), cursor.getReadPreference());

        cursor.setReadPreference(ReadPreference.secondary());
        assertEquals(ReadPreference.secondary(), cursor.getReadPreference());

        cursor.setReadPreference(null);
        assertEquals(ReadPreference.nearest(), cursor.getReadPreference());
    }

    @Test
    @DisplayName("should get and set read concern")
    void shouldGetAndSetReadConcern() {
        DBCollection collection = new DB(createMongoClient(), "myDatabase", new TestOperationExecutor(new ArrayList<>()))
                .getCollection("test");
        collection.setReadConcern(ReadConcern.MAJORITY);
        DBCursor cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary());

        assertEquals(ReadConcern.MAJORITY, cursor.getReadConcern());

        cursor.setReadConcern(ReadConcern.LOCAL);
        assertEquals(ReadConcern.LOCAL, cursor.getReadConcern());

        cursor.setReadConcern(null);
        assertEquals(ReadConcern.MAJORITY, cursor.getReadConcern());
    }

    @Test
    @DisplayName("should get and set collation")
    void shouldGetAndSetCollation() {
        DBCollection collection = new DB(createMongoClient(), "myDatabase", new TestOperationExecutor(new ArrayList<>()))
                .getCollection("test");
        Collation collation = Collation.builder().locale("en").build();
        DBCursor cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary());

        assertNull(cursor.getCollation());

        cursor.setCollation(collation);
        assertEquals(collation, cursor.getCollation());

        cursor.setCollation(null);
        assertNull(cursor.getCollation());
    }

    @Test
    @DisplayName("should copy as expected")
    void shouldCopyAsExpected() {
        DBCollection collection = new DB(createMongoClient(), "myDatabase", new TestOperationExecutor(new ArrayList<>()))
                .getCollection("test");
        DBCursor cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.nearest())
                .setReadConcern(ReadConcern.LOCAL)
                .setCollation(Collation.builder().locale("en").build());

        assertThat(cursor.copy(), isTheSameAs(cursor));
    }

    @Test
    @DisplayName("find should create the correct FindOperation")
    void findShouldCreateTheCorrectFindOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.singletonList(stubBatchCursor()));
        DBCollection collection = new DB(createMongoClient(), "myDatabase", executor).getCollection("test");
        DBCursor cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary());
        cursor.setReadConcern(ReadConcern.MAJORITY);

        cursor.toArray();

        assertThat(executor.getReadOperation(), isTheSameAs(new FindOperation<>(collection.getNamespace(),
                collection.getObjectCodec())
                .filter(new BsonDocument())
                .projection(new BsonDocument())
                .retryReads(true)));
    }

    @Test
    @DisplayName("one should create the correct FindOperation")
    void oneShouldCreateTheCorrectFindOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.singletonList(stubBatchCursor()));
        DBCollection collection = new DB(createMongoClient(), "myDatabase", executor).getCollection("test");
        DBCursor cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary());
        cursor.setReadConcern(ReadConcern.MAJORITY);

        cursor.one();

        assertThat(executor.getReadOperation(), isTheSameAs(
                new FindOperation<>(collection.getNamespace(), collection.getObjectCodec())
                        .limit(-1)
                        .filter(new BsonDocument())
                        .projection(new BsonDocument())
                        .retryReads(true)));
    }

    @Test
    @DisplayName("DBCursor methods should be used to create the expected operation")
    void dbCursorMethodsShouldBeUsedToCreateTheExpectedOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.singletonList(stubBatchCursor()));
        DBCollection collection = new DB(createMongoClient(), "myDatabase", executor).getCollection("test");
        Collation collation = Collation.builder().locale("en").build();
        CursorType cursorType = CursorType.NonTailable;
        BasicDBObject filter = new BasicDBObject();
        BasicDBObject sort = BasicDBObject.parse("{a: 1}");
        BsonDocument bsonFilter = new BsonDocument();
        BsonDocument bsonSort = BsonDocument.parse(sort.toJson());
        ReadConcern readConcern = ReadConcern.LOCAL;
        ReadPreference readPreference = ReadPreference.nearest();
        DBCollectionFindOptions findOptions = new DBCollectionFindOptions();
        DBCursor cursor = new DBCursor(collection, filter, findOptions)
                .setReadConcern(readConcern)
                .setReadPreference(readPreference)
                .setCollation(collation)
                .batchSize(1)
                .cursorType(cursorType)
                .limit(1)
                .maxTime(100, TimeUnit.MILLISECONDS)
                .noCursorTimeout(true)
                .partial(true)
                .skip(1)
                .sort(sort);

        cursor.toArray();

        assertThat(executor.getReadOperation(), isTheSameAs(
                new FindOperation<>(collection.getNamespace(), collection.getObjectCodec())
                        .batchSize(1)
                        .collation(collation)
                        .cursorType(cursorType)
                        .filter(bsonFilter)
                        .limit(1)
                        .noCursorTimeout(true)
                        .partial(true)
                        .skip(1)
                        .sort(bsonSort)
                        .retryReads(true)));

        assertEquals(readPreference, executor.getReadPreference());
        assertEquals(readConcern, executor.getReadConcern());
    }

    @Test
    @DisplayName("DBCollectionFindOptions should be used to create the expected operation")
    void dbCollectionFindOptionsShouldBeUsedToCreateTheExpectedOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.singletonList(stubBatchCursor()));
        DBCollection collection = new DB(createMongoClient(), "myDatabase", executor).getCollection("test");
        Collation collation = Collation.builder().locale("en").build();
        CursorType cursorType = CursorType.NonTailable;
        BasicDBObject filter = new BasicDBObject();
        BasicDBObject projection = BasicDBObject.parse("{a: 1, _id: 0}");
        BasicDBObject sort = BasicDBObject.parse("{a: 1}");
        BsonDocument bsonFilter = new BsonDocument();
        BsonDocument bsonProjection = BsonDocument.parse(projection.toJson());
        BsonDocument bsonSort = BsonDocument.parse(sort.toJson());
        String comment = "comment";
        BasicDBObject hint = BasicDBObject.parse("{x : 1}");
        BasicDBObject min = BasicDBObject.parse("{y : 1}");
        BasicDBObject max = BasicDBObject.parse("{y : 100}");
        BsonDocument bsonHint = BsonDocument.parse(hint.toJson());
        BsonDocument bsonMin = BsonDocument.parse(min.toJson());
        BsonDocument bsonMax = BsonDocument.parse(max.toJson());
        ReadConcern readConcern = ReadConcern.LOCAL;
        ReadPreference readPreference = ReadPreference.nearest();
        DBCollectionFindOptions findOptions = new DBCollectionFindOptions()
                .batchSize(1)
                .collation(collation)
                .cursorType(cursorType)
                .limit(1)
                .maxAwaitTime(1001, TimeUnit.MILLISECONDS)
                .maxTime(101, TimeUnit.MILLISECONDS)
                .noCursorTimeout(true)
                .partial(true)
                .projection(projection)
                .readConcern(readConcern)
                .readPreference(readPreference)
                .skip(1)
                .sort(sort)
                .comment(comment)
                .hint(hint)
                .max(max)
                .min(min)
                .returnKey(true)
                .showRecordId(true);

        DBCursor cursor = new DBCursor(collection, filter, findOptions);

        cursor.toArray();

        assertThat(executor.getReadOperation(), isTheSameAs(new FindOperation<>(collection.getNamespace(), collection.getObjectCodec())
                .batchSize(1)
                .collation(collation)
                .cursorType(cursorType)
                .filter(bsonFilter)
                .limit(1)
                .noCursorTimeout(true)
                .partial(true)
                .projection(bsonProjection)
                .skip(1)
                .sort(bsonSort)
                .comment(new BsonString(comment))
                .hint(bsonHint)
                .max(bsonMax)
                .min(bsonMin)
                .returnKey(true)
                .showRecordId(true)
                .retryReads(true)));

        assertEquals(findOptions.getReadPreference(), executor.getReadPreference());
    }

    @Test
    @DisplayName("count should create the correct CountOperation")
    void countShouldCreateTheCorrectCountOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.singletonList(42L));
        DBCollection collection = new DB(createMongoClient(), "myDatabase", executor).getCollection("test");
        DBCursor cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary());
        cursor.setReadConcern(ReadConcern.MAJORITY);

        int result = cursor.count();

        assertEquals(42, result);
        assertThat(executor.getReadOperation(), isTheSameAs(new CountOperation(collection.getNamespace())
                .filter(new BsonDocument()).retryReads(true)));
        assertEquals(ReadConcern.MAJORITY, executor.getReadConcern());
    }

    @Test
    @DisplayName("size should create the correct CountOperation")
    void sizeShouldCreateTheCorrectCountOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.singletonList(42L));
        DBCollection collection = new DB(createMongoClient(), "myDatabase", executor).getCollection("test");
        DBCursor cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary());
        cursor.setReadConcern(ReadConcern.MAJORITY);

        int result = cursor.size();

        assertEquals(42, result);
        assertThat(executor.getReadOperation(), isTheSameAs(new CountOperation(collection.getNamespace())
                .filter(new BsonDocument()).retryReads(true)));
        assertEquals(ReadConcern.MAJORITY, executor.getReadConcern());
    }

    @SuppressWarnings("unchecked")
    private static BatchCursor<DBObject> stubBatchCursor() {
        BatchCursor<DBObject> cursor = mock(BatchCursor.class);
        AtomicInteger count = new AtomicInteger(0);
        when(cursor.next()).thenAnswer(invocation -> {
            count.incrementAndGet();
            return new ArrayList<>(Collections.singletonList(new BasicDBObject("_id", 1)));
        });
        when(cursor.hasNext()).thenAnswer(invocation -> count.get() == 0);
        when(cursor.getServerCursor()).thenReturn(new ServerCursor(12L, new ServerAddress()));
        return cursor;
    }
}
