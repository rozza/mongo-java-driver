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

import com.mongodb.CursorType;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.FindOperation;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.CustomMatchers.isTheSameAs;
import static com.mongodb.ReadPreference.secondary;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FindIterableTest {

    private final CodecRegistry codecRegistry = fromProviders(Arrays.asList(new ValueCodecProvider(),
            new DocumentCodecProvider(), new BsonValueCodecProvider()));
    private final MongoNamespace namespace = new MongoNamespace("db", "coll");
    private final ReadConcern readConcern = ReadConcern.MAJORITY;
    private final Collation collation = Collation.builder().locale("en").build();

    @Test
    @DisplayName("should build the expected findOperation")
    void shouldBuildExpectedFindOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null, null));
        FindIterableImpl<Document, Document> findIterable = new FindIterableImpl<>(null, namespace, Document.class,
                Document.class, codecRegistry, secondary(), readConcern, executor, new Document("filter", 1), true, TIMEOUT_SETTINGS);
        findIterable.sort(new Document("sort", 1))
                .projection(new Document("projection", 1))
                .batchSize(100).limit(100).skip(10)
                .cursorType(CursorType.NonTailable)
                .noCursorTimeout(false).partial(false).collation(null)
                .comment(new BsonString("my comment")).hintString("a_1")
                .min(new Document("min", 1)).max(new Document("max", 1))
                .returnKey(false).showRecordId(false).allowDiskUse(false);

        // default
        findIterable.iterator();
        FindOperation<?> operation = (FindOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new FindOperation<>(namespace, new DocumentCodec())
                .filter(new BsonDocument("filter", new BsonInt32(1)))
                .sort(new BsonDocument("sort", new BsonInt32(1)))
                .projection(new BsonDocument("projection", new BsonInt32(1)))
                .batchSize(100).limit(100).skip(10)
                .cursorType(CursorType.NonTailable)
                .comment(new BsonString("my comment")).hint(new BsonString("a_1"))
                .min(new BsonDocument("min", new BsonInt32(1)))
                .max(new BsonDocument("max", new BsonInt32(1)))
                .returnKey(false).showRecordId(false).allowDiskUse(false).retryReads(true)));
        assertEquals(secondary(), executor.getReadPreference());

        // overriding
        findIterable.filter(new Document("filter", 2)).sort(new Document("sort", 2))
                .projection(new Document("projection", 2))
                .maxTime(101, MILLISECONDS).maxAwaitTime(1001, MILLISECONDS)
                .batchSize(99).limit(99).skip(9)
                .cursorType(CursorType.Tailable).noCursorTimeout(true).partial(true)
                .collation(collation).comment("alt comment").hint(new Document("hint", 2))
                .min(new Document("min", 2)).max(new Document("max", 2))
                .returnKey(true).showRecordId(true).allowDiskUse(true).iterator();

        operation = (FindOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new FindOperation<>(namespace, new DocumentCodec())
                .filter(new BsonDocument("filter", new BsonInt32(2)))
                .sort(new BsonDocument("sort", new BsonInt32(2)))
                .projection(new BsonDocument("projection", new BsonInt32(2)))
                .batchSize(99).limit(99).skip(9)
                .cursorType(CursorType.Tailable).noCursorTimeout(true).partial(true)
                .collation(collation).comment(new BsonString("alt comment"))
                .hint(new BsonDocument("hint", new BsonInt32(2)))
                .min(new BsonDocument("min", new BsonInt32(2)))
                .max(new BsonDocument("max", new BsonInt32(2)))
                .returnKey(true).showRecordId(true).allowDiskUse(true).retryReads(true)));

        // passing nulls
        new FindIterableImpl<>(null, namespace, Document.class, Document.class, codecRegistry, secondary(), readConcern,
                executor, new Document("filter", 1), true, TIMEOUT_SETTINGS)
                .filter((Bson) null).collation(null).projection(null).sort((Bson) null)
                .comment((BsonString) null).hint(null).max((Bson) null).min((Bson) null).iterator();
        operation = (FindOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new FindOperation<>(namespace, new DocumentCodec())
                .filter(new BsonDocument()).retryReads(true)));
    }

    @Test
    @DisplayName("should use ClientSession")
    void shouldUseClientSession() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            @SuppressWarnings("unchecked")
            BatchCursor<Document> batchCursor = mock(BatchCursor.class);
            when(batchCursor.hasNext()).thenReturn(false);
            TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(batchCursor, batchCursor));
            FindIterableImpl<Document, Document> findIterable = new FindIterableImpl<>(session, namespace,
                    Document.class, Document.class, codecRegistry, secondary(), readConcern, executor,
                    new Document("filter", 1), true, TIMEOUT_SETTINGS);

            findIterable.first();
            assertEquals(session, executor.getClientSession());

            findIterable.iterator();
            assertEquals(session, executor.getClientSession());
        }
    }

    @Test
    @DisplayName("should handle mixed types")
    void shouldHandleMixedTypes() {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null));
        FindIterableImpl<Document, Document> findIterable = new FindIterableImpl<>(null, namespace,
                Document.class, Document.class, codecRegistry, secondary(), readConcern, executor,
                new Document("filter", 1), true, TIMEOUT_SETTINGS);

        findIterable.filter(new Document("filter", 1))
                .sort(new BsonDocument("sort", new BsonInt32(1))).iterator();

        FindOperation<?> operation = (FindOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new FindOperation<>(namespace, new DocumentCodec())
                .filter(new BsonDocument("filter", new BsonInt32(1)))
                .sort(new BsonDocument("sort", new BsonInt32(1)))
                .cursorType(CursorType.NonTailable).retryReads(true)));
    }

    @Test
    @DisplayName("should follow the MongoIterable interface as expected")
    void shouldFollowMongoIterableInterface() {
        List<Document> cannedResults = Arrays.asList(new Document("_id", 1), new Document("_id", 2), new Document("_id", 3));
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(
                createBatchCursor(cannedResults), createBatchCursor(cannedResults),
                createBatchCursor(cannedResults), createBatchCursor(cannedResults)));
        FindIterableImpl<Document, Document> mongoIterable = new FindIterableImpl<>(null, namespace,
                Document.class, Document.class, codecRegistry, secondary(), readConcern, executor,
                new Document(), true, TIMEOUT_SETTINGS);

        assertEquals(cannedResults.get(0), mongoIterable.first());

        final int[] count = {0};
        mongoIterable.forEach(document -> count[0]++);
        assertEquals(3, count[0]);

        List<Document> target = new ArrayList<>();
        mongoIterable.into(target);
        assertEquals(cannedResults, target);

        List<Integer> mapped = new ArrayList<>();
        mongoIterable.map(document -> document.getInteger("_id")).into(mapped);
        assertEquals(Arrays.asList(1, 2, 3), mapped);
    }

    @Test
    @DisplayName("should get and set batchSize as expected")
    void shouldGetAndSetBatchSize() {
        FindIterableImpl<Document, Document> mongoIterable = new FindIterableImpl<>(null, namespace,
                Document.class, Document.class, codecRegistry, secondary(), readConcern,
                mock(OperationExecutor.class), new Document(), true, TIMEOUT_SETTINGS);

        assertNull(mongoIterable.getBatchSize());
        mongoIterable.batchSize(5);
        assertEquals(Integer.valueOf(5), mongoIterable.getBatchSize());
    }

    @Test
    @DisplayName("forEach should close cursor when there is an exception during iteration")
    void forEachShouldCloseCursorOnException() {
        @SuppressWarnings("unchecked")
        BatchCursor<Document> cursor = mock(BatchCursor.class);
        when(cursor.hasNext()).thenThrow(new MongoException(""));
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(cursor));
        FindIterableImpl<Document, Document> mongoIterable = new FindIterableImpl<>(null, namespace,
                Document.class, Document.class, codecRegistry, secondary(), readConcern, executor,
                new Document(), true, TIMEOUT_SETTINGS);

        assertThrows(MongoException.class, () -> mongoIterable.forEach(document -> { }));
        verify(cursor).close();
    }

    @SuppressWarnings("unchecked")
    private BatchCursor<Document> createBatchCursor(final List<Document> results) {
        BatchCursor<Document> batchCursor = mock(BatchCursor.class);
        when(batchCursor.hasNext()).thenReturn(true, true, false);
        when(batchCursor.next()).thenReturn(new ArrayList<>(results));
        return batchCursor;
    }
}
