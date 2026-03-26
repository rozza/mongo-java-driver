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

import com.mongodb.client.ClientSession;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.ListCollectionsOperation;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.CustomMatchers.isTheSameAs;
import static com.mongodb.ReadPreference.secondary;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ListCollectionsIterableTest {

    private final CodecRegistry codecRegistry = fromProviders(Arrays.asList(new ValueCodecProvider(),
            new DocumentCodecProvider(), new BsonValueCodecProvider()));

    @Test
    @DisplayName("should build the expected listCollectionOperation")
    void shouldBuildExpectedListCollectionOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null, null, null));
        ListCollectionsIterableImpl<Document> listCollectionIterable = new ListCollectionsIterableImpl<>(null, "db",
                false, Document.class, codecRegistry, secondary(), executor, true, TIMEOUT_SETTINGS);
        listCollectionIterable.filter(new Document("filter", 1)).batchSize(100);

        ListCollectionsIterableImpl<Document> listCollectionNamesIterable = new ListCollectionsIterableImpl<>(null, "db",
                true, Document.class, codecRegistry, secondary(), executor, true, TIMEOUT_SETTINGS);

        // default input
        listCollectionIterable.iterator();
        ListCollectionsOperation<?> operation = (ListCollectionsOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new ListCollectionsOperation<>("db", new DocumentCodec())
                .filter(new BsonDocument("filter", new BsonInt32(1))).batchSize(100).retryReads(true).authorizedCollections(false)));
        assertEquals(secondary(), executor.getReadPreference());

        // overriding
        listCollectionIterable.filter(new Document("filter", 2)).batchSize(99).maxTime(100, TimeUnit.MILLISECONDS).iterator();
        operation = (ListCollectionsOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new ListCollectionsOperation<>("db", new DocumentCodec())
                .filter(new BsonDocument("filter", new BsonInt32(2))).batchSize(99).retryReads(true)));

        // nameOnly
        listCollectionNamesIterable.iterator();
        operation = (ListCollectionsOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new ListCollectionsOperation<>("db", new DocumentCodec()).nameOnly(true)
                .retryReads(true)));

        // authorizedCollections
        listCollectionNamesIterable.authorizedCollections(true).iterator();
        operation = (ListCollectionsOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new ListCollectionsOperation<>("db", new DocumentCodec())
                .authorizedCollections(true).nameOnly(true).retryReads(true)));
    }

    @Test
    @DisplayName("should use ClientSession")
    void shouldUseClientSessionWithNull() {
        @SuppressWarnings("unchecked")
        BatchCursor<Document> batchCursor = mock(BatchCursor.class);
        when(batchCursor.hasNext()).thenReturn(false);
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(batchCursor, batchCursor));
        ListCollectionsIterableImpl<Document> listCollectionIterable = new ListCollectionsIterableImpl<>(null, "db",
                false, Document.class, codecRegistry, secondary(), executor, true, TIMEOUT_SETTINGS);

        listCollectionIterable.first();
        assertNull(executor.getClientSession());

        listCollectionIterable.iterator();
        assertNull(executor.getClientSession());
    }

    @Test
    @DisplayName("should use ClientSession with session")
    void shouldUseClientSessionWithSession() {
        @SuppressWarnings("unchecked")
        BatchCursor<Document> batchCursor = mock(BatchCursor.class);
        when(batchCursor.hasNext()).thenReturn(false);
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(batchCursor, batchCursor));
        ClientSession session = mock(ClientSession.class);
        ListCollectionsIterableImpl<Document> listCollectionIterable = new ListCollectionsIterableImpl<>(session, "db",
                false, Document.class, codecRegistry, secondary(), executor, true, TIMEOUT_SETTINGS);

        listCollectionIterable.first();
        assertEquals(session, executor.getClientSession());

        listCollectionIterable.iterator();
        assertEquals(session, executor.getClientSession());
    }

    @Test
    @DisplayName("should follow the MongoIterable interface as expected")
    void shouldFollowMongoIterableInterface() {
        List<Document> cannedResults = Arrays.asList(new Document("_id", 1), new Document("_id", 2), new Document("_id", 3));
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(
                createBatchCursor(cannedResults), createBatchCursor(cannedResults),
                createBatchCursor(cannedResults), createBatchCursor(cannedResults)));
        ListCollectionsIterableImpl<Document> mongoIterable = new ListCollectionsIterableImpl<>(null, "db",
                false, Document.class, codecRegistry, secondary(), executor, true, TIMEOUT_SETTINGS);

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
        ListCollectionsIterableImpl<Document> mongoIterable = new ListCollectionsIterableImpl<>(null, "db",
                false, Document.class, codecRegistry, secondary(), mock(OperationExecutor.class), true, TIMEOUT_SETTINGS);

        assertNull(mongoIterable.getBatchSize());
        mongoIterable.batchSize(5);
        assertEquals(Integer.valueOf(5), mongoIterable.getBatchSize());
    }

    @SuppressWarnings("unchecked")
    private BatchCursor<Document> createBatchCursor(final List<Document> results) {
        BatchCursor<Document> batchCursor = mock(BatchCursor.class);
        when(batchCursor.hasNext()).thenReturn(true, true, false);
        when(batchCursor.next()).thenReturn(new ArrayList<>(results));
        return batchCursor;
    }
}
