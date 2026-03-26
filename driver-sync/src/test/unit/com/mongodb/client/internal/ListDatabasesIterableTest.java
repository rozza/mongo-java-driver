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
import com.mongodb.internal.operation.ListDatabasesOperation;
import org.bson.BsonDocument;
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

import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.CustomMatchers.isTheSameAs;
import static com.mongodb.ReadPreference.secondary;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ListDatabasesIterableTest {

    private final CodecRegistry codecRegistry = fromProviders(Arrays.asList(new ValueCodecProvider(),
            new DocumentCodecProvider(), new BsonValueCodecProvider()));

    @Test
    @DisplayName("should build the expected listDatabasesOperation")
    void shouldBuildExpectedListDatabasesOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null, null));
        ListDatabasesIterableImpl<Document> listDatabaseIterable = new ListDatabasesIterableImpl<>(null, Document.class,
                codecRegistry, secondary(), executor, true, TIMEOUT_SETTINGS);

        // default input
        listDatabaseIterable.iterator();
        ListDatabasesOperation<?> operation = (ListDatabasesOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new ListDatabasesOperation<>(new DocumentCodec()).retryReads(true)));
        assertEquals(secondary(), executor.getReadPreference());

        // overriding
        listDatabaseIterable.maxTime(100, MILLISECONDS).filter(Document.parse("{a: 1}")).nameOnly(true).iterator();
        operation = (ListDatabasesOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new ListDatabasesOperation<>(new DocumentCodec())
                .filter(BsonDocument.parse("{a: 1}")).nameOnly(true).retryReads(true)));

        // authorizedDatabasesOnly
        listDatabaseIterable.filter(Document.parse("{a: 1}")).authorizedDatabasesOnly(true).iterator();
        operation = (ListDatabasesOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new ListDatabasesOperation<>(new DocumentCodec())
                .filter(BsonDocument.parse("{a: 1}")).nameOnly(true).authorizedDatabasesOnly(true).retryReads(true)));
    }

    @Test
    @DisplayName("should follow the MongoIterable interface as expected")
    void shouldFollowMongoIterableInterface() {
        List<Document> cannedResults = Arrays.asList(new Document("_id", 1), new Document("_id", 2), new Document("_id", 3));

        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(
                createBatchCursor(cannedResults), createBatchCursor(cannedResults),
                createBatchCursor(cannedResults), createBatchCursor(cannedResults)));
        ListDatabasesIterableImpl<Document> mongoIterable = new ListDatabasesIterableImpl<>(null, Document.class,
                codecRegistry, secondary(), executor, true, TIMEOUT_SETTINGS);

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
        ListDatabasesIterableImpl<Document> mongoIterable = new ListDatabasesIterableImpl<>(null, Document.class,
                codecRegistry, secondary(), mock(OperationExecutor.class), true, TIMEOUT_SETTINGS);

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
