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

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.DistinctOperation;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecConfigurationException;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DistinctIterableTest {

    private final MongoNamespace namespace = new MongoNamespace("db", "coll");
    private final CodecRegistry codecRegistry = fromProviders(Arrays.asList(new ValueCodecProvider(),
            new DocumentCodecProvider(), new BsonValueCodecProvider()));
    private final ReadConcern readConcern = ReadConcern.MAJORITY;
    private final Collation collation = Collation.builder().locale("en").build();

    @Test
    @DisplayName("should build the expected DistinctOperation")
    void shouldBuildExpectedDistinctOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null));
        DistinctIterableImpl<Document, Document> distinctIterable = new DistinctIterableImpl<>(null, namespace,
                Document.class, Document.class, codecRegistry, secondary(), readConcern, executor, "field",
                new BsonDocument(), true, TIMEOUT_SETTINGS);

        // default
        distinctIterable.iterator();
        DistinctOperation<?> operation = (DistinctOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new DistinctOperation<>(namespace, "field", new DocumentCodec())
                .filter(new BsonDocument()).retryReads(true)));
        assertEquals(secondary(), executor.getReadPreference());

        // overriding
        distinctIterable.filter(new Document("field", 1)).maxTime(100, MILLISECONDS).batchSize(99).collation(collation).iterator();
        operation = (DistinctOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new DistinctOperation<>(namespace, "field", new DocumentCodec())
                .filter(new BsonDocument("field", new BsonInt32(1))).collation(collation).retryReads(true)));
    }

    @Test
    @DisplayName("should use ClientSession")
    void shouldUseClientSession() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            @SuppressWarnings("unchecked")
            BatchCursor<Document> batchCursor = mock(BatchCursor.class);
            when(batchCursor.hasNext()).thenReturn(false);
            TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(batchCursor, batchCursor));
            DistinctIterableImpl<Document, Document> distinctIterable = new DistinctIterableImpl<>(session, namespace,
                    Document.class, Document.class, codecRegistry, secondary(), readConcern, executor, "field",
                    new BsonDocument(), true, TIMEOUT_SETTINGS);

            distinctIterable.first();
            assertEquals(session, executor.getClientSession());

            distinctIterable.iterator();
            assertEquals(session, executor.getClientSession());
        }
    }

    @Test
    @DisplayName("should handle exceptions correctly")
    void shouldHandleExceptions() {
        CodecRegistry altRegistry = fromProviders(Arrays.asList(new ValueCodecProvider(), new BsonValueCodecProvider()));
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(new MongoException("failure")));
        DistinctIterableImpl<Document, BsonDocument> distinctIterable = new DistinctIterableImpl<>(null, namespace,
                Document.class, BsonDocument.class, altRegistry, secondary(), readConcern, executor, "field",
                new BsonDocument(), true, TIMEOUT_SETTINGS);

        assertThrows(MongoException.class, () -> distinctIterable.iterator());
        assertThrows(CodecConfigurationException.class, () -> distinctIterable.filter(new Document("field", 1)).iterator());
    }

    @Test
    @DisplayName("should follow the MongoIterable interface as expected")
    void shouldFollowMongoIterableInterface() {
        List<Document> cannedResults = Arrays.asList(new Document("_id", 1), new Document("_id", 2), new Document("_id", 3));
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(
                createBatchCursor(cannedResults), createBatchCursor(cannedResults),
                createBatchCursor(cannedResults), createBatchCursor(cannedResults)));
        DistinctIterableImpl<Document, Document> mongoIterable = new DistinctIterableImpl<>(null, namespace,
                Document.class, Document.class, codecRegistry, secondary(), ReadConcern.LOCAL, executor, "field",
                new BsonDocument(), true, TIMEOUT_SETTINGS);

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
        DistinctIterableImpl<Document, Document> mongoIterable = new DistinctIterableImpl<>(null, namespace,
                Document.class, Document.class, codecRegistry, secondary(), readConcern, mock(OperationExecutor.class),
                "field", new BsonDocument(), true, TIMEOUT_SETTINGS);

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
