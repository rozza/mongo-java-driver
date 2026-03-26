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
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.operation.AggregateOperation;
import com.mongodb.internal.operation.AggregateToCollectionOperation;
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
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

class AggregateIterableTest {

    private final MongoNamespace namespace = new MongoNamespace("db", "coll");
    private final CodecRegistry codecRegistry = fromProviders(Arrays.asList(new ValueCodecProvider(),
            new DocumentCodecProvider(), new BsonValueCodecProvider()));
    private final ReadConcern readConcern = ReadConcern.MAJORITY;
    private final WriteConcern writeConcern = WriteConcern.MAJORITY;
    private final Collation collation = Collation.builder().locale("en").build();

    @Test
    @DisplayName("should build the expected AggregationOperation")
    void shouldBuildExpectedAggregationOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null, null, null, null));
        List<Document> pipeline = Arrays.asList(new Document("$match", 1));
        AggregateIterableImpl<Document, Document> aggregationIterable = new AggregateIterableImpl<>(null, namespace,
                Document.class, Document.class, codecRegistry, secondary(), readConcern, writeConcern, executor,
                pipeline, AggregationLevel.COLLECTION, true, TIMEOUT_SETTINGS);

        // default
        aggregationIterable.iterator();
        AggregateOperation<?> operation = (AggregateOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new AggregateOperation<>(namespace,
                Arrays.asList(new BsonDocument("$match", new BsonInt32(1))), new DocumentCodec()).retryReads(true)));
        assertEquals(secondary(), executor.getReadPreference());

        // overriding
        aggregationIterable.maxAwaitTime(1001, MILLISECONDS).maxTime(101, MILLISECONDS)
                .collation(collation).hint(new Document("a", 1)).comment("this is a comment").iterator();
        operation = (AggregateOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new AggregateOperation<>(namespace,
                Arrays.asList(new BsonDocument("$match", new BsonInt32(1))), new DocumentCodec())
                .retryReads(true).collation(collation)
                .hint(new BsonDocument("a", new BsonInt32(1)))
                .comment(new BsonString("this is a comment"))));

        // both hint and hint string
        AggregateIterableImpl<Document, Document> aggregationIterable2 = new AggregateIterableImpl<>(null, namespace,
                Document.class, Document.class, codecRegistry, secondary(), readConcern, writeConcern, executor,
                pipeline, AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS);
        aggregationIterable2.hint(new Document("a", 1)).hintString("a_1").iterator();
        operation = (AggregateOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new AggregateOperation<>(namespace,
                Arrays.asList(new BsonDocument("$match", new BsonInt32(1))), new DocumentCodec())
                .hint(new BsonDocument("a", new BsonInt32(1)))));
    }

    @Test
    @DisplayName("should build the expected AggregateToCollectionOperation for $out")
    void shouldBuildExpectedAggregateToCollectionOperationForOut() {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null, null, null, null));
        String collectionName = "collectionName";
        MongoNamespace collectionNamespace = new MongoNamespace(namespace.getDatabaseName(), collectionName);
        List<Document> pipeline = Arrays.asList(new Document("$match", 1), new Document("$out", collectionName));

        new AggregateIterableImpl<>(null, namespace, Document.class, Document.class, codecRegistry, secondary(),
                readConcern, writeConcern, executor, pipeline, AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS)
                .batchSize(99).allowDiskUse(true).collation(collation).hint(new Document("a", 1))
                .comment(new BsonString("this is a comment")).iterator();

        AggregateToCollectionOperation aggToCollOp = (AggregateToCollectionOperation) executor.getReadOperation();
        assertThat(aggToCollOp, isTheSameAs(new AggregateToCollectionOperation(namespace,
                Arrays.asList(new BsonDocument("$match", new BsonInt32(1)),
                        new BsonDocument("$out", new BsonString(collectionName))),
                readConcern, writeConcern, AggregationLevel.COLLECTION)
                .allowDiskUse(true).collation(collation)
                .hint(new BsonDocument("a", new BsonInt32(1)))
                .comment(new BsonString("this is a comment"))));

        FindOperation<?> findOp = (FindOperation<?>) executor.getReadOperation();
        assertEquals(collectionNamespace, findOp.getNamespace());
        assertEquals(99, findOp.getBatchSize());
        assertEquals(collation, findOp.getCollation());
    }

    @Test
    @DisplayName("should use ClientSession for AggregationOperation")
    void shouldUseClientSessionForAggregation() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            @SuppressWarnings("unchecked")
            BatchCursor<Document> batchCursor = mock(BatchCursor.class);
            when(batchCursor.hasNext()).thenReturn(false);
            TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(batchCursor, batchCursor));
            AggregateIterableImpl<Document, Document> aggregationIterable = new AggregateIterableImpl<>(session, namespace,
                    Document.class, Document.class, codecRegistry, secondary(), readConcern, writeConcern, executor,
                    Arrays.asList(new Document("$match", 1)), AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS);

            aggregationIterable.first();
            assertEquals(session, executor.getClientSession());

            aggregationIterable.iterator();
            assertEquals(session, executor.getClientSession());
        }
    }

    @Test
    @DisplayName("should handle exceptions correctly")
    void shouldHandleExceptions() {
        CodecRegistry altRegistry = fromProviders(Arrays.asList(new ValueCodecProvider(), new BsonValueCodecProvider()));
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(new MongoException("failure")));
        List<BsonDocument> pipeline = Arrays.asList(new BsonDocument("$match", new BsonInt32(1)));
        AggregateIterableImpl<BsonDocument, BsonDocument> aggregationIterable = new AggregateIterableImpl<>(null, namespace,
                BsonDocument.class, BsonDocument.class, altRegistry, secondary(), readConcern, writeConcern, executor,
                pipeline, AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS);

        assertThrows(MongoException.class, () -> aggregationIterable.iterator());
        assertThrows(IllegalStateException.class, () -> aggregationIterable.toCollection());
        assertThrows(CodecConfigurationException.class, () ->
                new AggregateIterableImpl<>(null, namespace, Document.class, Document.class, altRegistry, secondary(),
                        readConcern, writeConcern, executor, pipeline, AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS).iterator());
        assertThrows(IllegalArgumentException.class, () ->
                new AggregateIterableImpl<>(null, namespace, Document.class, Document.class, altRegistry, secondary(),
                        readConcern, writeConcern, executor, Arrays.asList((Document) null), AggregationLevel.COLLECTION,
                        false, TIMEOUT_SETTINGS).iterator());
    }

    @Test
    @DisplayName("should follow the MongoIterable interface as expected")
    void shouldFollowMongoIterableInterface() {
        List<Document> cannedResults = Arrays.asList(new Document("_id", 1), new Document("_id", 2), new Document("_id", 3));
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(
                createBatchCursor(cannedResults), createBatchCursor(cannedResults),
                createBatchCursor(cannedResults), createBatchCursor(cannedResults)));
        AggregateIterableImpl<Document, Document> mongoIterable = new AggregateIterableImpl<>(null, namespace,
                Document.class, Document.class, codecRegistry, secondary(), readConcern, writeConcern, executor,
                Arrays.asList(new Document("$match", 1)), AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS);

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
        AggregateIterableImpl<Document, Document> mongoIterable = new AggregateIterableImpl<>(null, namespace,
                Document.class, Document.class, codecRegistry, secondary(), readConcern, writeConcern,
                mock(OperationExecutor.class), Arrays.asList(new Document("$match", 1)), AggregationLevel.COLLECTION,
                false, TIMEOUT_SETTINGS);

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
