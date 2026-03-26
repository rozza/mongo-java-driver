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
import com.mongodb.client.model.MapReduceAction;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.FindOperation;
import com.mongodb.internal.operation.MapReduceToCollectionOperation;
import com.mongodb.internal.operation.MapReduceWithInlineResultsOperation;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonJavaScript;
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

@SuppressWarnings("deprecation")
class MapReduceIterableTest {

    private final MongoNamespace namespace = new MongoNamespace("db", "coll");
    private final CodecRegistry codecRegistry = fromProviders(Arrays.asList(new ValueCodecProvider(),
            new DocumentCodecProvider(), new BsonValueCodecProvider()));
    private final ReadConcern readConcern = ReadConcern.MAJORITY;
    private final WriteConcern writeConcern = WriteConcern.MAJORITY;
    private final Collation collation = Collation.builder().locale("en").build();

    @Test
    @DisplayName("should build the expected MapReduceWithInlineResultsOperation")
    void shouldBuildExpectedMapReduceWithInlineResultsOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null));
        MapReduceIterableImpl<Document, Document> mapReduceIterable = new MapReduceIterableImpl<>(null, namespace,
                Document.class, Document.class, codecRegistry, secondary(), readConcern, writeConcern, executor,
                "map", "reduce", TIMEOUT_SETTINGS);

        // default
        mapReduceIterable.iterator();
        MapReduceWithInlineResultsOperation<Document> operation =
                (MapReduceWithInlineResultsOperation<Document>) ((MapReduceIterableImpl.WrappedMapReduceReadOperation<Document>) executor.getReadOperation()).getOperation();
        assertThat(operation, isTheSameAs(new MapReduceWithInlineResultsOperation<>(namespace,
                new BsonJavaScript("map"), new BsonJavaScript("reduce"), new DocumentCodec())
                .verbose(true)));
        assertEquals(secondary(), executor.getReadPreference());

        // overriding
        mapReduceIterable.filter(new Document("filter", 1))
                .finalizeFunction("finalize")
                .limit(999)
                .maxTime(100, MILLISECONDS)
                .scope(new Document("scope", 1))
                .sort(new Document("sort", 1))
                .verbose(false)
                .collation(collation)
                .iterator();
        operation = (MapReduceWithInlineResultsOperation<Document>) ((MapReduceIterableImpl.WrappedMapReduceReadOperation<Document>) executor.getReadOperation()).getOperation();
        assertThat(operation, isTheSameAs(new MapReduceWithInlineResultsOperation<>(namespace,
                new BsonJavaScript("map"), new BsonJavaScript("reduce"), new DocumentCodec())
                .filter(new BsonDocument("filter", new BsonInt32(1)))
                .finalizeFunction(new BsonJavaScript("finalize"))
                .limit(999)
                .scope(new BsonDocument("scope", new BsonInt32(1)))
                .sort(new BsonDocument("sort", new BsonInt32(1)))
                .verbose(false)
                .collation(collation)));
    }

    @Test
    @DisplayName("should build the expected MapReduceToCollectionOperation")
    void shouldBuildExpectedMapReduceToCollectionOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null, null));
        MongoNamespace collectionNamespace = new MongoNamespace("dbName", "collName");

        MapReduceIterableImpl<Document, Document> mapReduceIterable = new MapReduceIterableImpl<>(null, namespace,
                Document.class, Document.class, codecRegistry, secondary(), readConcern, writeConcern, executor,
                "map", "reduce", TIMEOUT_SETTINGS);
        mapReduceIterable.collectionName(collectionNamespace.getCollectionName())
                .databaseName(collectionNamespace.getDatabaseName())
                .filter(new Document("filter", 1))
                .finalizeFunction("finalize")
                .limit(999)
                .maxTime(100, MILLISECONDS)
                .scope(new Document("scope", 1))
                .sort(new Document("sort", 1))
                .verbose(false)
                .batchSize(99)
                .action(MapReduceAction.MERGE)
                .jsMode(true)
                .bypassDocumentValidation(true)
                .collation(collation);
        mapReduceIterable.iterator();

        MapReduceToCollectionOperation writeOp = (MapReduceToCollectionOperation) executor.getWriteOperation();
        MapReduceToCollectionOperation expectedOperation = new MapReduceToCollectionOperation(namespace,
                new BsonJavaScript("map"), new BsonJavaScript("reduce"), "collName", writeConcern)
                .databaseName(collectionNamespace.getDatabaseName())
                .filter(new BsonDocument("filter", new BsonInt32(1)))
                .finalizeFunction(new BsonJavaScript("finalize"))
                .limit(999)
                .scope(new BsonDocument("scope", new BsonInt32(1)))
                .sort(new BsonDocument("sort", new BsonInt32(1)))
                .verbose(false)
                .action(MapReduceAction.MERGE.getValue())
                .jsMode(true)
                .bypassDocumentValidation(true)
                .collation(collation);
        assertThat(writeOp, isTheSameAs(expectedOperation));

        // subsequent read should have the batchSize set
        FindOperation<?> findOp = (FindOperation<?>) executor.getReadOperation();
        assertEquals(collectionNamespace, findOp.getNamespace());
        assertEquals(99, findOp.getBatchSize());
        assertEquals(collation, findOp.getCollation());

        // toCollection should work as expected
        mapReduceIterable.toCollection();
        writeOp = (MapReduceToCollectionOperation) executor.getWriteOperation();
        assertThat(writeOp, isTheSameAs(expectedOperation));
    }

    @Test
    @DisplayName("should use ClientSession for MapReduceWithInlineResultsOperation")
    void shouldUseClientSessionForInline() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            @SuppressWarnings("unchecked")
            BatchCursor<Document> batchCursor = mock(BatchCursor.class);
            when(batchCursor.hasNext()).thenReturn(false);
            TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(batchCursor, batchCursor));
            MapReduceIterableImpl<Document, Document> mapReduceIterable = new MapReduceIterableImpl<>(session,
                    namespace, Document.class, Document.class, codecRegistry, secondary(), readConcern, writeConcern,
                    executor, "map", "reduce", TIMEOUT_SETTINGS);

            mapReduceIterable.first();
            assertEquals(session, executor.getClientSession());

            mapReduceIterable.iterator();
            assertEquals(session, executor.getClientSession());
        }
    }

    @Test
    @DisplayName("should use ClientSession for MapReduceToCollectionOperation")
    void shouldUseClientSessionForToCollection() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            @SuppressWarnings("unchecked")
            BatchCursor<Document> batchCursor = mock(BatchCursor.class);
            when(batchCursor.hasNext()).thenReturn(false);
            TestOperationExecutor executor = new TestOperationExecutor(
                    Arrays.asList(null, batchCursor, null, batchCursor, null));
            MapReduceIterableImpl<Document, Document> mapReduceIterable = new MapReduceIterableImpl<>(session,
                    namespace, Document.class, Document.class, codecRegistry, secondary(), readConcern, writeConcern,
                    executor, "map", "reduce", TIMEOUT_SETTINGS);
            mapReduceIterable.collectionName("collName");

            mapReduceIterable.first();
            assertEquals(session, executor.getClientSession());
            assertEquals(session, executor.getClientSession());

            mapReduceIterable.iterator();
            assertEquals(session, executor.getClientSession());
            assertEquals(session, executor.getClientSession());

            mapReduceIterable.toCollection();
            assertEquals(session, executor.getClientSession());
        }
    }

    @Test
    @DisplayName("should handle exceptions correctly")
    void shouldHandleExceptions() {
        CodecRegistry altRegistry = fromProviders(Arrays.asList(new ValueCodecProvider(), new BsonValueCodecProvider()));
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(new MongoException("failure")));
        MapReduceIterableImpl<BsonDocument, BsonDocument> mapReduceIterable = new MapReduceIterableImpl<>(null,
                namespace, BsonDocument.class, BsonDocument.class, altRegistry, secondary(), readConcern,
                writeConcern, executor, "map", "reduce", TIMEOUT_SETTINGS);

        assertThrows(MongoException.class, () -> mapReduceIterable.iterator());
        assertThrows(IllegalStateException.class, () -> mapReduceIterable.toCollection());
        assertThrows(CodecConfigurationException.class, () ->
                new MapReduceIterableImpl<>(null, namespace, Document.class, Document.class, altRegistry,
                        secondary(), readConcern, writeConcern, executor, "map", "reduce", TIMEOUT_SETTINGS).iterator());
    }

    @Test
    @DisplayName("should follow the MongoIterable interface as expected")
    void shouldFollowMongoIterableInterface() {
        List<Document> cannedResults = Arrays.asList(new Document("_id", 1), new Document("_id", 2), new Document("_id", 3));
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(
                createBatchCursor(cannedResults), createBatchCursor(cannedResults),
                createBatchCursor(cannedResults), createBatchCursor(cannedResults)));
        MapReduceIterableImpl<Document, Document> mongoIterable = new MapReduceIterableImpl<>(null, namespace,
                Document.class, Document.class, codecRegistry, secondary(), readConcern, writeConcern,
                executor, "map", "reduce", TIMEOUT_SETTINGS);

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
        MapReduceIterableImpl<Document, Document> mongoIterable = new MapReduceIterableImpl<>(null, namespace,
                Document.class, Document.class, codecRegistry, secondary(), readConcern, writeConcern,
                mock(OperationExecutor.class), "map", "reduce", TIMEOUT_SETTINGS);

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
