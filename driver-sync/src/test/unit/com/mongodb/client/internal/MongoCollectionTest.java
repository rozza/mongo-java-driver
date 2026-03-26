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

import com.mongodb.CreateIndexCommitQuorum;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.WriteError;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ImmutableDocument;
import com.mongodb.client.ImmutableDocumentCodecProvider;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.test.Worker;
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.IndexRequest;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.CountDocumentsOperation;
import com.mongodb.internal.operation.CreateIndexesOperation;
import com.mongodb.internal.operation.DropCollectionOperation;
import com.mongodb.internal.operation.DropIndexOperation;
import com.mongodb.internal.operation.EstimatedDocumentCountOperation;
import com.mongodb.internal.operation.FindAndDeleteOperation;
import com.mongodb.internal.operation.FindAndReplaceOperation;
import com.mongodb.internal.operation.FindAndUpdateOperation;
import com.mongodb.internal.operation.ListIndexesOperation;
import com.mongodb.internal.operation.MixedBulkWriteOperation;
import com.mongodb.internal.operation.RenameCollectionOperation;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.UuidCodec;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.CustomMatchers.isTheSameAs;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.ReadPreference.secondary;
import static com.mongodb.WriteConcern.ACKNOWLEDGED;
import static com.mongodb.WriteConcern.UNACKNOWLEDGED;
import static com.mongodb.bulk.BulkWriteResult.acknowledged;
import static com.mongodb.bulk.BulkWriteResult.unacknowledged;
import static com.mongodb.internal.bulk.WriteRequest.Type.DELETE;
import static com.mongodb.internal.bulk.WriteRequest.Type.INSERT;
import static com.mongodb.internal.bulk.WriteRequest.Type.REPLACE;
import static com.mongodb.internal.bulk.WriteRequest.Type.UPDATE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.bson.UuidRepresentation.C_SHARP_LEGACY;
import static org.bson.UuidRepresentation.JAVA_LEGACY;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@SuppressWarnings("deprecation")
class MongoCollectionTest {

    private final MongoNamespace namespace = new MongoNamespace("databaseName", "collectionName");
    private final CodecRegistry codecRegistry = MongoClientSettings.getDefaultCodecRegistry();
    private final ReadConcern readConcern = ReadConcern.MAJORITY;
    private final Collation collation = Collation.builder().locale("en").build();

    @Test
    @DisplayName("should return the correct name from getName")
    void shouldReturnCorrectName() {
        MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class, codecRegistry,
                secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS,
                new TestOperationExecutor(Collections.singletonList(null)));
        assertEquals(namespace, collection.getNamespace());
    }

    @Test
    @DisplayName("should behave correctly when using withDocumentClass")
    void shouldBehaveCorrectlyWithDocumentClass() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
        MongoCollectionImpl<Worker> collection = (MongoCollectionImpl<Worker>) new MongoCollectionImpl<>(namespace,
                Document.class, codecRegistry, secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null,
                TIMEOUT_SETTINGS, executor).withDocumentClass(Worker.class);

        assertEquals(Worker.class, collection.getDocumentClass());
        assertThat(collection, isTheSameAs(new MongoCollectionImpl<>(namespace, Worker.class, codecRegistry,
                secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor)));
    }

    @Test
    @DisplayName("should behave correctly when using withCodecRegistry")
    void shouldBehaveCorrectlyWithCodecRegistry() {
        CodecRegistry newCodecRegistry = fromProviders(new ValueCodecProvider());
        TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
        MongoCollectionImpl<Document> collection = (MongoCollectionImpl<Document>) new MongoCollectionImpl<>(namespace,
                Document.class, codecRegistry, secondary(), ACKNOWLEDGED, true, true, readConcern, C_SHARP_LEGACY,
                null, TIMEOUT_SETTINGS, executor).withCodecRegistry(newCodecRegistry);

        assertEquals(C_SHARP_LEGACY, ((UuidCodec) collection.getCodecRegistry().get(java.util.UUID.class)).getUuidRepresentation());
        assertThat(collection, isTheSameAs(new MongoCollectionImpl<>(namespace, Document.class,
                collection.getCodecRegistry(), secondary(), ACKNOWLEDGED, true, true, readConcern, C_SHARP_LEGACY,
                null, TIMEOUT_SETTINGS, executor)));
    }

    @Test
    @DisplayName("should behave correctly when using withReadPreference")
    void shouldBehaveCorrectlyWithReadPreference() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
        MongoCollectionImpl<Document> collection = (MongoCollectionImpl<Document>) new MongoCollectionImpl<>(namespace,
                Document.class, codecRegistry, secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null,
                TIMEOUT_SETTINGS, executor).withReadPreference(primary());

        assertEquals(primary(), collection.getReadPreference());
        assertThat(collection, isTheSameAs(new MongoCollectionImpl<>(namespace, Document.class, codecRegistry,
                primary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor)));
    }

    @Test
    @DisplayName("should behave correctly when using withWriteConcern")
    void shouldBehaveCorrectlyWithWriteConcern() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
        MongoCollectionImpl<Document> collection = (MongoCollectionImpl<Document>) new MongoCollectionImpl<>(namespace,
                Document.class, codecRegistry, secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null,
                TIMEOUT_SETTINGS, executor).withWriteConcern(WriteConcern.MAJORITY);

        assertEquals(WriteConcern.MAJORITY, collection.getWriteConcern());
    }

    @Test
    @DisplayName("should behave correctly when using withReadConcern")
    void shouldBehaveCorrectlyWithReadConcern() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
        MongoCollectionImpl<Document> collection = (MongoCollectionImpl<Document>) new MongoCollectionImpl<>(namespace,
                Document.class, codecRegistry, secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null,
                TIMEOUT_SETTINGS, executor).withReadConcern(ReadConcern.MAJORITY);

        assertEquals(ReadConcern.MAJORITY, collection.getReadConcern());
    }

    @Test
    @DisplayName("should behave correctly when using withTimeout")
    void shouldBehaveCorrectlyWithTimeout() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
        MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class, codecRegistry,
                secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor);

        MongoCollectionImpl<Document> newCollection = (MongoCollectionImpl<Document>) collection.withTimeout(10_000, MILLISECONDS);
        assertEquals(10_000L, newCollection.getTimeout(MILLISECONDS));

        assertThrows(IllegalArgumentException.class, () -> collection.withTimeout(500, TimeUnit.NANOSECONDS));
    }

    @Test
    @DisplayName("should use CountOperation correctly with countDocuments")
    void shouldUseCountDocumentsCorrectly() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(1L, 2L, 3L));
            MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class,
                    codecRegistry, secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null,
                    TIMEOUT_SETTINGS, executor);

            if (session != null) {
                collection.countDocuments(session);
            } else {
                collection.countDocuments();
            }
            CountDocumentsOperation operation = (CountDocumentsOperation) executor.getReadOperation();
            assertEquals(session, executor.getClientSession());
            assertThat(operation, isTheSameAs(new CountDocumentsOperation(namespace)
                    .filter(new BsonDocument()).retryReads(true)));

            BsonDocument filter = new BsonDocument("a", new BsonInt32(1));
            if (session != null) {
                collection.countDocuments(session, filter);
            } else {
                collection.countDocuments(filter);
            }
            operation = (CountDocumentsOperation) executor.getReadOperation();
            assertEquals(session, executor.getClientSession());
            assertThat(operation, isTheSameAs(new CountDocumentsOperation(namespace)
                    .filter(filter).retryReads(true)));

            BsonDocument hint = new BsonDocument("hint", new BsonInt32(1));
            if (session != null) {
                collection.countDocuments(session, filter, new CountOptions().hint(hint).skip(10).limit(100).collation(collation));
            } else {
                collection.countDocuments(filter, new CountOptions().hint(hint).skip(10).limit(100).collation(collation));
            }
            operation = (CountDocumentsOperation) executor.getReadOperation();
            assertEquals(session, executor.getClientSession());
            assertThat(operation, isTheSameAs(new CountDocumentsOperation(namespace)
                    .filter(filter).hint(hint).skip(10).limit(100).collation(collation).retryReads(true)));
        }
    }

    @Test
    @DisplayName("should use EstimatedDocumentCountOperation correctly")
    void shouldUseEstimatedDocumentCountCorrectly() {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(1L, 2L));
        MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class, codecRegistry,
                secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor);

        collection.estimatedDocumentCount();
        EstimatedDocumentCountOperation operation = (EstimatedDocumentCountOperation) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new EstimatedDocumentCountOperation(namespace).retryReads(true)));

        collection.estimatedDocumentCount(new EstimatedDocumentCountOptions().maxTime(100, MILLISECONDS));
        operation = (EstimatedDocumentCountOperation) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new EstimatedDocumentCountOperation(namespace).retryReads(true)));
    }

    @Test
    @DisplayName("should create DistinctIterable correctly")
    void shouldCreateDistinctIterableCorrectly() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
            MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class,
                    codecRegistry, secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null,
                    TIMEOUT_SETTINGS, executor);

            DistinctIterableImpl<?, ?> distinctIterable;
            if (session != null) {
                distinctIterable = (DistinctIterableImpl<?, ?>) collection.distinct(session, "field", String.class);
            } else {
                distinctIterable = (DistinctIterableImpl<?, ?>) collection.distinct("field", String.class);
            }
            assertThat(distinctIterable, isTheSameAs(new DistinctIterableImpl<>(session, namespace, Document.class,
                    String.class, codecRegistry, secondary(), readConcern, executor, "field", new BsonDocument(),
                    true, TIMEOUT_SETTINGS)));
        }
    }

    @Test
    @DisplayName("should create FindIterable correctly")
    void shouldCreateFindIterableCorrectly() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
            MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class,
                    codecRegistry, secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null,
                    TIMEOUT_SETTINGS, executor);

            FindIterableImpl<?, ?> findIterable;
            if (session != null) {
                findIterable = (FindIterableImpl<?, ?>) collection.find(session);
            } else {
                findIterable = (FindIterableImpl<?, ?>) collection.find();
            }
            assertThat(findIterable, isTheSameAs(new FindIterableImpl<>(session, namespace, Document.class,
                    Document.class, codecRegistry, secondary(), readConcern, executor, new BsonDocument(), true,
                    TIMEOUT_SETTINGS)));

            if (session != null) {
                findIterable = (FindIterableImpl<?, ?>) collection.find(session, BsonDocument.class);
            } else {
                findIterable = (FindIterableImpl<?, ?>) collection.find(BsonDocument.class);
            }
            assertThat(findIterable, isTheSameAs(new FindIterableImpl<>(session, namespace, Document.class,
                    BsonDocument.class, codecRegistry, secondary(), readConcern, executor, new BsonDocument(), true,
                    TIMEOUT_SETTINGS)));
        }
    }

    @Test
    @DisplayName("should create AggregateIterable correctly")
    void shouldCreateAggregateIterableCorrectly() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
            MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class,
                    codecRegistry, secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null,
                    TIMEOUT_SETTINGS, executor);

            AggregateIterableImpl<?, ?> aggregateIterable;
            if (session != null) {
                aggregateIterable = (AggregateIterableImpl<?, ?>) collection.aggregate(session,
                        Collections.singletonList(new Document("$match", 1)));
            } else {
                aggregateIterable = (AggregateIterableImpl<?, ?>) collection.aggregate(
                        Collections.singletonList(new Document("$match", 1)));
            }
            assertThat(aggregateIterable, isTheSameAs(new AggregateIterableImpl<>(session, namespace, Document.class,
                    Document.class, codecRegistry, secondary(), readConcern, ACKNOWLEDGED, executor,
                    Collections.singletonList(new Document("$match", 1)), AggregationLevel.COLLECTION, true,
                    TIMEOUT_SETTINGS)));
        }
    }

    @Test
    @DisplayName("should validate the aggregation pipeline data correctly")
    void shouldValidateAggregationPipeline() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
        MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class, codecRegistry,
                secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor);

        assertThrows(IllegalArgumentException.class, () -> collection.aggregate(null));
        assertThrows(IllegalArgumentException.class, () ->
                collection.aggregate(Collections.singletonList(null)).into(new java.util.ArrayList<>()));
    }

    @Test
    @DisplayName("should create ChangeStreamIterable correctly")
    void shouldCreateChangeStreamIterableCorrectly() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            TestOperationExecutor executor = new TestOperationExecutor(Collections.emptyList());
            MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class,
                    codecRegistry, secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null,
                    TIMEOUT_SETTINGS, executor);

            ChangeStreamIterableImpl<?> changeStreamIterable;
            if (session != null) {
                changeStreamIterable = (ChangeStreamIterableImpl<?>) collection.watch(session);
            } else {
                changeStreamIterable = (ChangeStreamIterableImpl<?>) collection.watch();
            }
            assertThat(changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl<>(session, namespace,
                    codecRegistry, secondary(), readConcern, executor, Collections.emptyList(), Document.class,
                    ChangeStreamLevel.COLLECTION, true, TIMEOUT_SETTINGS), Collections.singletonList("codec")));
        }
    }

    @Test
    @DisplayName("should use FindOneAndDeleteOperation correctly")
    void shouldUseFindOneAndDeleteOperationCorrectly() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null));
            MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class,
                    codecRegistry, secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null,
                    TIMEOUT_SETTINGS, executor);

            if (session != null) {
                collection.findOneAndDelete(session, new Document("a", 1));
            } else {
                collection.findOneAndDelete(new Document("a", 1));
            }
            FindAndDeleteOperation<?> operation = (FindAndDeleteOperation<?>) executor.getWriteOperation();
            assertThat(operation, isTheSameAs(new FindAndDeleteOperation<>(namespace, ACKNOWLEDGED, true,
                    new DocumentCodec()).filter(new BsonDocument("a", new BsonInt32(1)))));

            if (session != null) {
                collection.findOneAndDelete(session, new Document("a", 1),
                        new FindOneAndDeleteOptions().projection(new Document("projection", 1))
                                .maxTime(100, MILLISECONDS).collation(collation));
            } else {
                collection.findOneAndDelete(new Document("a", 1),
                        new FindOneAndDeleteOptions().projection(new Document("projection", 1))
                                .maxTime(100, MILLISECONDS).collation(collation));
            }
            operation = (FindAndDeleteOperation<?>) executor.getWriteOperation();
            assertThat(operation, isTheSameAs(new FindAndDeleteOperation<>(namespace, ACKNOWLEDGED, true,
                    new DocumentCodec()).filter(new BsonDocument("a", new BsonInt32(1)))
                    .projection(new BsonDocument("projection", new BsonInt32(1)))
                    .collation(collation)));
        }
    }

    @Test
    @DisplayName("should use DropCollectionOperation correctly")
    void shouldUseDropCollectionOperationCorrectly() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            TestOperationExecutor executor = new TestOperationExecutor(Collections.singletonList(null));
            MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class,
                    codecRegistry, secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null,
                    TIMEOUT_SETTINGS, executor);

            if (session != null) {
                collection.drop(session);
            } else {
                collection.drop();
            }
            DropCollectionOperation operation = (DropCollectionOperation) executor.getWriteOperation();
            assertThat(operation, isTheSameAs(new DropCollectionOperation(namespace, ACKNOWLEDGED)));
        }
    }

    @Test
    @DisplayName("should use CreateIndexOperations correctly")
    void shouldUseCreateIndexOperationsCorrectly() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null, null, null, null));
            MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class,
                    codecRegistry, secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null,
                    TIMEOUT_SETTINGS, executor);

            // createIndex
            String indexName;
            if (session != null) {
                indexName = collection.createIndex(session, new Document("key", 1));
            } else {
                indexName = collection.createIndex(new Document("key", 1));
            }
            CreateIndexesOperation operation = (CreateIndexesOperation) executor.getWriteOperation();
            assertThat(operation, isTheSameAs(new CreateIndexesOperation(namespace,
                    Collections.singletonList(new IndexRequest(new BsonDocument("key", new BsonInt32(1)))),
                    ACKNOWLEDGED)));
            assertEquals("key_1", indexName);

            // createIndexes
            List<String> indexNames;
            if (session != null) {
                indexNames = collection.createIndexes(session, Arrays.asList(
                        new IndexModel(new Document("key", 1)), new IndexModel(new Document("key1", 1))));
            } else {
                indexNames = collection.createIndexes(Arrays.asList(
                        new IndexModel(new Document("key", 1)), new IndexModel(new Document("key1", 1))));
            }
            operation = (CreateIndexesOperation) executor.getWriteOperation();
            assertThat(operation, isTheSameAs(new CreateIndexesOperation(namespace,
                    Arrays.asList(new IndexRequest(new BsonDocument("key", new BsonInt32(1))),
                            new IndexRequest(new BsonDocument("key1", new BsonInt32(1)))), ACKNOWLEDGED)));
            assertEquals(Arrays.asList("key_1", "key1_1"), indexNames);

            // with commitQuorum
            if (session != null) {
                indexNames = collection.createIndexes(session,
                        Arrays.asList(new IndexModel(new Document("key", 1)), new IndexModel(new Document("key1", 1))),
                        new CreateIndexOptions().commitQuorum(CreateIndexCommitQuorum.VOTING_MEMBERS));
            } else {
                indexNames = collection.createIndexes(
                        Arrays.asList(new IndexModel(new Document("key", 1)), new IndexModel(new Document("key1", 1))),
                        new CreateIndexOptions().commitQuorum(CreateIndexCommitQuorum.VOTING_MEMBERS));
            }
            operation = (CreateIndexesOperation) executor.getWriteOperation();
            assertThat(operation, isTheSameAs(new CreateIndexesOperation(namespace,
                    Arrays.asList(new IndexRequest(new BsonDocument("key", new BsonInt32(1))),
                            new IndexRequest(new BsonDocument("key1", new BsonInt32(1)))), ACKNOWLEDGED)
                    .commitQuorum(CreateIndexCommitQuorum.VOTING_MEMBERS)));
            assertEquals(Arrays.asList("key_1", "key1_1"), indexNames);
        }
    }

    @Test
    @DisplayName("should validate the createIndexes data correctly")
    void shouldValidateCreateIndexesData() {
        MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class, codecRegistry,
                secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS,
                mock(OperationExecutor.class));

        assertThrows(IllegalArgumentException.class, () -> collection.createIndexes(null));
        assertThrows(IllegalArgumentException.class, () -> collection.createIndexes(Collections.singletonList(null)));
    }

    @Test
    @DisplayName("should use ListIndexesOperations correctly")
    void shouldUseListIndexesOperationsCorrectly() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            BatchCursor<Document> batchCursor = mock(BatchCursor.class);
            TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(batchCursor, batchCursor, batchCursor));
            MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class,
                    codecRegistry, secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null,
                    TIMEOUT_SETTINGS, executor);

            if (session != null) {
                collection.listIndexes(session).into(new java.util.ArrayList<>());
            } else {
                collection.listIndexes().into(new java.util.ArrayList<>());
            }
            ListIndexesOperation<?> operation = (ListIndexesOperation<?>) executor.getReadOperation();
            assertThat(operation, isTheSameAs(new ListIndexesOperation<>(namespace, new DocumentCodec()).retryReads(true)));
            assertEquals(session, executor.getClientSession());

            if (session != null) {
                collection.listIndexes(session, BsonDocument.class).into(new java.util.ArrayList<>());
            } else {
                collection.listIndexes(BsonDocument.class).into(new java.util.ArrayList<>());
            }
            operation = (ListIndexesOperation<?>) executor.getReadOperation();
            assertThat(operation, isTheSameAs(new ListIndexesOperation<>(namespace, new BsonDocumentCodec()).retryReads(true)));
            assertEquals(session, executor.getClientSession());
        }
    }

    @Test
    @DisplayName("should use DropIndexOperation correctly for dropIndex")
    void shouldUseDropIndexOperationCorrectly() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null, null));
            MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class,
                    codecRegistry, secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null,
                    TIMEOUT_SETTINGS, executor);

            if (session != null) {
                collection.dropIndex(session, "indexName");
            } else {
                collection.dropIndex("indexName");
            }
            DropIndexOperation operation = (DropIndexOperation) executor.getWriteOperation();
            assertThat(operation, isTheSameAs(new DropIndexOperation(namespace, "indexName", ACKNOWLEDGED)));
            assertEquals(session, executor.getClientSession());

            BsonDocument keys = new BsonDocument("x", new BsonInt32(1));
            if (session != null) {
                collection.dropIndex(session, keys);
            } else {
                collection.dropIndex(keys);
            }
            operation = (DropIndexOperation) executor.getWriteOperation();
            assertThat(operation, isTheSameAs(new DropIndexOperation(namespace, keys, ACKNOWLEDGED)));
            assertEquals(session, executor.getClientSession());
        }
    }

    @Test
    @DisplayName("should use DropIndexOperation correctly for dropIndexes")
    void shouldUseDropIndexesOperationCorrectly() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null));
            MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class,
                    codecRegistry, secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null,
                    TIMEOUT_SETTINGS, executor);

            if (session != null) {
                collection.dropIndexes(session);
            } else {
                collection.dropIndexes();
            }
            DropIndexOperation operation = (DropIndexOperation) executor.getWriteOperation();
            assertThat(operation, isTheSameAs(new DropIndexOperation(namespace, "*", ACKNOWLEDGED)));
            assertEquals(session, executor.getClientSession());
        }
    }

    @Test
    @DisplayName("should use RenameCollectionOperation correctly")
    void shouldUseRenameCollectionOperationCorrectly() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            for (boolean dropTarget : new boolean[]{true, false}) {
                TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null));
                MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class,
                        codecRegistry, secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null,
                        TIMEOUT_SETTINGS, executor);
                MongoNamespace newNamespace = new MongoNamespace(namespace.getDatabaseName(), "newName");

                if (session != null) {
                    collection.renameCollection(session, newNamespace);
                } else {
                    collection.renameCollection(newNamespace);
                }
                RenameCollectionOperation operation = (RenameCollectionOperation) executor.getWriteOperation();
                assertThat(operation, isTheSameAs(new RenameCollectionOperation(namespace, newNamespace, ACKNOWLEDGED)));
                assertEquals(session, executor.getClientSession());

                if (session != null) {
                    collection.renameCollection(session, newNamespace, new RenameCollectionOptions().dropTarget(dropTarget));
                } else {
                    collection.renameCollection(newNamespace, new RenameCollectionOptions().dropTarget(dropTarget));
                }
                operation = (RenameCollectionOperation) executor.getWriteOperation();
                assertThat(operation, isTheSameAs(new RenameCollectionOperation(namespace, newNamespace, ACKNOWLEDGED)
                        .dropTarget(dropTarget)));
                assertEquals(session, executor.getClientSession());
            }
        }
    }

    @Test
    @DisplayName("should handle exceptions in bulkWrite correctly")
    void shouldHandleExceptionsInBulkWrite() {
        CodecRegistry altRegistry = fromProviders(Arrays.asList(new ValueCodecProvider(), new BsonValueCodecProvider()));
        TestOperationExecutor executor = new TestOperationExecutor(
                Collections.singletonList(new MongoException("failure")));
        MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class, altRegistry,
                secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor);

        assertThrows(IllegalArgumentException.class, () -> collection.bulkWrite(null));
        assertThrows(IllegalArgumentException.class, () -> collection.bulkWrite(Collections.singletonList(null)));
        assertThrows(CodecConfigurationException.class, () ->
                collection.bulkWrite(Collections.singletonList(new InsertOneModel<>(new Document("_id", 1)))));
    }

    @Test
    @DisplayName("should translate MongoBulkWriteException to MongoWriteException")
    void shouldTranslateBulkWriteToWriteException() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.singletonList(
                new MongoBulkWriteException(acknowledged(INSERT, 1, 0, Collections.emptyList(), Collections.emptyList()),
                        Collections.singletonList(new BulkWriteError(11000, "oops", new BsonDocument(), 0)),
                        null, new ServerAddress(), new HashSet<>())));
        MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class, codecRegistry,
                secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor);

        MongoWriteException e = assertThrows(MongoWriteException.class, () -> collection.insertOne(new Document("_id", 1)));
        assertEquals(new WriteError(11000, "oops", new BsonDocument()), e.getError());
    }

    @Test
    @DisplayName("should translate MongoBulkWriteException to MongoWriteConcernException")
    void shouldTranslateBulkWriteToWriteConcernException() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.singletonList(
                new MongoBulkWriteException(acknowledged(INSERT, 1, 0, Collections.emptyList(), Collections.emptyList()),
                        Collections.emptyList(),
                        new WriteConcernError(42, "codeName", "Message", new BsonDocument()),
                        new ServerAddress(), new HashSet<>())));
        MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class, codecRegistry,
                secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor);

        MongoWriteConcernException e = assertThrows(MongoWriteConcernException.class, () ->
                collection.insertOne(new Document("_id", 1)));
        assertEquals(new WriteConcernError(42, "codeName", "Message", new BsonDocument()), e.getWriteConcernError());
    }

    @Test
    @DisplayName("should not expect to mutate the document when inserting")
    void shouldNotMutateDocumentWhenInserting() {
        TestOperationExecutor executor = new TestOperationExecutor(
                Collections.singletonList(acknowledged(INSERT, 1, 0, Collections.emptyList(), Collections.emptyList())));
        CodecRegistry customCodecRegistry = CodecRegistries.fromRegistries(
                fromProviders(new ImmutableDocumentCodecProvider()), codecRegistry);
        MongoCollectionImpl<ImmutableDocument> collection = new MongoCollectionImpl<>(namespace, ImmutableDocument.class,
                customCodecRegistry, secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null,
                TIMEOUT_SETTINGS, executor);
        ImmutableDocument document = new ImmutableDocument(Collections.singletonMap("a", 1));

        collection.insertOne(document);
        assertFalse(document.containsKey("_id"));

        MixedBulkWriteOperation op = (MixedBulkWriteOperation) executor.getWriteOperation();
        InsertRequest request = (InsertRequest) op.getWriteRequests().get(0);
        assertTrue(request.getDocument().containsKey("_id"));
    }

    @Test
    @DisplayName("should not expect to mutate the document when bulk writing")
    void shouldNotMutateDocumentWhenBulkWriting() {
        TestOperationExecutor executor = new TestOperationExecutor(Collections.singletonList(null));
        CodecRegistry customCodecRegistry = CodecRegistries.fromRegistries(
                fromProviders(new ImmutableDocumentCodecProvider()), codecRegistry);
        MongoCollectionImpl<ImmutableDocument> collection = new MongoCollectionImpl<>(namespace, ImmutableDocument.class,
                customCodecRegistry, secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null,
                TIMEOUT_SETTINGS, executor);
        ImmutableDocument document = new ImmutableDocument(Collections.singletonMap("a", 1));

        collection.bulkWrite(Collections.singletonList(new InsertOneModel<>(document)));
        assertFalse(document.containsKey("_id"));

        MixedBulkWriteOperation op = (MixedBulkWriteOperation) executor.getWriteOperation();
        InsertRequest request = (InsertRequest) op.getWriteRequests().get(0);
        assertTrue(request.getDocument().containsKey("_id"));
    }

    @Test
    @DisplayName("should validate the insertMany data correctly")
    void shouldValidateInsertManyData() {
        MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class, codecRegistry,
                secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS,
                mock(OperationExecutor.class));

        assertThrows(IllegalArgumentException.class, () -> collection.insertMany(null));
        assertThrows(IllegalArgumentException.class, () -> collection.insertMany(Collections.singletonList(null)));
    }

    @Test
    @DisplayName("should validate the client session correctly")
    void shouldValidateClientSession() {
        MongoCollectionImpl<Document> collection = new MongoCollectionImpl<>(namespace, Document.class, codecRegistry,
                secondary(), ACKNOWLEDGED, true, true, readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS,
                mock(OperationExecutor.class));

        assertThrows(IllegalArgumentException.class, () ->
                collection.aggregate(null, Collections.singletonList(Document.parse("{$match:{}}"))));
        assertThrows(IllegalArgumentException.class, () ->
                collection.bulkWrite(null, Collections.singletonList(new InsertOneModel<>(new Document()))));
        assertThrows(IllegalArgumentException.class, () -> collection.createIndex(null, new Document()));
        assertThrows(IllegalArgumentException.class, () ->
                collection.createIndexes(null, Collections.singletonList(mock(IndexModel.class))));
        assertThrows(IllegalArgumentException.class, () -> collection.deleteMany(null, new Document()));
        assertThrows(IllegalArgumentException.class, () -> collection.deleteOne(null, new Document()));
        assertThrows(IllegalArgumentException.class, () -> collection.distinct(null, "field", Document.class));
        assertThrows(IllegalArgumentException.class, () -> collection.distinct(null, new Document(), Document.class));
        assertThrows(IllegalArgumentException.class, () -> collection.drop((ClientSession) null));
        assertThrows(IllegalArgumentException.class, () -> collection.dropIndex(null, "index"));
        assertThrows(IllegalArgumentException.class, () -> collection.dropIndex(null, new Document()));
        assertThrows(IllegalArgumentException.class, () -> collection.dropIndexes((ClientSession) null));
        assertThrows(IllegalArgumentException.class, () -> collection.find((ClientSession) null));
        assertThrows(IllegalArgumentException.class, () -> collection.findOneAndDelete(null, new Document()));
        assertThrows(IllegalArgumentException.class, () ->
                collection.findOneAndReplace(null, new Document(), new Document()));
        assertThrows(IllegalArgumentException.class, () ->
                collection.findOneAndUpdate(null, new Document(), new Document()));
        assertThrows(IllegalArgumentException.class, () ->
                collection.insertMany(null, Collections.singletonList(new Document())));
        assertThrows(IllegalArgumentException.class, () -> collection.insertOne(null, new Document()));
        assertThrows(IllegalArgumentException.class, () -> collection.listIndexes((ClientSession) null));
        assertThrows(IllegalArgumentException.class, () -> collection.mapReduce(null, ""));
        assertThrows(IllegalArgumentException.class, () ->
                collection.renameCollection(null, new MongoNamespace("db", "coll")));
        assertThrows(IllegalArgumentException.class, () ->
                collection.replaceOne(null, new Document(), new Document()));
        assertThrows(IllegalArgumentException.class, () ->
                collection.updateMany(null, new Document(), Document.parse("{$set: {a: 1}}")));
        assertThrows(IllegalArgumentException.class, () ->
                collection.updateOne(null, new Document(), Document.parse("{$set: {a: 1}}")));
        assertThrows(IllegalArgumentException.class, () -> collection.watch((ClientSession) null));
    }
}
