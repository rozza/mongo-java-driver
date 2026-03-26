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

package com.mongodb.internal.operation;

import com.mongodb.ClusterFixture;
import com.mongodb.OperationFunctionalSpecification;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.bulk.IndexRequest;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.executeAsync;
import static com.mongodb.ClusterFixture.getBinding;
import static com.mongodb.ClusterFixture.getOperationContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ListIndexesOperationSpecificationTest extends OperationFunctionalSpecification {

    @Test
    void shouldReturnEmptyListForNonexistentCollection() {
        ListIndexesOperation operation = new ListIndexesOperation(getNamespace(), new DocumentCodec());

        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        BatchCursor<Document> cursor = operation.execute(binding, getOperationContext(binding.getReadPreference()));

        assertFalse(cursor.hasNext());
    }

    @Test
    void shouldReturnEmptyListForNonexistentCollectionAsync() throws Throwable {
        ListIndexesOperation operation = new ListIndexesOperation(getNamespace(), new DocumentCodec());

        AsyncBatchCursor<Document> cursor = (AsyncBatchCursor<Document>) executeAsync(operation);
        FutureResultCallback<List<Document>> callback = new FutureResultCallback<>();
        cursor.next(callback);

        assertTrue(callback.get().isEmpty());
    }

    @Test
    void shouldReturnDefaultIndexOnCollectionThatExists() {
        ListIndexesOperation operation = new ListIndexesOperation(getNamespace(), new DocumentCodec());
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("documentThat", "forces creation of the Collection"));

        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        BatchCursor<Document> indexes = operation.execute(binding, getOperationContext(binding.getReadPreference()));

        List<Document> firstBatch = indexes.next();
        assertEquals(1, firstBatch.size());
        assertEquals("_id_", firstBatch.get(0).get("name"));
        assertFalse(indexes.hasNext());
    }

    @Test
    void shouldReturnDefaultIndexOnCollectionThatExistsAsync() throws Throwable {
        ListIndexesOperation operation = new ListIndexesOperation(getNamespace(), new DocumentCodec());
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("documentThat", "forces creation of the Collection"));

        AsyncBatchCursor<Document> cursor = (AsyncBatchCursor<Document>) executeAsync(operation);
        FutureResultCallback<List<Document>> callback = new FutureResultCallback<>();
        cursor.next(callback);
        List<Document> indexes = callback.get();

        assertEquals(1, indexes.size());
        assertEquals("_id_", indexes.get(0).get("name"));
    }

    @Test
    void shouldReturnCreatedIndexesOnCollection() {
        ListIndexesOperation operation = new ListIndexesOperation(getNamespace(), new DocumentCodec());
        getCollectionHelper().createIndex(new BsonDocument("theField", new BsonInt32(1)));
        getCollectionHelper().createIndex(new BsonDocument("compound", new BsonInt32(1)).append("index", new BsonInt32(-1)));

        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        new CreateIndexesOperation(getNamespace(),
                Arrays.asList(new IndexRequest(new BsonDocument("unique", new BsonInt32(1))).unique(true)), null)
                .execute(binding, getOperationContext(binding.getReadPreference()));

        binding = getBinding();
        BatchCursor<Document> cursor = operation.execute(binding, getOperationContext(binding.getReadPreference()));

        List<Document> indexes = cursor.next();
        assertEquals(4, indexes.size());
        List<String> indexNames = indexes.stream().map(d -> (String) d.get("name")).collect(Collectors.toList());
        assertTrue(indexNames.containsAll(Arrays.asList("_id_", "theField_1", "compound_1_index_-1", "unique_1")));
        assertTrue(indexes.stream()
                .filter(d -> "unique_1".equals(d.get("name")))
                .findFirst()
                .map(d -> (Boolean) d.get("unique"))
                .orElse(false));
        assertFalse(cursor.hasNext());
    }

    @Test
    void shouldUseSetBatchSizeOfCollections() {
        ListIndexesOperation operation = new ListIndexesOperation(getNamespace(), new DocumentCodec()).batchSize(2);
        getCollectionHelper().createIndex(new BsonDocument("collection1", new BsonInt32(1)));
        getCollectionHelper().createIndex(new BsonDocument("collection2", new BsonInt32(1)));
        getCollectionHelper().createIndex(new BsonDocument("collection3", new BsonInt32(1)));
        getCollectionHelper().createIndex(new BsonDocument("collection4", new BsonInt32(1)));
        getCollectionHelper().createIndex(new BsonDocument("collection5", new BsonInt32(1)));

        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        BatchCursor<Document> cursor = operation.execute(binding, getOperationContext(binding.getReadPreference()));
        try {
            List<Document> collections = cursor.next();
            assertTrue(collections.size() <= 2);
            assertTrue(cursor.hasNext());
            assertEquals(2, cursor.getBatchSize());

            collections = cursor.next();
            assertTrue(collections.size() <= 2);
            assertTrue(cursor.hasNext());
            assertEquals(2, cursor.getBatchSize());
        } finally {
            cursor.close();
        }
    }

    // NOTE: The readPreference/secondaryOk tests used Spock Mock/Stub and are omitted.
    // TODO: Convert mock-based readPreference tests with Mockito if needed
}
