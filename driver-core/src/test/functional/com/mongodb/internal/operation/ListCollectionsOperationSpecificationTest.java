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

import com.mongodb.MongoNamespace;
import com.mongodb.OperationFunctionalSpecification;
import com.mongodb.WriteConcern;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.internal.async.AsyncBatchCursor;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.executeAsync;
import static com.mongodb.ClusterFixture.getBinding;
import static com.mongodb.ClusterFixture.getOperationContext;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ListCollectionsOperationSpecificationTest extends OperationFunctionalSpecification {

    private final String madeUpDatabase = "MadeUpDatabase";

    @Test
    void shouldReturnEmptySetIfDatabaseDoesNotExist() {
        ListCollectionsOperation operation = new ListCollectionsOperation(madeUpDatabase, new DocumentCodec());

        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        com.mongodb.internal.operation.BatchCursor<Document> cursor = operation.execute(binding, getOperationContext(binding.getReadPreference()));

        assertFalse(cursor.hasNext());
        getCollectionHelper().dropDatabase(madeUpDatabase);
    }

    @Test
    void shouldReturnEmptyCursorIfDatabaseDoesNotExistAsync() throws Throwable {
        ListCollectionsOperation operation = new ListCollectionsOperation(madeUpDatabase, new DocumentCodec());

        AsyncBatchCursor<Document> cursor = (AsyncBatchCursor<Document>) executeAsync(operation);
        FutureResultCallback<List<Document>> callback = new FutureResultCallback<>();
        cursor.next(callback);

        assertTrue(callback.get().isEmpty());
        getCollectionHelper().dropDatabase(madeUpDatabase);
    }

    @Test
    void shouldReturnCollectionNamesIfCollectionExists() {
        ListCollectionsOperation operation = new ListCollectionsOperation(getDatabaseName(), new DocumentCodec());
        DocumentCodec codec = new DocumentCodec();
        getCollectionHelper().insertDocuments(codec, new Document("a", 1));
        getCollectionHelper(new MongoNamespace(getDatabaseName(), "collection2")).insertDocuments(codec, new Document("a", 1));

        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        com.mongodb.internal.operation.BatchCursor<Document> cursor = operation.execute(binding, getOperationContext(binding.getReadPreference()));
        List<Document> collections = cursor.next();
        List<String> names = collections.stream().map(d -> (String) d.get("name")).collect(Collectors.toList());

        assertTrue(names.contains(getCollectionName()));
        assertTrue(names.contains("collection2"));
        assertFalse(names.contains(null));
        assertTrue(names.stream().noneMatch(n -> n.contains("$")));
    }

    @Test
    void shouldFilterCollectionNamesIfNameFilterIsSpecified() {
        ListCollectionsOperation operation = new ListCollectionsOperation(getDatabaseName(), new DocumentCodec())
                .filter(new BsonDocument("name", new BsonString("collection2")));
        DocumentCodec codec = new DocumentCodec();
        getCollectionHelper().insertDocuments(codec, new Document("a", 1));
        getCollectionHelper(new MongoNamespace(getDatabaseName(), "collection2")).insertDocuments(codec, new Document("a", 1));

        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        com.mongodb.internal.operation.BatchCursor<Document> cursor = operation.execute(binding, getOperationContext(binding.getReadPreference()));
        List<Document> collections = cursor.next();
        List<String> names = collections.stream().map(d -> (String) d.get("name")).collect(Collectors.toList());

        assertTrue(names.contains("collection2"));
        assertFalse(names.contains(getCollectionName()));
    }

    @Test
    void shouldFilterCappedCollections() {
        ListCollectionsOperation operation = new ListCollectionsOperation(getDatabaseName(), new DocumentCodec())
                .filter(new BsonDocument("options.capped", BsonBoolean.TRUE));
        getCollectionHelper().create("collection3", new CreateCollectionOptions().capped(true).sizeInBytes(1000));
        DocumentCodec codec = new DocumentCodec();
        getCollectionHelper().insertDocuments(codec, new Document("a", 1));

        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        com.mongodb.internal.operation.BatchCursor<Document> cursor = operation.execute(binding, getOperationContext(binding.getReadPreference()));
        List<Document> collections = cursor.next();
        List<String> names = collections.stream().map(d -> (String) d.get("name")).collect(Collectors.toList());

        assertTrue(names.contains("collection3"));
        assertFalse(names.contains(getCollectionName()));
    }

    @Test
    void shouldOnlyGetCollectionNamesWhenNameOnlyIsRequested() {
        ListCollectionsOperation operation = new ListCollectionsOperation(getDatabaseName(), new DocumentCodec())
                .nameOnly(true);
        getCollectionHelper().create("collection5", new CreateCollectionOptions());

        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        com.mongodb.internal.operation.BatchCursor<Document> cursor = operation.execute(binding, getOperationContext(binding.getReadPreference()));
        Document collection = cursor.next().get(0);

        assertTrue(collection.size() == 2);
    }

    @Test
    void shouldOnlyGetCollectionNamesWhenNameOnlyAndAuthorizedCollectionsAreRequested() {
        ListCollectionsOperation operation = new ListCollectionsOperation(getDatabaseName(), new DocumentCodec())
                .nameOnly(true)
                .authorizedCollections(true);
        getCollectionHelper().create("collection6", new CreateCollectionOptions());

        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        com.mongodb.internal.operation.BatchCursor<Document> cursor = operation.execute(binding, getOperationContext(binding.getReadPreference()));
        Document collection = cursor.next().get(0);

        assertTrue(collection.size() == 2);
    }

    @Test
    void shouldGetAllFieldsWhenAuthorizedCollectionsAndNotNameOnly() {
        ListCollectionsOperation operation = new ListCollectionsOperation(getDatabaseName(), new DocumentCodec())
                .nameOnly(false)
                .authorizedCollections(true);
        getCollectionHelper().create("collection8", new CreateCollectionOptions());

        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        com.mongodb.internal.operation.BatchCursor<Document> cursor = operation.execute(binding, getOperationContext(binding.getReadPreference()));
        Document collection = cursor.next().get(0);

        assertTrue(collection.size() > 2);
    }

    @Test
    void shouldUseSetBatchSizeOfCollections() {
        ListCollectionsOperation operation = new ListCollectionsOperation(getDatabaseName(), new DocumentCodec()).batchSize(2);
        DocumentCodec codec = new DocumentCodec();
        getCollectionHelper().insertDocuments(codec, new Document("a", 1));
        getCollectionHelper(new MongoNamespace(getDatabaseName(), "collection2")).insertDocuments(codec, new Document("a", 1));
        getCollectionHelper(new MongoNamespace(getDatabaseName(), "collection3")).insertDocuments(codec, new Document("a", 1));
        getCollectionHelper(new MongoNamespace(getDatabaseName(), "collection4")).insertDocuments(codec, new Document("a", 1));
        getCollectionHelper(new MongoNamespace(getDatabaseName(), "collection5")).insertDocuments(codec, new Document("a", 1));

        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        com.mongodb.internal.operation.BatchCursor<Document> cursor = operation.execute(binding, getOperationContext(binding.getReadPreference()));
        try {
            List<Document> collections = cursor.next();
            assertTrue(collections.size() <= 2);
            assertTrue(cursor.hasNext());
            assertTrue(cursor.getBatchSize() == 2);

            collections = cursor.next();
            assertTrue(collections.size() <= 2);
            assertTrue(cursor.hasNext());
            assertTrue(cursor.getBatchSize() == 2);
        } finally {
            cursor.close();
        }
    }

    @Test
    void shouldFilterIndexesWhenCallingHasNextBeforeNext() {
        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        new DropDatabaseOperation(getDatabaseName(), WriteConcern.ACKNOWLEDGED)
                .execute(binding, getOperationContext(binding.getReadPreference()));
        addSeveralIndexes();
        ListCollectionsOperation operation = new ListCollectionsOperation(getDatabaseName(), new DocumentCodec()).batchSize(2);

        binding = getBinding();
        com.mongodb.internal.operation.BatchCursor<Document> cursor = operation.execute(binding, getOperationContext(binding.getReadPreference()));

        assertTrue(cursor.hasNext());
        assertTrue(cursor.hasNext());
        List<Document> list = cursorToListWithNext(cursor);
        assertTrue(list.stream().anyMatch(d -> getCollectionName().equals(d.get("name"))));
        assertFalse(cursor.hasNext());
    }

    @Test
    void shouldFilterIndexesWithoutCallingHasNextBeforeNext() {
        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        new DropDatabaseOperation(getDatabaseName(), WriteConcern.ACKNOWLEDGED)
                .execute(binding, getOperationContext(binding.getReadPreference()));
        addSeveralIndexes();
        ListCollectionsOperation operation = new ListCollectionsOperation(getDatabaseName(), new DocumentCodec()).batchSize(2);

        binding = getBinding();
        com.mongodb.internal.operation.BatchCursor<Document> cursor = operation.execute(binding, getOperationContext(binding.getReadPreference()));
        List<Document> list = cursorToListWithNext(cursor);

        assertTrue(list.stream().anyMatch(d -> getCollectionName().equals(d.get("name"))));
        assertTrue(list.stream().noneMatch(d -> ((String) d.get("name")).contains("$")));
        assertFalse(cursor.hasNext());
        assertThrows(NoSuchElementException.class, () -> cursor.next());
    }

    // NOTE: The readPreference/secondaryOk tests used Spock Mock/Stub and are omitted.
    // TODO: Convert mock-based readPreference tests with Mockito if needed

    private void addSeveralIndexes() {
        getCollectionHelper().create(getCollectionName(), new CreateCollectionOptions());
        getCollectionHelper().createIndex(new Document("a", 1));
        getCollectionHelper().createIndex(new Document("b", 1));
        getCollectionHelper().createIndex(new Document("c", 1));
        getCollectionHelper().createIndex(new Document("d", 1));
        getCollectionHelper().createIndex(new Document("e", 1));
        getCollectionHelper().createIndex(new Document("f", 1));
        getCollectionHelper().createIndex(new Document("g", 1));
    }

    private List<Document> cursorToListWithNext(final BatchCursor<Document> cursor) {
        List<Document> list = new ArrayList<>();
        while (cursor.hasNext()) {
            list.addAll(cursor.next());
        }
        return list;
    }
}
