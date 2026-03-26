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
import com.mongodb.MongoNamespace;
import com.mongodb.MongoServerException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.OperationFunctionalSpecification;
import com.mongodb.WriteConcern;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static com.mongodb.ClusterFixture.executeAsync;
import static com.mongodb.ClusterFixture.getBinding;
import static com.mongodb.ClusterFixture.getOperationContext;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isSharded;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class RenameCollectionOperationSpecificationTest extends OperationFunctionalSpecification {

    @AfterEach
    public void cleanup() {
        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        new DropCollectionOperation(new MongoNamespace(getDatabaseName(), "newCollection"),
                WriteConcern.ACKNOWLEDGED).execute(binding, getOperationContext(binding.getReadPreference()));
    }

    @Test
    void shouldRenameCollectionSync() {
        assumeTrue(!isSharded());
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("documentThat", "forces creation of the Collection"));
        assertTrue(collectionNameExists(getCollectionName()));
        RenameCollectionOperation operation = new RenameCollectionOperation(getNamespace(),
                new MongoNamespace(getDatabaseName(), "newCollection"), null);

        execute(operation, false);

        assertFalse(collectionNameExists(getCollectionName()));
        assertTrue(collectionNameExists("newCollection"));
    }

    @Test
    void shouldRenameCollectionAsync() {
        assumeTrue(!isSharded());
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("documentThat", "forces creation of the Collection"));
        assertTrue(collectionNameExists(getCollectionName()));
        RenameCollectionOperation operation = new RenameCollectionOperation(getNamespace(),
                new MongoNamespace(getDatabaseName(), "newCollection"), null);

        execute(operation, true);

        assertFalse(collectionNameExists(getCollectionName()));
        assertTrue(collectionNameExists("newCollection"));
    }

    @Test
    void shouldThrowIfNotDropAndCollectionExistsSync() {
        assumeTrue(!isSharded());
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("documentThat", "forces creation of the Collection"));
        assertTrue(collectionNameExists(getCollectionName()));
        RenameCollectionOperation operation = new RenameCollectionOperation(getNamespace(), getNamespace(), null);

        assertThrows(MongoServerException.class, () -> execute(operation, false));
        assertTrue(collectionNameExists(getCollectionName()));
    }

    @Test
    void shouldThrowIfNotDropAndCollectionExistsAsync() {
        assumeTrue(!isSharded());
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("documentThat", "forces creation of the Collection"));
        assertTrue(collectionNameExists(getCollectionName()));
        RenameCollectionOperation operation = new RenameCollectionOperation(getNamespace(), getNamespace(), null);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> execute(operation, true));
        assertTrue(ex.getCause() instanceof MongoServerException);
        assertTrue(collectionNameExists(getCollectionName()));
    }

    @Test
    void shouldThrowOnWriteConcernErrorSync() {
        assumeTrue(!isSharded());
        assumeTrue(isDiscoverableReplicaSet());
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("documentThat", "forces creation of the Collection"));
        assertTrue(collectionNameExists(getCollectionName()));
        RenameCollectionOperation operation = new RenameCollectionOperation(getNamespace(),
                new MongoNamespace(getDatabaseName(), "newCollection"), new WriteConcern(5));

        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        MongoWriteConcernException ex = assertThrows(MongoWriteConcernException.class, () ->
                operation.execute(binding, getOperationContext(binding.getReadPreference())));
        assertTrue(ex.getWriteConcernError().getCode() == 100);
        assertTrue(ex.getWriteResult().wasAcknowledged());
    }

    @Test
    void shouldThrowOnWriteConcernErrorAsync() {
        assumeTrue(!isSharded());
        assumeTrue(isDiscoverableReplicaSet());
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("documentThat", "forces creation of the Collection"));
        assertTrue(collectionNameExists(getCollectionName()));
        RenameCollectionOperation operation = new RenameCollectionOperation(getNamespace(),
                new MongoNamespace(getDatabaseName(), "newCollection"), new WriteConcern(5));

        MongoWriteConcernException ex = assertThrows(MongoWriteConcernException.class, () ->
                executeAsync(operation));
        assertTrue(ex.getWriteConcernError().getCode() == 100);
        assertTrue(ex.getWriteResult().wasAcknowledged());
    }

    private boolean collectionNameExists(final String collectionName) {
        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        BatchCursor<Document> cursor = new ListCollectionsOperation(getDatabaseName(), new DocumentCodec())
                .execute(binding, getOperationContext(binding.getReadPreference()));
        if (!cursor.hasNext()) {
            return false;
        }
        return cursor.next().stream().anyMatch(doc -> collectionName.equals(doc.get("name")));
    }
}
