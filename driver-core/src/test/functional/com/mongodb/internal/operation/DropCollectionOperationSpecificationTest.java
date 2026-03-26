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
import com.mongodb.MongoWriteConcernException;
import com.mongodb.OperationFunctionalSpecification;
import com.mongodb.WriteConcern;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.Test;

import static com.mongodb.ClusterFixture.executeAsync;
import static com.mongodb.ClusterFixture.getBinding;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class DropCollectionOperationSpecificationTest extends OperationFunctionalSpecification {

    @Test
    void shouldDropCollectionThatExists() {
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document("documentTo", "createTheCollection"));
        assertTrue(collectionNameExists(getCollectionName()));

        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        new DropCollectionOperation(getNamespace(), WriteConcern.ACKNOWLEDGED)
                .execute(binding, ClusterFixture.getOperationContext(binding.getReadPreference()));

        assertFalse(collectionNameExists(getCollectionName()));
    }

    @Test
    void shouldDropCollectionThatExistsAsync() throws Throwable {
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document("documentTo", "createTheCollection"));
        assertTrue(collectionNameExists(getCollectionName()));

        executeAsync(new DropCollectionOperation(getNamespace(), WriteConcern.ACKNOWLEDGED));

        assertFalse(collectionNameExists(getCollectionName()));
    }

    @Test
    void shouldNotErrorWhenDroppingNonExistentCollection() {
        MongoNamespace namespace = new MongoNamespace(getDatabaseName(), "nonExistingCollection");

        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        new DropCollectionOperation(namespace, WriteConcern.ACKNOWLEDGED)
                .execute(binding, ClusterFixture.getOperationContext(binding.getReadPreference()));

        assertFalse(collectionNameExists("nonExistingCollection"));
    }

    @Test
    void shouldNotErrorWhenDroppingNonExistentCollectionAsync() throws Throwable {
        MongoNamespace namespace = new MongoNamespace(getDatabaseName(), "nonExistingCollection");

        executeAsync(new DropCollectionOperation(namespace, WriteConcern.ACKNOWLEDGED));

        assertFalse(collectionNameExists("nonExistingCollection"));
    }

    @Test
    void shouldThrowOnWriteConcernErrorSync() {
        assumeTrue(isDiscoverableReplicaSet());
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document("documentTo", "createTheCollection"));
        assertTrue(collectionNameExists(getCollectionName()));
        DropCollectionOperation operation = new DropCollectionOperation(getNamespace(), new WriteConcern(5));

        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        MongoWriteConcernException ex = assertThrows(MongoWriteConcernException.class, () ->
                operation.execute(binding, ClusterFixture.getOperationContext(binding.getReadPreference())));
        assertTrue(ex.getWriteConcernError().getCode() == 100);
        assertTrue(ex.getWriteResult().wasAcknowledged());
    }

    @Test
    void shouldThrowOnWriteConcernErrorAsync() {
        assumeTrue(isDiscoverableReplicaSet());
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document("documentTo", "createTheCollection"));
        assertTrue(collectionNameExists(getCollectionName()));
        DropCollectionOperation operation = new DropCollectionOperation(getNamespace(), new WriteConcern(5));

        MongoWriteConcernException ex = assertThrows(MongoWriteConcernException.class, () ->
                executeAsync(operation));
        assertTrue(ex.getWriteConcernError().getCode() == 100);
        assertTrue(ex.getWriteResult().wasAcknowledged());
    }

    private boolean collectionNameExists(final String collectionName) {
        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        com.mongodb.internal.operation.BatchCursor<Document> cursor = new ListCollectionsOperation(getDatabaseName(), new DocumentCodec())
                .execute(binding, ClusterFixture.getOperationContext(binding.getReadPreference()));
        if (!cursor.hasNext()) {
            return false;
        }
        return cursor.next().stream().anyMatch(doc -> collectionName.equals(doc.get("name")));
    }
}
