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
import com.mongodb.MongoException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.OperationFunctionalSpecification;
import com.mongodb.WriteConcern;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.ClusterFixture.getBinding;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class DropIndexOperationSpecificationTest extends OperationFunctionalSpecification {

    @Test
    void shouldNotErrorWhenDroppingNonExistentIndexOnNonExistentCollectionSync() {
        execute(new DropIndexOperation(getNamespace(), "made_up_index_1", null), false);
        assertEquals(0, getIndexes().size());
    }

    @Test
    void shouldNotErrorWhenDroppingNonExistentIndexOnNonExistentCollectionAsync() {
        execute(new DropIndexOperation(getNamespace(), "made_up_index_1", null), true);
        assertEquals(0, getIndexes().size());
    }

    @Test
    void shouldErrorWhenDroppingNonExistentIndexOnExistingCollectionSync() {
        assumeTrue(!serverVersionAtLeast(8, 3));
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("documentThat", "forces creation of the Collection"));
        assertThrows(MongoException.class, () ->
                execute(new DropIndexOperation(getNamespace(), "made_up_index_1", null), false));
    }

    @Test
    void shouldErrorWhenDroppingNonExistentIndexOnExistingCollectionAsync() {
        assumeTrue(!serverVersionAtLeast(8, 3));
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("documentThat", "forces creation of the Collection"));
        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                execute(new DropIndexOperation(getNamespace(), "made_up_index_1", null), true));
        assertTrue(ex.getCause() instanceof MongoException);
    }

    @Test
    void shouldDropExistingIndexByNameSync() {
        getCollectionHelper().createIndex(new BsonDocument("theField", new BsonInt32(1)));

        execute(new DropIndexOperation(getNamespace(), "theField_1", null), false);
        List<Document> indexes = getIndexes();

        assertEquals(1, indexes.size());
        assertEquals("_id_", indexes.get(0).get("name"));
    }

    @Test
    void shouldDropExistingIndexByNameAsync() {
        getCollectionHelper().createIndex(new BsonDocument("theField", new BsonInt32(1)));

        execute(new DropIndexOperation(getNamespace(), "theField_1", null), true);
        List<Document> indexes = getIndexes();

        assertEquals(1, indexes.size());
        assertEquals("_id_", indexes.get(0).get("name"));
    }

    @Test
    void shouldDropExistingIndexByKeysSync() {
        BsonDocument keys = new BsonDocument("theField", new BsonInt32(1));
        getCollectionHelper().createIndex(keys);

        execute(new DropIndexOperation(getNamespace(), keys, null), false);
        List<Document> indexes = getIndexes();

        assertEquals(1, indexes.size());
        assertEquals("_id_", indexes.get(0).get("name"));
    }

    @Test
    void shouldDropExistingIndexByKeysAsync() {
        BsonDocument keys = new BsonDocument("theField", new BsonInt32(1));
        getCollectionHelper().createIndex(keys);

        execute(new DropIndexOperation(getNamespace(), keys, null), true);
        List<Document> indexes = getIndexes();

        assertEquals(1, indexes.size());
        assertEquals("_id_", indexes.get(0).get("name"));
    }

    @Test
    void shouldDropExistingCompoundIndexByKeys() {
        BsonDocument keys = new BsonDocument("theField", new BsonInt32(1))
                .append("theSecondField", new BsonInt32(-1));
        getCollectionHelper().createIndex(keys);

        execute(new DropIndexOperation(getNamespace(), keys, null), false);
        List<Document> indexes = getIndexes();

        assertEquals(1, indexes.size());
        assertEquals("_id_", indexes.get(0).get("name"));
    }

    @Test
    void shouldDropExisting2dIndexByKeys() {
        BsonDocument keys = new BsonDocument("theField", new BsonString("2d"));
        getCollectionHelper().createIndex(keys);

        execute(new DropIndexOperation(getNamespace(), keys, null), false);
        List<Document> indexes = getIndexes();

        assertEquals(1, indexes.size());
        assertEquals("_id_", indexes.get(0).get("name"));
    }

    @Test
    void shouldDropExistingHashedIndexByKeys() {
        BsonDocument keys = new BsonDocument("theField", new BsonString("hashed"));
        getCollectionHelper().createIndex(keys);

        execute(new DropIndexOperation(getNamespace(), keys, null), false);
        List<Document> indexes = getIndexes();

        assertEquals(1, indexes.size());
        assertEquals("_id_", indexes.get(0).get("name"));
    }

    @Test
    void shouldDropExistingIndexByKeyWhenUsingBsonInt64Sync() {
        BsonDocument keys = new BsonDocument("theField", new BsonInt32(1));
        getCollectionHelper().createIndex(keys);

        execute(new DropIndexOperation(getNamespace(), new BsonDocument("theField", new BsonInt64(1)), null), false);
        List<Document> indexes = getIndexes();

        assertEquals(1, indexes.size());
        assertEquals("_id_", indexes.get(0).get("name"));
    }

    @Test
    void shouldDropExistingIndexByKeyWhenUsingBsonInt64Async() {
        BsonDocument keys = new BsonDocument("theField", new BsonInt32(1));
        getCollectionHelper().createIndex(keys);

        execute(new DropIndexOperation(getNamespace(), new BsonDocument("theField", new BsonInt64(1)), null), true);
        List<Document> indexes = getIndexes();

        assertEquals(1, indexes.size());
        assertEquals("_id_", indexes.get(0).get("name"));
    }

    @Test
    void shouldDropAllIndexesWhenPassedStarSync() {
        getCollectionHelper().createIndex(new BsonDocument("theField", new BsonInt32(1)));
        getCollectionHelper().createIndex(new BsonDocument("theOtherField", new BsonInt32(1)));

        execute(new DropIndexOperation(getNamespace(), "*", null), false);
        List<Document> indexes = getIndexes();

        assertEquals(1, indexes.size());
        assertEquals("_id_", indexes.get(0).get("name"));
    }

    @Test
    void shouldDropAllIndexesWhenPassedStarAsync() {
        getCollectionHelper().createIndex(new BsonDocument("theField", new BsonInt32(1)));
        getCollectionHelper().createIndex(new BsonDocument("theOtherField", new BsonInt32(1)));

        execute(new DropIndexOperation(getNamespace(), "*", null), true);
        List<Document> indexes = getIndexes();

        assertEquals(1, indexes.size());
        assertEquals("_id_", indexes.get(0).get("name"));
    }

    @Test
    void shouldThrowOnWriteConcernErrorSync() {
        assumeTrue(isDiscoverableReplicaSet());
        getCollectionHelper().createIndex(new BsonDocument("theField", new BsonInt32(1)));
        DropIndexOperation operation = new DropIndexOperation(getNamespace(), "theField_1", new WriteConcern(5));

        MongoWriteConcernException ex = assertThrows(MongoWriteConcernException.class, () ->
                execute(operation, false));
        assertTrue(ex.getWriteConcernError().getCode() == 100);
        assertTrue(ex.getWriteResult().wasAcknowledged());
    }

    @Test
    void shouldThrowOnWriteConcernErrorAsync() {
        assumeTrue(isDiscoverableReplicaSet());
        getCollectionHelper().createIndex(new BsonDocument("theField", new BsonInt32(1)));
        DropIndexOperation operation = new DropIndexOperation(getNamespace(), "theField_1", new WriteConcern(5));

        MongoWriteConcernException ex = assertThrows(MongoWriteConcernException.class, () ->
                execute(operation, true));
        assertTrue(ex.getWriteConcernError().getCode() == 100);
        assertTrue(ex.getWriteResult().wasAcknowledged());
    }

    private List<Document> getIndexes() {
        List<Document> indexes = new ArrayList<>();
        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        com.mongodb.internal.operation.BatchCursor<Document> cursor = new ListIndexesOperation(getNamespace(), new DocumentCodec())
                .execute(binding, ClusterFixture.getOperationContext(binding.getReadPreference()));
        while (cursor.hasNext()) {
            indexes.addAll(cursor.next());
        }
        return indexes;
    }
}
