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
import com.mongodb.CreateIndexCommitQuorum;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoClientException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.OperationFunctionalSpecification;
import com.mongodb.WriteConcern;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.internal.bulk.IndexRequest;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.ClusterFixture.serverVersionLessThan;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class CreateIndexesOperationSpecificationTest extends OperationFunctionalSpecification {

    @Test
    void shouldGetIndexNames() {
        CreateIndexesOperation operation = createOperation(Arrays.asList(
                new IndexRequest(new BsonDocument("field1", new BsonInt32(1))),
                new IndexRequest(new BsonDocument("field2", new BsonInt32(-1))),
                new IndexRequest(new BsonDocument("field3", new BsonInt32(1)).append("field4", new BsonInt32(-1))),
                new IndexRequest(new BsonDocument("field5", new BsonInt32(-1))).name("customName")));

        assertEquals(Arrays.asList("field1_1", "field2_-1", "field3_1_field4_-1", "customName"), operation.getIndexNames());
    }

    @Test
    void shouldCreateSingleIndexSync() {
        BsonDocument keys = new BsonDocument("field", new BsonInt32(1));
        execute(createOperation(Arrays.asList(new IndexRequest(keys))), false);

        assertTrue(getUserCreatedIndexes("key").contains(Collections.singletonMap("field", 1)));
    }

    @Test
    void shouldCreateSingleIndexAsync() {
        BsonDocument keys = new BsonDocument("field", new BsonInt32(1));
        execute(createOperation(Arrays.asList(new IndexRequest(keys))), true);

        assertTrue(getUserCreatedIndexes("key").contains(Collections.singletonMap("field", 1)));
    }

    @Test
    void shouldCreateSingleIndexWithBsonInt64Sync() {
        BsonDocument keys = new BsonDocument("field", new BsonInt64(1));
        execute(createOperation(Arrays.asList(new IndexRequest(keys))), false);

        List<Object> indexKeys = getUserCreatedIndexes("key");
        boolean found = false;
        for (Object key : indexKeys) {
            if (key instanceof java.util.Map) {
                java.util.Map<?, ?> keyMap = (java.util.Map<?, ?>) key;
                Object fieldValue = keyMap.get("field");
                if ((fieldValue instanceof Integer && ((Integer) fieldValue) == 1) ||
                    (fieldValue instanceof Long && ((Long) fieldValue) == 1L)) {
                    found = true;
                    break;
                }
            }
        }
        assertTrue(found);
    }

    @Test
    void shouldCreateMultipleIndexesSync() {
        BsonDocument keysFirst = new BsonDocument("field", new BsonInt32(1));
        BsonDocument keysSecond = new BsonDocument("field2", new BsonInt32(1));
        execute(createOperation(Arrays.asList(new IndexRequest(keysFirst), new IndexRequest(keysSecond))), false);

        List<Object> keys = getUserCreatedIndexes("key");
        assertTrue(keys.contains(Collections.singletonMap("field", 1)));
        assertTrue(keys.contains(Collections.singletonMap("field2", 1)));
    }

    @Test
    void shouldCreateSingleIndexOnNestedFieldSync() {
        BsonDocument keys = new BsonDocument("x.y", new BsonInt32(1));
        execute(createOperation(Arrays.asList(new IndexRequest(keys))), false);

        assertTrue(getUserCreatedIndexes("key").contains(Collections.singletonMap("x.y", 1)));
    }

    @Test
    void shouldHandleDuplicateKeyErrorsSync() {
        Document x1 = new Document("x", 1);
        getCollectionHelper().insertDocuments(new DocumentCodec(), x1, x1);
        CreateIndexesOperation operation = createOperation(
                Arrays.asList(new IndexRequest(new BsonDocument("x", new BsonInt32(1))).unique(true)));

        assertThrows(DuplicateKeyException.class, () -> execute(operation, false));
    }

    @Test
    void shouldHandleDuplicateKeyErrorsAsync() {
        Document x1 = new Document("x", 1);
        getCollectionHelper().insertDocuments(new DocumentCodec(), x1, x1);
        CreateIndexesOperation operation = createOperation(
                Arrays.asList(new IndexRequest(new BsonDocument("x", new BsonInt32(1))).unique(true)));

        RuntimeException ex = assertThrows(RuntimeException.class, () -> execute(operation, true));
        assertTrue(ex.getCause() instanceof DuplicateKeyException);
    }

    @Test
    void shouldThrowWhenBuildingInvalidIndexSync() {
        CreateIndexesOperation operation = createOperation(Arrays.asList(new IndexRequest(new BsonDocument())));
        assertThrows(MongoCommandException.class, () -> execute(operation, false));
    }

    @Test
    void shouldCreateUniqueIndexSync() {
        execute(createOperation(Arrays.asList(new IndexRequest(new BsonDocument("field", new BsonInt32(1))))), false);
        assertEquals(0, getUserCreatedIndexes("unique").size());

        CollectionHelper.drop(getNamespace());
        execute(createOperation(
                Arrays.asList(new IndexRequest(new BsonDocument("field", new BsonInt32(1))).unique(true))), false);
        assertEquals(1, getUserCreatedIndexes("unique").size());
    }

    @Test
    void shouldCreateSparseIndexSync() {
        execute(createOperation(Arrays.asList(new IndexRequest(new BsonDocument("field", new BsonInt32(1))))), false);
        assertEquals(0, getUserCreatedIndexes("sparse").size());

        CollectionHelper.drop(getNamespace());
        execute(createOperation(
                Arrays.asList(new IndexRequest(new BsonDocument("field", new BsonInt32(1))).sparse(true))), false);
        assertEquals(1, getUserCreatedIndexes("sparse").size());
    }

    @Test
    void shouldCreateTTLIndexSync() {
        execute(createOperation(Arrays.asList(new IndexRequest(new BsonDocument("field", new BsonInt32(1))))), false);
        assertEquals(0, getUserCreatedIndexes("expireAfterSeconds").size());

        CollectionHelper.drop(getNamespace());
        execute(createOperation(Arrays.asList(
                new IndexRequest(new BsonDocument("field", new BsonInt32(1))).expireAfter(100L, SECONDS))), false);
        assertEquals(1, getUserCreatedIndexes("expireAfterSeconds").size());
        List<Object> expirations = getUserCreatedIndexes("expireAfterSeconds");
        assertTrue(expirations.size() == 1);
        Object expiration = expirations.get(0);
        assertTrue((expiration instanceof Integer && ((Integer) expiration) == 100) ||
                   (expiration instanceof Long && ((Long) expiration) == 100L));
    }

    @Test
    void shouldCreate2dIndexSync() {
        execute(createOperation(Arrays.asList(
                new IndexRequest(new BsonDocument("field", new BsonString("2d"))))), false);
        assertTrue(getUserCreatedIndexes("key").contains(Collections.singletonMap("field", "2d")));
    }

    @Test
    void shouldCreate2dSphereIndexSync() {
        execute(createOperation(Arrays.asList(
                new IndexRequest(new BsonDocument("field", new BsonString("2dsphere"))))), false);
        assertTrue(getUserCreatedIndexes("key").contains(Collections.singletonMap("field", "2dsphere")));
    }

    @Test
    void shouldCreateTextIndexSync() {
        execute(createOperation(Arrays.asList(
                new IndexRequest(new BsonDocument("field", new BsonString("text")))
                        .defaultLanguage("es")
                        .languageOverride("language")
                        .weights(new BsonDocument("field", new BsonInt32(100))))), false);

        assertEquals(1, getUserCreatedIndexes().size());
        assertTrue(getUserCreatedIndexes("weights").contains(Collections.singletonMap("field", 100)));
        assertTrue(getUserCreatedIndexes("default_language").contains("es"));
        assertTrue(getUserCreatedIndexes("language_override").contains("language"));
    }

    @Test
    void shouldCreateWildcardIndexSync() {
        execute(createOperation(Arrays.asList(
                new IndexRequest(new BsonDocument("$**", new BsonInt32(1))),
                new IndexRequest(new BsonDocument("tags.$**", new BsonInt32(1))))), false);

        assertTrue(getUserCreatedIndexes("key").contains(Collections.singletonMap("$**", 1)));
        assertTrue(getUserCreatedIndexes("key").contains(Collections.singletonMap("tags.$**", 1)));
    }

    @Test
    void shouldCreateIndexWithCollationSync() {
        execute(createOperation(Arrays.asList(
                new IndexRequest(new BsonDocument("a", new BsonInt32(1))).collation(DEFAULT_COLLATION))), false);

        Document index = getIndex("a_1");
        BsonDocument indexCollation = new BsonDocumentWrapper<>((Document) index.get("collation"), new DocumentCodec());
        indexCollation.remove("version");

        assertEquals(DEFAULT_COLLATION.asDocument(), indexCollation);
    }

    @Test
    void shouldThrowOnWriteConcernErrorSync() {
        assumeTrue(isDiscoverableReplicaSet());
        BsonDocument keys = new BsonDocument("field", new BsonInt32(1));
        CreateIndexesOperation operation = new CreateIndexesOperation(getNamespace(),
                Arrays.asList(new IndexRequest(keys)), new WriteConcern(5));

        MongoWriteConcernException ex = assertThrows(MongoWriteConcernException.class, () ->
                execute(operation, false));
        assertTrue(ex.getWriteConcernError().getCode() == 100);
        assertTrue(ex.getWriteResult().wasAcknowledged());
    }

    @Test
    void shouldThrowOnWriteConcernErrorAsync() {
        assumeTrue(isDiscoverableReplicaSet());
        BsonDocument keys = new BsonDocument("field", new BsonInt32(1));
        CreateIndexesOperation operation = new CreateIndexesOperation(getNamespace(),
                Arrays.asList(new IndexRequest(keys)), new WriteConcern(5));

        MongoWriteConcernException ex = assertThrows(MongoWriteConcernException.class, () ->
                execute(operation, true));
        assertTrue(ex.getWriteConcernError().getCode() == 100);
        assertTrue(ex.getWriteResult().wasAcknowledged());
    }

    @Test
    void shouldSetHiddenIndexSync() {
        assumeTrue(serverVersionAtLeast(4, 4));
        execute(createOperation(Arrays.asList(
                new IndexRequest(new BsonDocument("field", new BsonInt32(1))))), false);
        assertEquals(0, getUserCreatedIndexes("hidden").size());

        CollectionHelper.drop(getNamespace());
        execute(createOperation(Arrays.asList(
                new IndexRequest(new BsonDocument("field", new BsonInt32(1))).hidden(true))), false);
        assertEquals(1, getUserCreatedIndexes("hidden").size());
    }

    @Test
    void shouldThrowExceptionIfCommitQuorumSetOnOlderServer() {
        assumeTrue(!serverVersionAtLeast(4, 4));
        BsonDocument keys = new BsonDocument("field", new BsonInt32(1));
        CreateIndexesOperation operation = createOperation(Arrays.asList(new IndexRequest(keys)))
                .commitQuorum(CreateIndexCommitQuorum.MAJORITY);

        assertThrows(MongoClientException.class, () -> execute(operation, false));
    }

    @Test
    void shouldCreateIndexWithCommitQuorum() {
        assumeTrue(isDiscoverableReplicaSet());
        assumeTrue(serverVersionAtLeast(4, 4));
        BsonDocument keys = new BsonDocument("field", new BsonInt32(1));
        CreateIndexesOperation operation = createOperation(Arrays.asList(new IndexRequest(keys)))
                .commitQuorum(CreateIndexCommitQuorum.MAJORITY);

        assertEquals(CreateIndexCommitQuorum.MAJORITY, operation.getCommitQuorum());
        execute(operation, false);
        assertTrue(getUserCreatedIndexes("key").contains(Collections.singletonMap("field", 1)));
    }

    private Document getIndex(final String indexName) {
        return getIndexes().stream()
                .filter(d -> indexName.equals(d.getString("name")))
                .findFirst()
                .orElse(null);
    }

    private List<Document> getIndexes() {
        List<Document> indexes = new ArrayList<>();
        com.mongodb.internal.binding.ReadWriteBinding binding = ClusterFixture.getBinding();
        BatchCursor<Document> cursor = new ListIndexesOperation(getNamespace(), new DocumentCodec())
                .execute(binding, ClusterFixture.getOperationContext(binding.getReadPreference()));
        while (cursor.hasNext()) {
            indexes.addAll(cursor.next());
        }
        return indexes;
    }

    private List<Document> getUserCreatedIndexes() {
        Map<String, Integer> idKey = new HashMap<>();
        idKey.put("_id", 1);
        return getIndexes().stream()
                .filter(d -> !idKey.equals(d.get("key")))
                .collect(Collectors.toList());
    }

    private List<Object> getUserCreatedIndexes(final String keyname) {
        return getUserCreatedIndexes().stream()
                .map(d -> d.get(keyname))
                .filter(v -> v != null)
                .collect(Collectors.toList());
    }

    private CreateIndexesOperation createOperation(final List<IndexRequest> requests) {
        return new CreateIndexesOperation(getNamespace(), requests, null);
    }
}
