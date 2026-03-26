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
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.OperationFunctionalSpecification;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.ValidationAction;
import com.mongodb.client.model.ValidationLevel;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static com.mongodb.ClusterFixture.getBinding;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionLessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class CreateCollectionOperationSpecificationTest extends OperationFunctionalSpecification {

    @Test
    void shouldHaveCorrectDefaults() {
        CreateCollectionOperation operation = createOperation();

        assertFalse(operation.isCapped());
        assertEquals(0, operation.getSizeInBytes());
        assertTrue(operation.isAutoIndex());
        assertEquals(0, operation.getMaxDocuments());
        assertNull(operation.getStorageEngineOptions());
        assertNull(operation.getIndexOptionDefaults());
        assertNull(operation.getValidator());
        assertNull(operation.getValidationLevel());
        assertNull(operation.getValidationAction());
        assertNull(operation.getCollation());
    }

    @Test
    void shouldSetOptionalValuesCorrectly() {
        BsonDocument storageEngineOptions = BsonDocument.parse("{ wiredTiger : {}}");
        BsonDocument indexOptionDefaults = BsonDocument.parse("{ storageEngine: { wiredTiger : {} }}");
        BsonDocument validator = BsonDocument.parse("{ level: { $gte : 10 }}");

        CreateCollectionOperation operation = createOperation()
                .autoIndex(false)
                .capped(true)
                .sizeInBytes(1000)
                .maxDocuments(1000)
                .storageEngineOptions(storageEngineOptions)
                .indexOptionDefaults(indexOptionDefaults)
                .validator(validator)
                .validationLevel(ValidationLevel.MODERATE)
                .validationAction(ValidationAction.WARN)
                .collation(DEFAULT_COLLATION);

        assertTrue(operation.isCapped());
        assertEquals(1000, operation.getSizeInBytes());
        assertFalse(operation.isAutoIndex());
        assertEquals(1000, operation.getMaxDocuments());
        assertEquals(storageEngineOptions, operation.getStorageEngineOptions());
        assertEquals(indexOptionDefaults, operation.getIndexOptionDefaults());
        assertEquals(validator, operation.getValidator());
        assertEquals(ValidationLevel.MODERATE, operation.getValidationLevel());
        assertEquals(ValidationAction.WARN, operation.getValidationAction());
        assertEquals(DEFAULT_COLLATION, operation.getCollation());
    }

    @Test
    void shouldCreateCollectionSync() {
        assertFalse(collectionNameExists(getCollectionName()));
        execute(createOperation(), false);
        assertTrue(collectionNameExists(getCollectionName()));
    }

    @Test
    void shouldCreateCollectionAsync() {
        assertFalse(collectionNameExists(getCollectionName()));
        execute(createOperation(), true);
        assertTrue(collectionNameExists(getCollectionName()));
    }

    @Test
    void shouldCreateCappedCollectionSync() {
        assertFalse(collectionNameExists(getCollectionName()));
        CreateCollectionOperation operation = createOperation()
                .capped(true)
                .maxDocuments(100)
                .sizeInBytes(40 * 1024);

        execute(operation, false);
        assertTrue(collectionNameExists(getCollectionName()));

        BsonDocument stats = storageStats();
        assertTrue(stats.getBoolean("capped").getValue());
        assertEquals(100, stats.getNumber("max").intValue());
    }

    @Test
    void shouldCreateCappedCollectionAsync() {
        assertFalse(collectionNameExists(getCollectionName()));
        CreateCollectionOperation operation = createOperation()
                .capped(true)
                .maxDocuments(100)
                .sizeInBytes(40 * 1024);

        execute(operation, true);
        assertTrue(collectionNameExists(getCollectionName()));
    }

    @Test
    void shouldAllowValidatorSync() {
        assertFalse(collectionNameExists(getCollectionName()));
        BsonDocument validator = BsonDocument.parse("{ level: { $gte : 10 }}");
        CreateCollectionOperation operation = createOperation()
                .validator(validator)
                .validationLevel(ValidationLevel.MODERATE)
                .validationAction(ValidationAction.ERROR);

        execute(operation, false);

        BsonDocument options = getCollectionInfo(getCollectionName()).getDocument("options");
        assertEquals(validator, options.get("validator"));
        assertEquals(new BsonString(ValidationLevel.MODERATE.getValue()), options.get("validationLevel"));
        assertEquals(new BsonString(ValidationAction.ERROR.getValue()), options.get("validationAction"));

        assertThrows(MongoBulkWriteException.class, () ->
                getCollectionHelper().insertDocuments(BsonDocument.parse("{ level: 8}")));
    }

    @Test
    void shouldThrowOnWriteConcernErrorSync() {
        assumeTrue(isDiscoverableReplicaSet());
        assertFalse(collectionNameExists(getCollectionName()));
        CreateCollectionOperation operation = createOperation(new WriteConcern(5));

        MongoWriteConcernException ex = assertThrows(MongoWriteConcernException.class, () ->
                execute(operation, false));
        assertTrue(ex.getWriteConcernError().getCode() == 100);
        assertTrue(ex.getWriteResult().wasAcknowledged());
    }

    @Test
    void shouldThrowOnWriteConcernErrorAsync() {
        assumeTrue(isDiscoverableReplicaSet());
        assertFalse(collectionNameExists(getCollectionName()));
        CreateCollectionOperation operation = createOperation(new WriteConcern(5));

        MongoWriteConcernException ex = assertThrows(MongoWriteConcernException.class, () ->
                execute(operation, true));
        assertTrue(ex.getWriteConcernError().getCode() == 100);
        assertTrue(ex.getWriteResult().wasAcknowledged());
    }

    @Test
    void shouldCreateCollectionWithCollationSync() {
        CreateCollectionOperation operation = createOperation().collation(DEFAULT_COLLATION);

        execute(operation, false);
        BsonDocument collectionCollation = getCollectionInfo(getCollectionName())
                .getDocument("options").getDocument("collation");
        collectionCollation.remove("version");

        assertEquals(DEFAULT_COLLATION.asDocument(), collectionCollation);
    }

    @Test
    void shouldCreateCollectionWithCollationAsync() {
        CreateCollectionOperation operation = createOperation().collation(DEFAULT_COLLATION);

        execute(operation, true);
        BsonDocument collectionCollation = getCollectionInfo(getCollectionName())
                .getDocument("options").getDocument("collation");
        collectionCollation.remove("version");

        assertEquals(DEFAULT_COLLATION.asDocument(), collectionCollation);
    }

    private BsonDocument getCollectionInfo(final String collectionName) {
        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        com.mongodb.internal.operation.BatchCursor<BsonDocument> cursor = new ListCollectionsOperation(getDatabaseName(), new BsonDocumentCodec())
                .filter(new BsonDocument("name", new BsonString(collectionName)))
                .execute(binding, ClusterFixture.getOperationContext(binding.getReadPreference()));
        java.util.List<BsonDocument> batch = cursor.tryNext();
        return batch != null && !batch.isEmpty() ? batch.get(0) : null;
    }

    private boolean collectionNameExists(final String collectionName) {
        return getCollectionInfo(collectionName) != null;
    }

    @SuppressWarnings("deprecation")
    private BsonDocument storageStats() {
        if (serverVersionLessThan(6, 2)) {
            com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
            return new CommandReadOperation<>(getDatabaseName(),
                    new BsonDocument("collStats", new BsonString(getCollectionName())),
                    new BsonDocumentCodec()).execute(binding, ClusterFixture.getOperationContext(binding.getReadPreference()));
        }
        com.mongodb.internal.binding.ReadWriteBinding binding = ClusterFixture.getBinding();
        BatchCursor<BsonDocument> cursor = new AggregateOperation(
                getNamespace(),
                Collections.singletonList(new BsonDocument("$collStats", new BsonDocument("storageStats", new BsonDocument()))),
                new BsonDocumentCodec()).execute(binding, ClusterFixture.getOperationContext(binding.getReadPreference()));
        try {
            return cursor.next().get(0).getDocument("storageStats");
        } finally {
            cursor.close();
        }
    }

    private CreateCollectionOperation createOperation() {
        return createOperation(null);
    }

    private CreateCollectionOperation createOperation(final WriteConcern writeConcern) {
        return new CreateCollectionOperation(getDatabaseName(), getCollectionName(), writeConcern);
    }
}
