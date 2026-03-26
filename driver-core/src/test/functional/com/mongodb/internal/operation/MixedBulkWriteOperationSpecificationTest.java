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

import com.mongodb.MongoBulkWriteException;
import com.mongodb.OperationFunctionalSpecification;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.bulk.WriteRequest;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MixedBulkWriteOperationSpecificationTest extends OperationFunctionalSpecification {

    @Test
    void shouldInsertDocumentsSync() {
        MixedBulkWriteOperation operation = new MixedBulkWriteOperation(getNamespace(),
                Arrays.asList(new InsertRequest(new BsonDocument("_id", new BsonInt32(1))),
                        new InsertRequest(new BsonDocument("_id", new BsonInt32(2)))),
                true, WriteConcern.ACKNOWLEDGED, false);
        BulkWriteResult result = execute(operation, false);
        assertEquals(2, result.getInsertedCount());
        assertEquals(2, getCollectionHelper().find().size());
    }

    @Test
    void shouldInsertDocumentsAsync() {
        MixedBulkWriteOperation operation = new MixedBulkWriteOperation(getNamespace(),
                Arrays.asList(new InsertRequest(new BsonDocument("_id", new BsonInt32(1))),
                        new InsertRequest(new BsonDocument("_id", new BsonInt32(2)))),
                true, WriteConcern.ACKNOWLEDGED, false);
        BulkWriteResult result = execute(operation, true);
        assertEquals(2, result.getInsertedCount());
        assertEquals(2, getCollectionHelper().find().size());
    }

    @Test
    void shouldUpdateDocumentsSync() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("_id", 1).append("x", 1),
                new Document("_id", 2).append("x", 2));
        MixedBulkWriteOperation operation = new MixedBulkWriteOperation(getNamespace(),
                Arrays.asList(new UpdateRequest(new BsonDocument("x", new BsonInt32(1)),
                        new BsonDocument("$set", new BsonDocument("x", new BsonInt32(10))),
                        WriteRequest.Type.UPDATE)),
                true, WriteConcern.ACKNOWLEDGED, false);
        BulkWriteResult result = execute(operation, false);
        assertEquals(1, result.getModifiedCount());
    }

    @Test
    void shouldDeleteDocumentsSync() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("_id", 1).append("x", 1),
                new Document("_id", 2).append("x", 2));
        MixedBulkWriteOperation operation = new MixedBulkWriteOperation(getNamespace(),
                Arrays.asList(new DeleteRequest(new BsonDocument("x", new BsonInt32(1)))),
                true, WriteConcern.ACKNOWLEDGED, false);
        BulkWriteResult result = execute(operation, false);
        assertEquals(1, result.getDeletedCount());
        assertEquals(1, getCollectionHelper().find().size());
    }

    @Test
    void shouldThrowOnDuplicateKeyInsert() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("_id", 1));
        MixedBulkWriteOperation operation = new MixedBulkWriteOperation(getNamespace(),
                Arrays.asList(new InsertRequest(new BsonDocument("_id", new BsonInt32(1)))),
                true, WriteConcern.ACKNOWLEDGED, false);
        assertThrows(MongoBulkWriteException.class, () -> execute(operation, false));
    }

    // NOTE: This is a very large test file (1253 lines) with many more tests for:
    // ordered/unordered writes, write concern, retryable writes, transactions, etc.
    // Many use testOperationInTransaction/testOperationRetries mock infrastructure.
    // TODO: Convert remaining tests with Mockito if needed
}
