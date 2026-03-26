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

import com.mongodb.OperationFunctionalSpecification;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AggregateOperationSpecificationTest extends OperationFunctionalSpecification {

    @Test
    void shouldReturnEmptyResultForEmptyCollection() {
        AggregateOperation<Document> operation = new AggregateOperation<>(getNamespace(),
                Collections.emptyList(), new DocumentCodec());
        List<Document> results = executeAndCollectBatchCursorResults(operation, false);
        assertEquals(0, results.size());
    }

    @Test
    void shouldReturnAllDocumentsWithEmptyPipelineSync() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("_id", 1).append("x", 1),
                new Document("_id", 2).append("x", 2));
        AggregateOperation<Document> operation = new AggregateOperation<>(getNamespace(),
                Collections.emptyList(), new DocumentCodec());
        List<Document> results = executeAndCollectBatchCursorResults(operation, false);
        assertEquals(2, results.size());
    }

    @Test
    void shouldReturnAllDocumentsWithEmptyPipelineAsync() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("_id", 1).append("x", 1),
                new Document("_id", 2).append("x", 2));
        AggregateOperation<Document> operation = new AggregateOperation<>(getNamespace(),
                Collections.emptyList(), new DocumentCodec());
        List<Document> results = executeAndCollectBatchCursorResults(operation, true);
        assertEquals(2, results.size());
    }

    @Test
    void shouldReturnFilteredDocumentsWithMatchStage() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("_id", 1).append("x", 1),
                new Document("_id", 2).append("x", 2),
                new Document("_id", 3).append("x", 1));
        AggregateOperation<Document> operation = new AggregateOperation<>(getNamespace(),
                Collections.singletonList(new BsonDocument("$match", new BsonDocument("x", new BsonInt32(1)))),
                new DocumentCodec());
        List<Document> results = executeAndCollectBatchCursorResults(operation, false);
        assertEquals(2, results.size());
    }

    // NOTE: Many mock-based tests for readPreference, readConcern, collation, hint,
    // batchSize, allowDiskUse, etc. are omitted.
    // TODO: Convert mock-based tests with Mockito if needed
}
