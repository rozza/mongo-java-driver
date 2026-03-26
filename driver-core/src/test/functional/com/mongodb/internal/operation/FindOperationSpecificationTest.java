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
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FindOperationSpecificationTest extends OperationFunctionalSpecification {

    @Test
    void shouldReturnEmptyResultForEmptyCollectionSync() {
        FindOperation<Document> operation = new FindOperation<>(getNamespace(), new DocumentCodec());
        List<Document> results = executeAndCollectBatchCursorResults(operation, false);
        assertEquals(0, results.size());
    }

    @Test
    void shouldReturnEmptyResultForEmptyCollectionAsync() {
        FindOperation<Document> operation = new FindOperation<>(getNamespace(), new DocumentCodec());
        List<Document> results = executeAndCollectBatchCursorResults(operation, true);
        assertEquals(0, results.size());
    }

    @Test
    void shouldFindAllDocumentsSync() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("_id", 1).append("x", 1),
                new Document("_id", 2).append("x", 2),
                new Document("_id", 3).append("x", 3));
        FindOperation<Document> operation = new FindOperation<>(getNamespace(), new DocumentCodec());
        List<Document> results = executeAndCollectBatchCursorResults(operation, false);
        assertEquals(3, results.size());
    }

    @Test
    void shouldFindAllDocumentsAsync() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("_id", 1).append("x", 1),
                new Document("_id", 2).append("x", 2),
                new Document("_id", 3).append("x", 3));
        FindOperation<Document> operation = new FindOperation<>(getNamespace(), new DocumentCodec());
        List<Document> results = executeAndCollectBatchCursorResults(operation, true);
        assertEquals(3, results.size());
    }

    @Test
    void shouldFindWithFilter() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("_id", 1).append("x", 1),
                new Document("_id", 2).append("x", 2),
                new Document("_id", 3).append("x", 1));
        FindOperation<Document> operation = new FindOperation<>(getNamespace(), new DocumentCodec())
                .filter(new BsonDocument("x", new BsonInt32(1)));
        List<Document> results = executeAndCollectBatchCursorResults(operation, false);
        assertEquals(2, results.size());
    }

    @Test
    void shouldFindWithLimitAndSkip() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("_id", 1).append("x", 1),
                new Document("_id", 2).append("x", 2),
                new Document("_id", 3).append("x", 3));
        FindOperation<Document> operation = new FindOperation<>(getNamespace(), new DocumentCodec())
                .skip(1)
                .limit(1);
        List<Document> results = executeAndCollectBatchCursorResults(operation, false);
        assertEquals(1, results.size());
    }

    // NOTE: Mock-based tests for readPreference, readConcern, collation, hint,
    // batchSize, projection, sort, noCursorTimeout, etc. are omitted.
    // TODO: Convert mock-based tests with Mockito if needed
}
