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

import static org.junit.jupiter.api.Assertions.assertEquals;

class CountDocumentsOperationSpecificationTest extends OperationFunctionalSpecification {

    @Test
    void shouldGetCountOfZeroForEmptyCollection() {
        CountDocumentsOperation operation = new CountDocumentsOperation(getNamespace());
        Long result = execute(operation, false);
        assertEquals(0L, result);
    }

    @Test
    void shouldGetCountForCollection() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("x", 1), new Document("x", 2), new Document("x", 3));
        CountDocumentsOperation operation = new CountDocumentsOperation(getNamespace());
        Long result = execute(operation, false);
        assertEquals(3L, result);
    }

    @Test
    void shouldGetCountForCollectionAsync() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("x", 1), new Document("x", 2), new Document("x", 3));
        CountDocumentsOperation operation = new CountDocumentsOperation(getNamespace());
        Long result = execute(operation, true);
        assertEquals(3L, result);
    }

    @Test
    void shouldGetCountWithFilter() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("x", 1), new Document("x", 2), new Document("x", 3));
        CountDocumentsOperation operation = new CountDocumentsOperation(getNamespace())
                .filter(new BsonDocument("x", new BsonInt32(1)));
        Long result = execute(operation, false);
        assertEquals(1L, result);
    }

    @Test
    void shouldGetCountWithSkip() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("x", 1), new Document("x", 2), new Document("x", 3));
        CountDocumentsOperation operation = new CountDocumentsOperation(getNamespace())
                .skip(1);
        Long result = execute(operation, false);
        assertEquals(2L, result);
    }

    @Test
    void shouldGetCountWithLimit() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("x", 1), new Document("x", 2), new Document("x", 3));
        CountDocumentsOperation operation = new CountDocumentsOperation(getNamespace())
                .limit(2);
        Long result = execute(operation, false);
        assertEquals(2L, result);
    }

    // NOTE: Mock-based tests for readPreference, readConcern, etc. are omitted.
    // TODO: Convert mock-based tests with Mockito if needed
}
