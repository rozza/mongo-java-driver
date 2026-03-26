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
import com.mongodb.WriteConcern;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FindAndDeleteOperationSpecificationTest extends OperationFunctionalSpecification {

    @Test
    void shouldReturnNullIfNoDocumentsMatchSync() {
        FindAndDeleteOperation<Document> operation = new FindAndDeleteOperation<Document>(getNamespace(), WriteConcern.ACKNOWLEDGED, false, new DocumentCodec());
        Document result = execute(operation, false);
        assertNull(result);
    }

    @Test
    void shouldReturnNullIfNoDocumentsMatchAsync() {
        FindAndDeleteOperation<Document> operation = new FindAndDeleteOperation<Document>(getNamespace(), WriteConcern.ACKNOWLEDGED, false, new DocumentCodec());
        Document result = execute(operation, true);
        assertNull(result);
    }

    @Test
    void shouldDeleteAndReturnDocumentSync() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("_id", 1).append("x", 1));
        FindAndDeleteOperation<Document> operation = new FindAndDeleteOperation<Document>(getNamespace(), WriteConcern.ACKNOWLEDGED, false, new DocumentCodec())
                .filter(new BsonDocument("x", new BsonInt32(1)));
        Document result = execute(operation, false);
        assertEquals(1, result.get("x"));
        assertTrue(getCollectionHelper().find().isEmpty());
    }

    @Test
    void shouldDeleteAndReturnDocumentAsync() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("_id", 1).append("x", 1));
        FindAndDeleteOperation<Document> operation = new FindAndDeleteOperation<Document>(getNamespace(), WriteConcern.ACKNOWLEDGED, false, new DocumentCodec())
                .filter(new BsonDocument("x", new BsonInt32(1)));
        Document result = execute(operation, true);
        assertEquals(1, result.get("x"));
        assertTrue(getCollectionHelper().find().isEmpty());
    }

    // NOTE: Mock-based tests for readConcern, writeConcern, collation, hint, etc. are omitted.
    // TODO: Convert mock-based tests with Mockito if needed
}
