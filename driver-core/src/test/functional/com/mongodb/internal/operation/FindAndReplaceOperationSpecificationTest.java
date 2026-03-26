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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class FindAndReplaceOperationSpecificationTest extends OperationFunctionalSpecification {

    @Test
    void shouldReturnNullIfNoDocumentsMatchSync() {
        FindAndReplaceOperation<Document> operation = new FindAndReplaceOperation<Document>(getNamespace(),
                WriteConcern.ACKNOWLEDGED, false, new DocumentCodec(),
                new BsonDocument("_id", new BsonInt32(1)).append("x", new BsonInt32(2)));
        Document result = execute(operation, false);
        assertNull(result);
    }

    @Test
    void shouldReplaceAndReturnOldDocumentSync() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("_id", 1).append("x", 1));
        FindAndReplaceOperation<Document> operation = new FindAndReplaceOperation<Document>(getNamespace(),
                WriteConcern.ACKNOWLEDGED, false, new DocumentCodec(),
                new BsonDocument("_id", new BsonInt32(1)).append("x", new BsonInt32(2)))
                .filter(new BsonDocument("x", new BsonInt32(1)));
        Document result = execute(operation, false);
        assertEquals(1, result.get("x"));
        assertEquals(2, getCollectionHelper().find().get(0).get("x"));
    }

    @Test
    void shouldReplaceAndReturnOldDocumentAsync() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("_id", 1).append("x", 1));
        FindAndReplaceOperation<Document> operation = new FindAndReplaceOperation<Document>(getNamespace(),
                WriteConcern.ACKNOWLEDGED, false, new DocumentCodec(),
                new BsonDocument("_id", new BsonInt32(1)).append("x", new BsonInt32(2)))
                .filter(new BsonDocument("x", new BsonInt32(1)));
        Document result = execute(operation, true);
        assertEquals(1, result.get("x"));
        assertEquals(2, getCollectionHelper().find().get(0).get("x"));
    }

    // NOTE: Mock-based tests for readConcern, writeConcern, collation, hint,
    // returnOriginal, upsert, projection, sort, etc. are omitted.
    // TODO: Convert mock-based tests with Mockito if needed
}
