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
import org.bson.codecs.StringCodec;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DistinctOperationSpecificationTest extends OperationFunctionalSpecification {

    @Test
    void shouldReturnDistinctValuesSync() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("name", "Pete"), new Document("name", "Sam"),
                new Document("name", "Pete"), new Document("name", "Jill"));
        DistinctOperation<String> operation = new DistinctOperation<>(getNamespace(), "name", new StringCodec());

        List<String> results = executeAndCollectBatchCursorResults(operation, false);
        assertEquals(3, results.size());
        assertTrue(results.containsAll(Arrays.asList("Pete", "Sam", "Jill")));
    }

    @Test
    void shouldReturnDistinctValuesAsync() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("name", "Pete"), new Document("name", "Sam"),
                new Document("name", "Pete"), new Document("name", "Jill"));
        DistinctOperation<String> operation = new DistinctOperation<>(getNamespace(), "name", new StringCodec());

        List<String> results = executeAndCollectBatchCursorResults(operation, true);
        assertEquals(3, results.size());
        assertTrue(results.containsAll(Arrays.asList("Pete", "Sam", "Jill")));
    }

    @Test
    void shouldReturnDistinctValuesWithFilter() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("name", "Pete"), new Document("name", "Sam"),
                new Document("name", "Pete"), new Document("name", "Jill"));
        DistinctOperation<String> operation = new DistinctOperation<>(getNamespace(), "name", new StringCodec())
                .filter(new BsonDocument("name", new BsonString("Pete")));

        List<String> results = executeAndCollectBatchCursorResults(operation, false);
        assertEquals(1, results.size());
        assertEquals("Pete", results.get(0));
    }

    // NOTE: Mock-based tests for readPreference, readConcern, collation etc. are omitted.
    // TODO: Convert mock-based tests with Mockito if needed
}
