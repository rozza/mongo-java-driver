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
import org.bson.BsonJavaScript;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MapReduceWithInlineResultsOperationSpecificationTest extends OperationFunctionalSpecification {

    @Test
    void shouldReturnEmptyResultForEmptyCollection() {
        MapReduceWithInlineResultsOperation<Document> operation = new MapReduceWithInlineResultsOperation<>(
                getNamespace(),
                new BsonJavaScript("function() { emit(this.x, 1); }"),
                new BsonJavaScript("function(key, values) { return Array.sum(values); }"),
                new DocumentCodec());
        List<Document> results = executeAndCollectBatchCursorResults(operation, false);
        assertEquals(0, results.size());
    }

    @Test
    void shouldReturnResultsForCollectionSync() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("x", 1), new Document("x", 1), new Document("x", 2));
        MapReduceWithInlineResultsOperation<Document> operation = new MapReduceWithInlineResultsOperation<>(
                getNamespace(),
                new BsonJavaScript("function() { emit(this.x, 1); }"),
                new BsonJavaScript("function(key, values) { return Array.sum(values); }"),
                new DocumentCodec());
        List<Document> results = executeAndCollectBatchCursorResults(operation, false);
        assertEquals(2, results.size());
    }

    @Test
    void shouldReturnResultsForCollectionAsync() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("x", 1), new Document("x", 1), new Document("x", 2));
        MapReduceWithInlineResultsOperation<Document> operation = new MapReduceWithInlineResultsOperation<>(
                getNamespace(),
                new BsonJavaScript("function() { emit(this.x, 1); }"),
                new BsonJavaScript("function(key, values) { return Array.sum(values); }"),
                new DocumentCodec());
        List<Document> results = executeAndCollectBatchCursorResults(operation, true);
        assertEquals(2, results.size());
    }

    // NOTE: Mock-based tests for readPreference, readConcern, collation, etc. are omitted.
    // TODO: Convert mock-based tests with Mockito if needed
}
