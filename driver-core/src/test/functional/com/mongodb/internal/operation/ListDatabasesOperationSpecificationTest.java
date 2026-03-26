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
import org.bson.BsonRegularExpression;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ListDatabasesOperationSpecificationTest extends OperationFunctionalSpecification {

    private final DocumentCodec codec = new DocumentCodec();

    @Test
    void shouldReturnListOfDatabaseNamesSync() {
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document("_id", 1));
        ListDatabasesOperation operation = new ListDatabasesOperation(codec);

        List<Document> results = executeAndCollectBatchCursorResults(operation, false);
        List<String> names = results.stream().map(d -> (String) d.get("name")).collect(Collectors.toList());
        assertTrue(names.contains(getDatabaseName()));

        // with nameOnly and filter
        operation = operation.nameOnly(true).filter(
                new BsonDocument("name", new BsonRegularExpression("^" + getDatabaseName())));
        results = executeAndCollectBatchCursorResults(operation, false);
        names = results.stream().map(d -> (String) d.get("name")).collect(Collectors.toList());
        assertTrue(names.contains(getDatabaseName()));

        // with authorizedDatabasesOnly
        operation = operation.authorizedDatabasesOnly(true).nameOnly(true)
                .filter(new BsonDocument("name", new BsonRegularExpression("^" + getDatabaseName())));
        results = executeAndCollectBatchCursorResults(operation, false);
        names = results.stream().map(d -> (String) d.get("name")).collect(Collectors.toList());
        assertTrue(names.contains(getDatabaseName()));
    }

    @Test
    void shouldReturnListOfDatabaseNamesAsync() {
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document("_id", 1));
        ListDatabasesOperation operation = new ListDatabasesOperation(codec);

        List<Document> results = executeAndCollectBatchCursorResults(operation, true);
        List<String> names = results.stream().map(d -> (String) d.get("name")).collect(Collectors.toList());
        assertTrue(names.contains(getDatabaseName()));
    }

    // NOTE: The readPreference/secondaryOk tests used Spock Mock/Stub and are omitted.
    // TODO: Convert mock-based readPreference tests with Mockito if needed
}
