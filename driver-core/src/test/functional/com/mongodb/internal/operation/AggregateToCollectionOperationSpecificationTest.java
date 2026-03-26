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

import com.mongodb.MongoNamespace;
import com.mongodb.OperationFunctionalSpecification;
import com.mongodb.WriteConcern;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregateToCollectionOperationSpecificationTest extends OperationFunctionalSpecification {

    @Test
    void shouldAggregateToCollectionSync() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("_id", 1).append("x", 1),
                new Document("_id", 2).append("x", 2));
        String outCollectionName = getCollectionName() + "_out";
        AggregateToCollectionOperation operation = new AggregateToCollectionOperation(
                getNamespace(),
                Collections.singletonList(new BsonDocument("$out", new BsonString(outCollectionName))),
                null, WriteConcern.ACKNOWLEDGED);

        execute(operation, false);

        MongoNamespace outNamespace = new MongoNamespace(getDatabaseName(), outCollectionName);
        List<Document> results = getCollectionHelper(outNamespace).find();
        assertEquals(2, results.size());
    }

    @Test
    void shouldAggregateToCollectionAsync() {
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document("_id", 1).append("x", 1),
                new Document("_id", 2).append("x", 2));
        String outCollectionName = getCollectionName() + "_out";
        AggregateToCollectionOperation operation = new AggregateToCollectionOperation(
                getNamespace(),
                Collections.singletonList(new BsonDocument("$out", new BsonString(outCollectionName))),
                null, WriteConcern.ACKNOWLEDGED);

        execute(operation, true);

        MongoNamespace outNamespace = new MongoNamespace(getDatabaseName(), outCollectionName);
        List<Document> results = getCollectionHelper(outNamespace).find();
        assertEquals(2, results.size());
    }

    // NOTE: Mock-based tests for readPreference, readConcern, writeConcern, collation, etc. are omitted.
    // TODO: Convert mock-based tests with Mockito if needed
}
