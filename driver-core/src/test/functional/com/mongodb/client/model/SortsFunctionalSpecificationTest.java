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

package com.mongodb.client.model;

import com.mongodb.OperationFunctionalSpecification;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Sorts.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SortsFunctionalSpecificationTest extends OperationFunctionalSpecification {
    private final Document a = new Document("_id", 1).append("x", 1).append("y", "bear");
    private final Document b = new Document("_id", 2).append("x", 1).append("y", "albatross");
    private final Document c = new Document("_id", 3).append("x", 2).append("y", "cat");

    @BeforeEach
    public void setUp() {
        super.setUp();
        getCollectionHelper().insertDocuments(a, b, c);
    }

    private List<Document> find(final Bson sort) {
        return getCollectionHelper().find(new Document(), sort);
    }

    private List<Document> find(final Bson filter, final Bson sort, final Bson projection) {
        return getCollectionHelper().find(filter, sort, projection);
    }

    @Test
    void ascendingTest() {
        assertEquals(Arrays.asList(a, b, c), find(ascending("_id")));
        assertEquals(Arrays.asList(b, a, c), find(ascending("y")));
        assertEquals(Arrays.asList(b, a, c), find(ascending("x", "y")));
    }

    @Test
    void descendingTest() {
        assertEquals(Arrays.asList(c, b, a), find(descending("_id")));
        assertEquals(Arrays.asList(c, a, b), find(descending("y")));
        assertEquals(Arrays.asList(c, a, b), find(descending("x", "y")));
    }

    @Test
    void metaTextScoreTest() {
        getCollectionHelper().createIndex(new Document("y", "text"));
        List<Document> results = find(new Document("$text", new Document("$search", "bear")),
                metaTextScore("score"),
                new Document("score", new Document("$meta", "textScore")));
        assertTrue(results.stream().allMatch(d -> d.containsKey("score")));
    }

    @Test
    void orderByTest() {
        assertEquals(Arrays.asList(a, b, c), find(orderBy(Arrays.asList(ascending("x"), descending("y")))));
        assertEquals(Arrays.asList(c, a, b), find(orderBy(ascending("x"), descending("y"), descending("x"))));
    }
}
