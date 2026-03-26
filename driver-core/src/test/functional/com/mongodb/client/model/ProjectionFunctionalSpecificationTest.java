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

import com.mongodb.MongoQueryException;
import com.mongodb.OperationFunctionalSpecification;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProjectionFunctionalSpecificationTest extends OperationFunctionalSpecification {
    private final Document a = new Document("_id", 1).append("x", "coffee")
            .append("y", Arrays.asList(new Document("a", 1).append("b", 2),
                    new Document("a", 2).append("b", 3),
                    new Document("a", 3).append("b", 4)));
    private final Document aYSlice1 = new Document("_id", 1).append("x", "coffee")
            .append("y", Collections.singletonList(new Document("a", 1).append("b", 2)));
    private final Document aYSlice12 = new Document("_id", 1).append("x", "coffee")
            .append("y", Arrays.asList(new Document("a", 2).append("b", 3),
                    new Document("a", 3).append("b", 4)));
    private final Document aNoY = new Document("_id", 1).append("x", "coffee");
    private final Document aId = new Document("_id", 1);
    private final Document aNoId = new Document().append("x", "coffee")
            .append("y", Arrays.asList(new Document("a", 1).append("b", 2),
                    new Document("a", 2).append("b", 3),
                    new Document("a", 3).append("b", 4)));
    private final Document aWithScore = new Document("_id", 1).append("x", "coffee")
            .append("y", Arrays.asList(new Document("a", 1).append("b", 2),
                    new Document("a", 2).append("b", 3),
                    new Document("a", 3).append("b", 4)))
            .append("score", 1.0);

    @BeforeEach
    public void setUp() {
        super.setUp();
        getCollectionHelper().insertDocuments(a);
    }

    private List<Document> find(final Bson projection) {
        return getCollectionHelper().find(null, null, projection);
    }

    private List<Document> find(final Bson filter, final Bson projection) {
        return getCollectionHelper().find(filter, null, projection);
    }

    @Test
    void includeTest() {
        assertEquals(Collections.singletonList(aNoY), find(include("x")));
        assertEquals(Collections.singletonList(a), find(include("x", "y")));
        assertEquals(Collections.singletonList(a), find(include(Arrays.asList("x", "y", "x"))));
    }

    @Test
    void excludeTest() {
        assertEquals(Collections.singletonList(aNoY), find(exclude("y")));
        assertEquals(Collections.singletonList(aId), find(exclude("x", "y")));
        assertEquals(Collections.singletonList(aId), find(exclude(Arrays.asList("x", "y", "x"))));
    }

    @Test
    void excludeIdHelperTest() {
        assertEquals(Collections.singletonList(aNoId), find(excludeId()));
    }

    @Test
    void firstElemTest() {
        assertEquals(Collections.singletonList(aYSlice1),
                find(new Document("y", new Document("$elemMatch", new Document("a", 1).append("b", 2))),
                        fields(include("x"), elemMatch("y"))));
    }

    @Test
    void elemMatchTest() {
        assertEquals(Collections.singletonList(aYSlice1),
                find(fields(include("x"), Projections.elemMatch("y", and(eq("a", 1), eq("b", 2))))));
    }

    @Test
    void sliceTest() {
        assertEquals(Collections.singletonList(aYSlice1), find(slice("y", 1)));
        assertEquals(Collections.singletonList(aYSlice12), find(slice("y", 1, 2)));
    }

    @Test
    void metaTextScoreTest() {
        getCollectionHelper().createIndex(new Document("x", "text"));
        assertEquals(Collections.singletonList(aWithScore), find(text("coffee"), metaTextScore("score")));
    }

    @Test
    void combineFieldsTest() {
        assertEquals(Collections.singletonList(aNoId), find(fields(include("x", "y"), exclude("_id"))));
    }

    @Test
    void combineFieldsIllegallyTest() {
        assertThrows(MongoQueryException.class, () -> find(fields(include("x", "y"), exclude("y"))));
    }
}
