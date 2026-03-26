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
import java.util.Collections;
import java.util.List;

import static com.mongodb.client.model.Updates.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ArrayUpdatesFunctionalSpecificationTest extends OperationFunctionalSpecification {
    private final Document a = new Document("_id", 1).append("x", Arrays.asList(1, 2, 3));

    @BeforeEach
    public void setUp() {
        super.setUp();
        getCollectionHelper().insertDocuments(a);
    }

    private List<Document> find() {
        return find(new Document("_id", 1));
    }

    private List<Document> find(final Bson filter) {
        return getCollectionHelper().find(filter);
    }

    private void updateOne(final Bson update) {
        getCollectionHelper().updateOne(new Document("_id", 1), update);
    }

    @Test
    void addToSetTest() {
        updateOne(addToSet("x", 4));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", Arrays.asList(1, 2, 3, 4))), find());

        updateOne(addToSet("x", 4));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", Arrays.asList(1, 2, 3, 4))), find());

        updateOne(addEachToSet("x", Arrays.asList(4, 5, 6)));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", Arrays.asList(1, 2, 3, 4, 5, 6))), find());
    }

    @Test
    void pushTest() {
        updateOne(push("x", 4));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", Arrays.asList(1, 2, 3, 4))), find());

        updateOne(push("x", 4));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", Arrays.asList(1, 2, 3, 4, 4))), find());
    }

    @Test
    void pushWithEach() {
        updateOne(pushEach("x", Arrays.asList(4, 4, 4, 5, 6), new PushOptions()));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", Arrays.asList(1, 2, 3, 4, 4, 4, 5, 6))), find());

        updateOne(pushEach("x", Arrays.asList(4, 5, 6), new PushOptions().position(0).slice(5)));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", Arrays.asList(4, 5, 6, 1, 2))), find());

        updateOne(pushEach("x", Collections.emptyList(), new PushOptions().sort(-1)));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", Arrays.asList(6, 5, 4, 2, 1))), find());

        updateOne(combine(unset("x"), pushEach("scores",
                Arrays.asList(new Document("score", 89), new Document("score", 65)),
                new PushOptions().sortDocument(new Document("score", 1)))));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("scores",
                Arrays.asList(new Document("score", 65), new Document("score", 89)))), find());
    }

    @Test
    void pullTest() {
        updateOne(pull("x", 1));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", Arrays.asList(2, 3))), find());
    }

    @Test
    void pullByFilterTest() {
        updateOne(pullByFilter(Filters.gt("x", 1)));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", Collections.singletonList(1))), find());
    }

    @Test
    void pullAllTest() {
        updateOne(pullAll("x", Arrays.asList(2, 3)));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", Collections.singletonList(1))), find());
    }

    @Test
    void popFirstTest() {
        updateOne(popFirst("x"));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", Arrays.asList(2, 3))), find());
    }

    @Test
    void popLastTest() {
        updateOne(popLast("x"));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", Arrays.asList(1, 2))), find());
    }
}
