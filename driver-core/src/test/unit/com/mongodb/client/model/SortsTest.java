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

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static com.mongodb.client.model.BsonHelper.toBson;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Sorts.metaTextScore;
import static com.mongodb.client.model.Sorts.orderBy;
import static org.bson.BsonDocument.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SortsTest {

    @Test
    void testAscending() {
        assertEquals(parse("{x : 1}"), toBson(ascending("x")));
        assertEquals(parse("{x : 1, y : 1}"), toBson(ascending("x", "y")));
        assertEquals(parse("{x : 1, y : 1}"), toBson(ascending(Arrays.asList("x", "y"))));
    }

    @Test
    void testDescending() {
        assertEquals(parse("{x : -1}"), toBson(descending("x")));
        assertEquals(parse("{x : -1, y : -1}"), toBson(descending("x", "y")));
        assertEquals(parse("{x : -1, y : -1}"), toBson(descending(Arrays.asList("x", "y"))));
    }

    @Test
    void testMetaTextScore() {
        assertEquals(parse("{x : {$meta : \"textScore\"}}"), toBson(metaTextScore("x")));
    }

    @Test
    void testOrderBy() {
        assertEquals(parse("{x : 1, y : -1}"), toBson(orderBy(Arrays.asList(ascending("x"), descending("y")))));
        assertEquals(parse("{x : 1, y : -1}"), toBson(orderBy(ascending("x"), descending("y"))));
        assertEquals(parse("{y : -1, x : -1}"), toBson(orderBy(ascending("x"), descending("y"), descending("x"))));
        assertEquals(parse("{x : 1, y : 1, a : -1, b : -1}"),
                toBson(orderBy(ascending("x", "y"), descending("a", "b"))));
    }

    @Test
    void shouldCreateStringRepresentationForSimpleSorts() {
        assertEquals("{\"x\": 1, \"y\": 1}", ascending("x", "y").toString());
        assertEquals("{\"x\": -1, \"y\": -1}", descending("x", "y").toString());
        assertEquals("{\"x\": {\"$meta\": \"textScore\"}}", metaTextScore("x").toString());
    }

    @Test
    void shouldCreateStringRepresentationForCompoundSorts() {
        assertEquals("Compound Sort{sorts=[{\"x\": 1, \"y\": 1}, {\"a\": -1, \"b\": -1}]}",
                orderBy(ascending("x", "y"), descending("a", "b")).toString());
    }

    @Test
    void shouldTestEqualsForCompoundSort() {
        assertTrue(orderBy(Arrays.asList(ascending("x"), descending("y")))
                .equals(orderBy(Arrays.asList(ascending("x"), descending("y")))));
        assertTrue(orderBy(ascending("x"), descending("y"))
                .equals(orderBy(ascending("x"), descending("y"))));
    }

    @Test
    void shouldTestHashCodeForCompoundSort() {
        assertEquals(orderBy(Arrays.asList(ascending("x"), descending("y"))).hashCode(),
                orderBy(Arrays.asList(ascending("x"), descending("y"))).hashCode());
    }
}
