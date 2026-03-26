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
import static com.mongodb.client.model.Indexes.ascending;
import static com.mongodb.client.model.Indexes.compoundIndex;
import static com.mongodb.client.model.Indexes.descending;
import static com.mongodb.client.model.Indexes.geo2d;
import static com.mongodb.client.model.Indexes.geo2dsphere;
import static com.mongodb.client.model.Indexes.hashed;
import static com.mongodb.client.model.Indexes.text;
import static org.bson.BsonDocument.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexesTest {

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
    void testGeo2dsphere() {
        assertEquals(parse("{x : \"2dsphere\"}"), toBson(geo2dsphere("x")));
        assertEquals(parse("{x : \"2dsphere\", y : \"2dsphere\"}"), toBson(geo2dsphere("x", "y")));
    }

    @Test
    void testGeo2d() {
        assertEquals(parse("{x : \"2d\"}"), toBson(geo2d("x")));
    }

    @Test
    void testText() {
        assertEquals(parse("{x : \"text\"}"), toBson(text("x")));
        assertEquals(parse("{ \"$**\" : \"text\"}"), toBson(text()));
    }

    @Test
    void testHashed() {
        assertEquals(parse("{x : \"hashed\"}"), toBson(hashed("x")));
    }

    @Test
    void testCompoundIndex() {
        assertEquals(parse("{x : 1, y : -1}"), toBson(compoundIndex(Arrays.asList(ascending("x"), descending("y")))));
        assertEquals(parse("{x : 1, y : -1}"), toBson(compoundIndex(ascending("x"), descending("y"))));
        assertEquals(parse("{y : -1, x : -1}"), toBson(compoundIndex(ascending("x"), descending("y"), descending("x"))));
        assertEquals(parse("{x : 1, y : 1, a : -1, b : -1}"),
                toBson(compoundIndex(ascending("x", "y"), descending("a", "b"))));
    }

    @Test
    void shouldTestEqualsOnCompoundIndex() {
        assertTrue(compoundIndex(Arrays.asList(ascending("x"), descending("y")))
                .equals(compoundIndex(Arrays.asList(ascending("x"), descending("y")))));
    }

    @Test
    void shouldTestHashCodeOnCompoundIndex() {
        assertEquals(compoundIndex(Arrays.asList(ascending("x"), descending("y"))).hashCode(),
                compoundIndex(Arrays.asList(ascending("x"), descending("y"))).hashCode());
    }
}
