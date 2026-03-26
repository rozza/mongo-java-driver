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
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.elemMatch;
import static com.mongodb.client.model.Projections.exclude;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Projections.meta;
import static com.mongodb.client.model.Projections.metaTextScore;
import static com.mongodb.client.model.Projections.slice;
import static org.bson.BsonDocument.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ProjectionsTest {

    @Test
    void testInclude() {
        assertEquals(parse("{x : 1}"), toBson(include("x")));
        assertEquals(parse("{x : 1, y : 1}"), toBson(include("x", "y")));
        assertEquals(parse("{x : 1, y : 1}"), toBson(include(Arrays.asList("x", "y"))));
        assertEquals(parse("{y : 1, x : 1}"), toBson(include(Arrays.asList("x", "y", "x"))));
    }

    @Test
    void testExclude() {
        assertEquals(parse("{x : 0}"), toBson(exclude("x")));
        assertEquals(parse("{x : 0, y : 0}"), toBson(exclude("x", "y")));
        assertEquals(parse("{x : 0, y : 0}"), toBson(exclude(Arrays.asList("x", "y"))));
    }

    @Test
    void testExcludeId() {
        assertEquals(parse("{_id : 0}"), toBson(excludeId()));
    }

    @Test
    void testFirstElem() {
        assertEquals(parse("{\"x.$\" : 1}"), toBson(elemMatch("x")));
    }

    @Test
    void testElemMatch() {
        assertEquals(parse("{x : {$elemMatch : {$and: [{y : 1}, {z : 2}]}}}"),
                toBson(elemMatch("x", and(eq("y", 1), eq("z", 2)))));
    }

    @Test
    void testSlice() {
        assertEquals(parse("{x : {$slice : 5}}"), toBson(slice("x", 5)));
        assertEquals(parse("{x : {$slice : [5, 10]}}"), toBson(slice("x", 5, 10)));
    }

    @Test
    void testMeta() {
        assertEquals(parse("{x : {$meta : \"textScore\"}}"), toBson(meta("x", "textScore")));
        assertEquals(parse("{x : {$meta : \"recordId\"}}"), toBson(meta("x", "recordId")));
    }

    @Test
    void testMetaTextScore() {
        assertEquals(parse("{x : {$meta : \"textScore\"}}"), toBson(metaTextScore("x")));
    }

    @Test
    void testComputed() {
        assertEquals(parse("{c : \"$y\"}"), toBson(computed("c", "$y")));
    }

    @Test
    void testCombineFields() {
        assertEquals(parse("{x : 1, y : 1, _id : 0}"), toBson(fields(include("x", "y"), exclude("_id"))));
        assertEquals(parse("{x : 1, y : 1, _id : 0}"), toBson(fields(Arrays.asList(include("x", "y"), exclude("_id")))));
        assertEquals(parse("{y : 1, x : 0}"), toBson(fields(include("x", "y"), exclude("x"))));
    }

    @Test
    void shouldCreateStringRepresentationForIncludeAndExclude() {
        assertEquals("{\"y\": 1, \"x\": 1}", include(Arrays.asList("x", "y", "x")).toString());
        assertEquals("{\"y\": 0, \"x\": 0}", exclude(Arrays.asList("x", "y", "x")).toString());
        assertEquals("{\"_id\": 0}", excludeId().toString());
    }

    @Test
    void shouldCreateStringRepresentationForComputed() {
        assertEquals("Expression{name='c', expression=$y}", computed("c", "$y").toString());
    }

    @Test
    void shouldCreateStringRepresentationForElemMatchWithFilter() {
        assertEquals("ElemMatch Projection{fieldName='x', filter=And Filter{filters=["
                        + "Filter{fieldName='y', value=1}, Filter{fieldName='z', value=2}]}}",
                elemMatch("x", and(eq("y", 1), eq("z", 2))).toString());
    }

    @Test
    void shouldCreateStringRepresentationForFields() {
        assertEquals("Projections{projections=[{\"x\": 1, \"y\": 1}, {\"_id\": 0}]}",
                fields(include("x", "y"), exclude("_id")).toString());
    }
}
