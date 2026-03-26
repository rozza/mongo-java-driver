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
import java.util.Collections;

import static com.mongodb.client.model.BsonHelper.toBson;
import static com.mongodb.client.model.Updates.addEachToSet;
import static com.mongodb.client.model.Updates.addToSet;
import static com.mongodb.client.model.Updates.bitwiseAnd;
import static com.mongodb.client.model.Updates.bitwiseOr;
import static com.mongodb.client.model.Updates.bitwiseXor;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.currentDate;
import static com.mongodb.client.model.Updates.currentTimestamp;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.max;
import static com.mongodb.client.model.Updates.min;
import static com.mongodb.client.model.Updates.mul;
import static com.mongodb.client.model.Updates.popFirst;
import static com.mongodb.client.model.Updates.popLast;
import static com.mongodb.client.model.Updates.pull;
import static com.mongodb.client.model.Updates.pullAll;
import static com.mongodb.client.model.Updates.pullByFilter;
import static com.mongodb.client.model.Updates.push;
import static com.mongodb.client.model.Updates.pushEach;
import static com.mongodb.client.model.Updates.rename;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.setOnInsert;
import static com.mongodb.client.model.Updates.unset;
import static org.bson.BsonDocument.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UpdatesTest {

    @Test
    void shouldRenderSet() {
        assertEquals(parse("{$set : { x : 1} }"), toBson(set("x", 1)));
        assertEquals(parse("{$set : { x : null } }"), toBson(set("x", null)));
    }

    @Test
    void shouldRenderSetOnInsert() {
        assertEquals(parse("{$setOnInsert : { x : 1} }"), toBson(setOnInsert("x", 1)));
        assertEquals(parse("{$setOnInsert : { x : null } }"), toBson(setOnInsert("x", null)));
        assertEquals(parse("{$setOnInsert : {a: 1, b: \"two\"} }"), toBson(setOnInsert(parse("{ a : 1, b: \"two\"}"))));
        assertThrows(IllegalArgumentException.class, () -> toBson(setOnInsert(null)));
    }

    @Test
    void shouldRenderUnset() {
        assertEquals(parse("{$unset : { x : \"\"} }"), toBson(unset("x")));
    }

    @Test
    void shouldRenderRename() {
        assertEquals(parse("{$rename : { x : \"y\"} }"), toBson(rename("x", "y")));
    }

    @Test
    void shouldRenderInc() {
        assertEquals(parse("{$inc : { x : 1} }"), toBson(inc("x", 1)));
        assertEquals(parse("{$inc : { x : {$numberLong : \"5\"}} }"), toBson(inc("x", 5L)));
        assertEquals(parse("{$inc : { x : 3.4} }"), toBson(inc("x", 3.4d)));
    }

    @Test
    void shouldRenderMul() {
        assertEquals(parse("{$mul : { x : 1} }"), toBson(mul("x", 1)));
        assertEquals(parse("{$mul : { x : {$numberLong : \"5\"}} }"), toBson(mul("x", 5L)));
        assertEquals(parse("{$mul : { x : 3.4} }"), toBson(mul("x", 3.4d)));
    }

    @Test
    void shouldRenderMin() {
        assertEquals(parse("{$min : { x : 42} }"), toBson(min("x", 42)));
    }

    @Test
    void shouldRenderMax() {
        assertEquals(parse("{$max : { x : 42} }"), toBson(max("x", 42)));
    }

    @Test
    void shouldRenderCurrentDate() {
        assertEquals(parse("{$currentDate : { x : true} }"), toBson(currentDate("x")));
        assertEquals(parse("{$currentDate : { x : {$type : \"timestamp\"} } }"), toBson(currentTimestamp("x")));
    }

    @Test
    void shouldRenderAddToSet() {
        assertEquals(parse("{$addToSet : { x : 1} }"), toBson(addToSet("x", 1)));
        assertEquals(parse("{$addToSet : { x : { $each : [1, 2, 3] } } }"),
                toBson(addEachToSet("x", Arrays.asList(1, 2, 3))));
    }

    @Test
    void shouldRenderPush() {
        assertEquals(parse("{$push : { x : 1} }"), toBson(push("x", 1)));
        assertEquals(parse("{$push : { x : { $each : [1, 2, 3] } } }"),
                toBson(pushEach("x", Arrays.asList(1, 2, 3), new PushOptions())));
        assertEquals(parse("{$push : { x : { $each : [89, 65], $position : 0, $slice : 3, $sort : -1 } } }"),
                toBson(pushEach("x", Arrays.asList(89, 65), new PushOptions().position(0).slice(3).sort(-1))));
    }

    @Test
    void shouldRenderPull() {
        assertEquals(parse("{$pull : { x : 1} }"), toBson(pull("x", 1)));
        assertEquals(parse("{$pull : { x : { $gte : 5 }} }"), toBson(pullByFilter(Filters.gte("x", 5))));
    }

    @Test
    void shouldRenderPullAll() {
        assertEquals(parse("{$pullAll : { x : []} }"), toBson(pullAll("x", Collections.emptyList())));
        assertEquals(parse("{$pullAll : { x : [1, 2, 3]} }"), toBson(pullAll("x", Arrays.asList(1, 2, 3))));
    }

    @Test
    void shouldRenderPop() {
        assertEquals(parse("{$pop : { x : -1} }"), toBson(popFirst("x")));
        assertEquals(parse("{$pop : { x : 1} }"), toBson(popLast("x")));
    }

    @Test
    void shouldRenderBit() {
        assertEquals(parse("{$bit : { x : {and : 5} } }"), toBson(bitwiseAnd("x", 5)));
        assertEquals(parse("{$bit : { x : {and : {$numberLong : \"5\"} } } }"), toBson(bitwiseAnd("x", 5L)));
        assertEquals(parse("{$bit : { x : {or : 5} } }"), toBson(bitwiseOr("x", 5)));
        assertEquals(parse("{$bit : { x : {or : {$numberLong : \"5\"} } } }"), toBson(bitwiseOr("x", 5L)));
        assertEquals(parse("{$bit : { x : {xor : 5} } }"), toBson(bitwiseXor("x", 5)));
        assertEquals(parse("{$bit : { x : {xor : {$numberLong : \"5\"} } } }"), toBson(bitwiseXor("x", 5L)));
    }

    @Test
    void shouldCombineUpdates() {
        assertEquals(parse("{$set : { x : 1} }"), toBson(combine(set("x", 1))));
        assertEquals(parse("{$set : { x : 1, y : 2} }"), toBson(combine(set("x", 1), set("y", 2))));
        assertEquals(parse("{$set : { x : 2} }"), toBson(combine(set("x", 1), set("x", 2))));
        assertEquals(parse("{$set : { x : 1, y : 2}, $inc : { z : 3, a : 4}}"),
                toBson(combine(set("x", 1), inc("z", 3), set("y", 2), inc("a", 4))));
    }

    @Test
    void shouldCreateStringRepresentationForSimpleUpdates() {
        assertEquals("Update{fieldName='x', operator='$set', value=1}", set("x", 1).toString());
    }

    @Test
    void shouldTestEqualsForSimpleUpdate() {
        assertTrue(setOnInsert("x", 1).equals(setOnInsert("x", 1)));
        assertTrue(rename("x", "y").equals(rename("x", "y")));
        assertTrue(inc("x", 1).equals(inc("x", 1)));
        assertTrue(min("x", 42).equals(min("x", 42)));
        assertTrue(max("x", 42).equals(max("x", 42)));
        assertTrue(currentDate("x").equals(currentDate("x")));
        assertTrue(addToSet("x", 1).equals(addToSet("x", 1)));
        assertTrue(push("x", 1).equals(push("x", 1)));
        assertTrue(pull("x", 1).equals(pull("x", 1)));
        assertTrue(popFirst("x").equals(popFirst("x")));
        assertTrue(popLast("x").equals(popLast("x")));
    }

    @Test
    void shouldTestHashCodeForSimpleUpdate() {
        assertEquals(setOnInsert("x", 1).hashCode(), setOnInsert("x", 1).hashCode());
        assertEquals(inc("x", 1).hashCode(), inc("x", 1).hashCode());
        assertEquals(popFirst("x").hashCode(), popFirst("x").hashCode());
    }

    @Test
    void shouldTestEqualsForWithEachUpdate() {
        assertTrue(addEachToSet("x", Arrays.asList(1, 2, 3)).equals(addEachToSet("x", Arrays.asList(1, 2, 3))));
    }

    @Test
    void shouldTestEqualsForPushUpdate() {
        assertTrue(pushEach("x", Arrays.asList(1, 2, 3), new PushOptions())
                .equals(pushEach("x", Arrays.asList(1, 2, 3), new PushOptions())));
    }

    @Test
    void shouldTestEqualsForPullAllUpdate() {
        assertTrue(pullAll("x", Collections.emptyList()).equals(pullAll("x", Collections.emptyList())));
        assertTrue(pullAll("x", Arrays.asList(1, 2, 3)).equals(pullAll("x", Arrays.asList(1, 2, 3))));
    }

    @Test
    void shouldTestEqualsForCompositeUpdate() {
        assertTrue(combine(set("x", 1)).equals(combine(set("x", 1))));
        assertTrue(combine(set("x", 1), set("y", 2)).equals(combine(set("x", 1), set("y", 2))));
    }

    @Test
    void shouldTestHashCodeForCompositeUpdate() {
        assertEquals(combine(set("x", 1)).hashCode(), combine(set("x", 1)).hashCode());
    }
}
