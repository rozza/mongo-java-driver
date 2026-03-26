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

import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.Position;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.regex.Pattern;

import static com.mongodb.client.model.BsonHelper.toBson;
import static com.mongodb.client.model.Filters.all;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.bitsAllClear;
import static com.mongodb.client.model.Filters.bitsAllSet;
import static com.mongodb.client.model.Filters.bitsAnyClear;
import static com.mongodb.client.model.Filters.bitsAnySet;
import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.empty;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.expr;
import static com.mongodb.client.model.Filters.geoIntersects;
import static com.mongodb.client.model.Filters.geoWithin;
import static com.mongodb.client.model.Filters.geoWithinBox;
import static com.mongodb.client.model.Filters.geoWithinCenter;
import static com.mongodb.client.model.Filters.geoWithinCenterSphere;
import static com.mongodb.client.model.Filters.geoWithinPolygon;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.jsonSchema;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.mod;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Filters.near;
import static com.mongodb.client.model.Filters.nearSphere;
import static com.mongodb.client.model.Filters.nin;
import static com.mongodb.client.model.Filters.nor;
import static com.mongodb.client.model.Filters.not;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Filters.size;
import static com.mongodb.client.model.Filters.text;
import static com.mongodb.client.model.Filters.type;
import static com.mongodb.client.model.Filters.where;
import static java.util.Arrays.asList;
import static org.bson.BsonDocument.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FiltersTest {

    @Test
    void eqShouldRenderWithoutDollarEq() {
        assertEquals(parse("{x : 1}"), toBson(eq("x", 1)));
        assertEquals(parse("{x : null}"), toBson(eq("x", null)));
        assertEquals(parse("{_id : 1}"), toBson(eq(1)));
    }

    @Test
    void shouldRenderNe() {
        assertEquals(parse("{x : {$ne : 1} }"), toBson(ne("x", 1)));
        assertEquals(parse("{x : {$ne : null} }"), toBson(ne("x", null)));
    }

    @Test
    void shouldRenderNot() {
        assertEquals(parse("{x : {$not: {$eq: 1}}}"), toBson(not(eq("x", 1))));
        assertEquals(parse("{x : {$not: {$gt: 1}}}"), toBson(not(gt("x", 1))));
        assertEquals(parse("{x : {$not: /^p.*/}}"), toBson(not(regex("x", "^p.*"))));
        assertEquals(parse("{$not: {$and: [{x: {$gt: 1}}, {y: 20}]}}"), toBson(not(and(gt("x", 1), eq("y", 20)))));
    }

    @Test
    void shouldRenderNor() {
        assertEquals(parse("{$nor : [{price: 1}]}"), toBson(nor(eq("price", 1))));
        assertEquals(parse("{$nor : [{price: 1}, {sale: true}]}"), toBson(nor(eq("price", 1), eq("sale", true))));
    }

    @Test
    void shouldRenderGt() {
        assertEquals(parse("{x : {$gt : 1} }"), toBson(gt("x", 1)));
    }

    @Test
    void shouldRenderLt() {
        assertEquals(parse("{x : {$lt : 1} }"), toBson(lt("x", 1)));
    }

    @Test
    void shouldRenderGte() {
        assertEquals(parse("{x : {$gte : 1} }"), toBson(gte("x", 1)));
    }

    @Test
    void shouldRenderLte() {
        assertEquals(parse("{x : {$lte : 1} }"), toBson(lte("x", 1)));
    }

    @Test
    void shouldRenderExists() {
        assertEquals(parse("{x : {$exists : true} }"), toBson(exists("x")));
        assertEquals(parse("{x : {$exists : false} }"), toBson(exists("x", false)));
    }

    @Test
    void orShouldRenderEmptyOrUsingDollarOr() {
        assertEquals(parse("{$or : []}"), toBson(or(Collections.emptyList())));
        assertEquals(parse("{$or : []}"), toBson(or()));
    }

    @Test
    void shouldRenderOr() {
        assertEquals(parse("{$or : [{x : 1}, {y : 2}]}"), toBson(or(asList(eq("x", 1), eq("y", 2)))));
        assertEquals(parse("{$or : [{x : 1}, {y : 2}]}"), toBson(or(eq("x", 1), eq("y", 2))));
    }

    @Test
    void andShouldRenderEmptyAndUsingDollarAnd() {
        assertEquals(parse("{$and : []}"), toBson(and(Collections.emptyList())));
        assertEquals(parse("{$and : []}"), toBson(and()));
    }

    @Test
    void andShouldRenderUsingDollarAnd() {
        assertEquals(parse("{$and: [{x : 1}, {y : 2}]}"), toBson(and(asList(eq("x", 1), eq("y", 2)))));
        assertEquals(parse("{$and: [{x : 1}, {y : 2}]}"), toBson(and(eq("x", 1), eq("y", 2))));
    }

    @Test
    void andShouldRenderDollarAndWithClashingKeys() {
        assertEquals(parse("{$and: [{a: 1}, {a: 2}]}"), toBson(and(asList(eq("a", 1), eq("a", 2)))));
    }

    @Test
    void andShouldNotFlattenNested() {
        assertEquals(parse("{$and: [{$and: [{a : 1}, {b : 2}]}, {c : 3}]}"),
                toBson(and(asList(and(asList(eq("a", 1), eq("b", 2))), eq("c", 3)))));
    }

    @Test
    void shouldRenderAll() {
        assertEquals(parse("{a : {$all : [1, 2, 3]} }"), toBson(all("a", asList(1, 2, 3))));
        assertEquals(parse("{a : {$all : [1, 2, 3]} }"), toBson(all("a", 1, 2, 3)));
    }

    @Test
    void shouldRenderElemMatch() {
        assertEquals(parse("{results : {$elemMatch : {$gte: 80, $lt: 85}}}"),
                toBson(elemMatch("results", new BsonDocument("$gte", new BsonInt32(80)).append("$lt", new BsonInt32(85)))));
    }

    @Test
    void shouldRenderIn() {
        assertEquals(parse("{a : {$in : [1, 2, 3]} }"), toBson(Filters.in("a", asList(1, 2, 3))));
        assertEquals(parse("{a : {$in : [1, 2, 3]} }"), toBson(Filters.in("a", 1, 2, 3)));
    }

    @Test
    void shouldRenderNin() {
        assertEquals(parse("{a : {$nin : [1, 2, 3]} }"), toBson(nin("a", asList(1, 2, 3))));
        assertEquals(parse("{a : {$nin : [1, 2, 3]} }"), toBson(nin("a", 1, 2, 3)));
    }

    @Test
    void shouldRenderMod() {
        assertEquals(new BsonDocument("a", new BsonDocument("$mod", new BsonArray(asList(new BsonInt64(100), new BsonInt64(7))))),
                toBson(mod("a", 100, 7)));
    }

    @Test
    void shouldRenderSize() {
        assertEquals(parse("{a : {$size : 13} }"), toBson(size("a", 13)));
    }

    @Test
    void shouldRenderBitsAllClear() {
        assertEquals(parse("{a : {$bitsAllClear : { \"$numberLong\" : \"13\" }} }"), toBson(bitsAllClear("a", 13)));
    }

    @Test
    void shouldRenderBitsAllSet() {
        assertEquals(parse("{a : {$bitsAllSet : { \"$numberLong\" : \"13\" }} }"), toBson(bitsAllSet("a", 13)));
    }

    @Test
    void shouldRenderBitsAnyClear() {
        assertEquals(parse("{a : {$bitsAnyClear : { \"$numberLong\" : \"13\" }} }"), toBson(bitsAnyClear("a", 13)));
    }

    @Test
    void shouldRenderBitsAnySet() {
        assertEquals(parse("{a : {$bitsAnySet : { \"$numberLong\" : \"13\" }} }"), toBson(bitsAnySet("a", 13)));
    }

    @Test
    void shouldRenderType() {
        assertEquals(parse("{a : {$type : 4} }"), toBson(type("a", BsonType.ARRAY)));
        assertEquals(parse("{a : {$type : \"number\"} }"), toBson(type("a", "number")));
    }

    @Test
    void shouldRenderText() {
        assertEquals(parse("{$text: {$search: \"mongoDB for GIANT ideas\"} }"), toBson(text("mongoDB for GIANT ideas")));
        assertEquals(parse("{$text : {$search : \"mongoDB for GIANT ideas\", $language : \"english\"} }"),
                toBson(text("mongoDB for GIANT ideas", new TextSearchOptions().language("english"))));
    }

    @Test
    void shouldRenderRegex() {
        assertEquals(parse("{name : {$regex : \"acme.*corp\", $options : \"\"}}"), toBson(regex("name", "acme.*corp")));
        assertEquals(parse("{name : {$regex : \"acme.*corp\", $options : \"si\"}}"), toBson(regex("name", "acme.*corp", "si")));
        assertEquals(parse("{name : {$regex : \"acme.*corp\", $options : \"\"}}"),
                toBson(regex("name", Pattern.compile("acme.*corp"))));
    }

    @Test
    void shouldRenderWhere() {
        assertEquals(parse("{$where: \"this.credits == this.debits\"}"), toBson(where("this.credits == this.debits")));
    }

    @Test
    void shouldRenderExpr() {
        assertEquals(parse("{$expr: { $gt: [ \"$spent\" , \"$budget\" ] } }"),
                toBson(expr(new BsonDocument("$gt", new BsonArray(asList(new BsonString("$spent"), new BsonString("$budget")))))));
    }

    @Test
    void shouldRenderGeoWithin() {
        Polygon polygon = new Polygon(asList(new Position(asList(40.0d, 18.0d)),
                new Position(asList(40.0d, 19.0d)),
                new Position(asList(41.0d, 19.0d)),
                new Position(asList(40.0d, 18.0d))));
        assertEquals(parse("{loc: {$geoWithin: {$geometry: {type: \"Polygon\","
                        + " coordinates: [[[40.0, 18.0], [40.0, 19.0], [41.0, 19.0], [40.0, 18.0]]]}}}}"),
                toBson(geoWithin("loc", polygon)));
    }

    @Test
    void shouldRenderGeoWithinWithBox() {
        assertEquals(parse("{loc: {$geoWithin: {$box: [[1.0, 2.0], [3.0, 4.0]]}}}"),
                toBson(geoWithinBox("loc", 1d, 2d, 3d, 4d)));
    }

    @Test
    void shouldRenderGeoWithinWithPolygon() {
        assertEquals(parse("{loc: {$geoWithin: {$polygon: [[0.0, 0.0], [3.0, 6.0], [6.0, 0.0]]}}}"),
                toBson(geoWithinPolygon("loc", asList(asList(0d, 0d), asList(3d, 6d), asList(6d, 0d)))));
    }

    @Test
    void shouldRenderGeoWithinWithCenter() {
        assertEquals(parse("{ loc: { $geoWithin: { $center: [ [-74.0, 40.74], 10.0 ] } } }"),
                toBson(geoWithinCenter("loc", -74d, 40.74d, 10d)));
    }

    @Test
    void shouldRenderGeoWithinWithCenterSphere() {
        assertEquals(parse("{loc: {$geoWithin: {$centerSphere: [[-74.0, 40.74], 10.0]}}}"),
                toBson(geoWithinCenterSphere("loc", -74d, 40.74d, 10d)));
    }

    @Test
    void shouldRenderGeoIntersects() {
        Polygon polygon = new Polygon(asList(new Position(asList(40.0d, 18.0d)),
                new Position(asList(40.0d, 19.0d)),
                new Position(asList(41.0d, 19.0d)),
                new Position(asList(40.0d, 18.0d))));
        assertEquals(parse("{loc: {$geoIntersects: {$geometry: {type: \"Polygon\","
                        + " coordinates: [[[40.0, 18.0], [40.0, 19.0], [41.0, 19.0], [40.0, 18.0]]]}}}}"),
                toBson(geoIntersects("loc", polygon)));
    }

    @Test
    void shouldRenderNear() {
        Point point = new Point(new Position(-73.9667, 40.78));
        assertEquals(parse("{loc : {$near: {$geometry: {type : \"Point\", coordinates : [ -73.9667, 40.78 ]},"
                        + " $maxDistance: 5000.0, $minDistance: 1000.0}}}"),
                toBson(near("loc", point, 5000d, 1000d)));
        assertEquals(parse("{loc : {$near: [-73.9667, 40.78], $maxDistance: 5000.0, $minDistance: 1000.0}}"),
                toBson(near("loc", -73.9667, 40.78, 5000d, 1000d)));
    }

    @Test
    void shouldRenderNearSphere() {
        Point point = new Point(new Position(-73.9667, 40.78));
        assertEquals(parse("{loc : {$nearSphere: {$geometry: {type : \"Point\", coordinates : [ -73.9667, 40.78 ]},"
                        + " $maxDistance: 5000.0, $minDistance: 1000.0}}}"),
                toBson(nearSphere("loc", point, 5000d, 1000d)));
    }

    @Test
    void shouldRenderJsonSchema() {
        assertEquals(parse("{$jsonSchema : {bsonType : \"object\"}}"),
                toBson(jsonSchema(new BsonDocument("bsonType", new BsonString("object")))));
    }

    @Test
    void shouldRenderAnEmptyDocument() {
        assertEquals(parse("{}"), toBson(empty()));
    }

    @Test
    void shouldRenderWithIterableValue() {
        assertEquals(parse("{x : {}}"), toBson(eq("x", new Document())));
        assertEquals(parse("{x : [1, 2, 3]}"), toBson(eq("x", asList(1, 2, 3))));
    }

    @Test
    void shouldCreateStringRepresentationForSimpleFilter() {
        assertEquals("Filter{fieldName='x', value=1}", eq("x", 1).toString());
    }

    @Test
    void shouldTestEqualsForSimpleEncodingFilter() {
        assertTrue(eq("x", 1).equals(eq("x", 1)));
        assertNotEquals(eq("x", 1), ne("x", 1));
        assertNotEquals(eq("x", 1), eq("x", 2));
    }

    @Test
    void shouldTestHashCodeForSimpleEncodingFilter() {
        assertEquals(eq("x", 1).hashCode(), eq("x", 1).hashCode());
        assertNotEquals(eq("x", 1).hashCode(), ne("x", 1).hashCode());
    }

    @Test
    void shouldTestEqualsForAndFilter() {
        assertTrue(and(Collections.emptyList()).equals(and()));
        assertTrue(and(asList(eq("x", 1), eq("y", 2))).equals(and(eq("x", 1), eq("y", 2))));
    }

    @Test
    void shouldTestEqualsForOrNorFilter() {
        assertTrue(or(Collections.emptyList()).equals(or()));
        assertTrue(nor(eq("x", 1), eq("x", 2)).equals(nor(eq("x", 1), eq("x", 2))));
        assertNotEquals(nor(eq("x", 1), eq("x", 2)), or(eq("x", 1), eq("x", 2)));
    }

    @Test
    void shouldTestEqualsForNotFilter() {
        assertTrue(not(eq("x", 1)).equals(not(eq("x", 1))));
    }

    @Test
    void shouldTestEqualsForGeometryOperatorFilter() {
        Polygon polygon = new Polygon(asList(new Position(asList(40.0d, 18.0d)),
                new Position(asList(40.0d, 19.0d)),
                new Position(asList(41.0d, 19.0d)),
                new Position(asList(40.0d, 18.0d))));
        assertTrue(geoWithin("loc", polygon).equals(geoWithin("loc", polygon)));
    }

    @Test
    void shouldTestEqualsForTextFilter() {
        assertTrue(text("mongoDB for GIANT ideas").equals(text("mongoDB for GIANT ideas")));
    }
}
