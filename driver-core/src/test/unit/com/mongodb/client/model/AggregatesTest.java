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

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.fill.FillOutputField;
import com.mongodb.client.model.search.SearchCollector;
import com.mongodb.client.model.search.SearchOperator;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BinaryVector;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Accumulators.accumulator;
import static com.mongodb.client.model.Accumulators.addToSet;
import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Accumulators.bottom;
import static com.mongodb.client.model.Accumulators.bottomN;
import static com.mongodb.client.model.Accumulators.first;
import static com.mongodb.client.model.Accumulators.firstN;
import static com.mongodb.client.model.Accumulators.last;
import static com.mongodb.client.model.Accumulators.lastN;
import static com.mongodb.client.model.Accumulators.max;
import static com.mongodb.client.model.Accumulators.maxN;
import static com.mongodb.client.model.Accumulators.mergeObjects;
import static com.mongodb.client.model.Accumulators.min;
import static com.mongodb.client.model.Accumulators.minN;
import static com.mongodb.client.model.Accumulators.push;
import static com.mongodb.client.model.Accumulators.stdDevPop;
import static com.mongodb.client.model.Accumulators.stdDevSamp;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Accumulators.top;
import static com.mongodb.client.model.Accumulators.topN;
import static com.mongodb.client.model.Aggregates.addFields;
import static com.mongodb.client.model.Aggregates.bucket;
import static com.mongodb.client.model.Aggregates.bucketAuto;
import static com.mongodb.client.model.Aggregates.count;
import static com.mongodb.client.model.Aggregates.densify;
import static com.mongodb.client.model.Aggregates.fill;
import static com.mongodb.client.model.Aggregates.graphLookup;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Aggregates.lookup;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.merge;
import static com.mongodb.client.model.Aggregates.out;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.replaceRoot;
import static com.mongodb.client.model.Aggregates.replaceWith;
import static com.mongodb.client.model.Aggregates.sample;
import static com.mongodb.client.model.Aggregates.search;
import static com.mongodb.client.model.Aggregates.searchMeta;
import static com.mongodb.client.model.Aggregates.set;
import static com.mongodb.client.model.Aggregates.setWindowFields;
import static com.mongodb.client.model.Aggregates.skip;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Aggregates.sortByCount;
import static com.mongodb.client.model.Aggregates.unionWith;
import static com.mongodb.client.model.Aggregates.unwind;
import static com.mongodb.client.model.Aggregates.vectorSearch;
import static com.mongodb.client.model.BsonHelper.toBson;
import static com.mongodb.client.model.BucketGranularity.R5;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.expr;
import static com.mongodb.client.model.MongoTimeUnit.DAY;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Windows.Bound.CURRENT;
import static com.mongodb.client.model.Windows.Bound.UNBOUNDED;
import static com.mongodb.client.model.Windows.documents;
import static com.mongodb.client.model.densify.DensifyRange.fullRangeWithStep;
import static com.mongodb.client.model.fill.FillOptions.fillOptions;
import static com.mongodb.client.model.search.SearchCollector.facet;
import static com.mongodb.client.model.search.SearchCount.total;
import static com.mongodb.client.model.search.SearchFacet.stringFacet;
import static com.mongodb.client.model.search.SearchHighlight.paths;
import static com.mongodb.client.model.search.SearchOperator.exists;
import static com.mongodb.client.model.search.SearchOptions.searchOptions;
import static com.mongodb.client.model.search.SearchPath.fieldPath;
import static com.mongodb.client.model.search.SearchPath.wildcardPath;
import static com.mongodb.client.model.search.VectorSearchOptions.approximateVectorSearchOptions;
import static com.mongodb.client.model.search.VectorSearchOptions.exactVectorSearchOptions;
import static java.util.Arrays.asList;
import static org.bson.BsonDocument.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregatesTest {

    @Test
    void shouldRenderAccumulator() {
        String initFunction = "function() { return { count : 0, sum : 0 } }";
        String initFunctionWithArgs = "function(initCount, initSun) { return { count : parseInt(initCount), sum : parseInt(initSun) } }";
        String accumulateFunction = "function(state, numCopies) { return { count : state.count + 1, sum : state.sum + numCopies } }";
        String mergeFunction = "function(state1, state2) { return { count : state1.count + state2.count, sum : state1.sum + state2.sum } }";
        String finalizeFunction = "function(state) { return (state.sum / state.count) }";

        assertEquals(
                parse("{$group: {_id: null, test: {$accumulator: {init: \"" + initFunction + "\", initArgs: [], accumulate: \""
                        + accumulateFunction + "\", accumulateArgs: [], merge: \"" + mergeFunction + "\", lang: \"js\"}}}}"),
                toBson(group(null, accumulator("test", initFunction, accumulateFunction, mergeFunction))));

        assertEquals(
                parse("{$group: {_id: null, test: {$accumulator: {init: \"" + initFunction + "\", initArgs: [], accumulate: \""
                        + accumulateFunction + "\", accumulateArgs: [], merge: \"" + mergeFunction + "\", finalize: \""
                        + finalizeFunction + "\", lang: \"js\"}}}}"),
                toBson(group(null, accumulator("test", initFunction, accumulateFunction, mergeFunction, finalizeFunction))));

        assertEquals(
                parse("{$group: {_id: null, test: {$accumulator: {init: \"" + initFunctionWithArgs
                        + "\", initArgs: [ \"0\", \"0\" ], accumulate: \"" + accumulateFunction
                        + "\", accumulateArgs: [ \"$copies\" ], merge: \"" + mergeFunction
                        + "\", finalize: \"" + finalizeFunction + "\", lang: \"js\"}}}}"),
                toBson(group(null, accumulator("test", initFunctionWithArgs, asList("0", "0"), accumulateFunction,
                        asList("$copies"), mergeFunction, finalizeFunction))));
    }

    @Test
    void shouldRenderAddFields() {
        assertEquals(parse("{$addFields: {newField: null}}"), toBson(addFields(new Field<>("newField", null))));
        assertEquals(parse("{$addFields: {newField: \"hello\"}}"), toBson(addFields(new Field<>("newField", "hello"))));
        assertEquals(parse("{$addFields: {this: \"$$CURRENT\"}}"), toBson(addFields(new Field<>("this", "$$CURRENT"))));
        assertEquals(parse("{$addFields: {myNewField: {c: 3, d: 4}}}"),
                toBson(addFields(new Field<>("myNewField", new Document("c", 3).append("d", 4)))));
        assertEquals(parse("{$addFields: {b: 3, c: 5}}"),
                toBson(addFields(new Field<>("b", 3), new Field<>("c", 5))));
        assertEquals(parse("{$addFields: {b: 3, c: 5}}"),
                toBson(addFields(asList(new Field<>("b", 3), new Field<>("c", 5)))));
    }

    @Test
    void shouldRenderSet() {
        assertEquals(parse("{$set: {newField: null}}"), toBson(set(new Field<>("newField", null))));
        assertEquals(parse("{$set: {newField: \"hello\"}}"), toBson(set(new Field<>("newField", "hello"))));
        assertEquals(parse("{$set: {b: 3, c: 5}}"), toBson(set(new Field<>("b", 3), new Field<>("c", 5))));
        assertEquals(parse("{$set: {b: 3, c: 5}}"), toBson(set(asList(new Field<>("b", 3), new Field<>("c", 5)))));
    }

    @Test
    void shouldRenderBucket() {
        assertEquals(parse("{$bucket: {groupBy: \"$screenSize\", boundaries: [0, 24, 32, 50, 100000]}}"),
                toBson(bucket("$screenSize", asList(0, 24, 32, 50, 100000))));

        assertEquals(parse("{$bucket: {groupBy: \"$screenSize\", boundaries: [0, 24, 32, 50, 100000], default: \"other\"}}"),
                toBson(bucket("$screenSize", asList(0, 24, 32, 50, 100000),
                        new BucketOptions().defaultBucket("other"))));

        assertEquals(parse("{$bucket: {groupBy: \"$screenSize\", boundaries: [0, 24, 32, 50, 100000], default: \"other\","
                        + " output: {count: {$sum: 1}, matches: {$push: \"$screenSize\"}}}}"),
                toBson(bucket("$screenSize", asList(0, 24, 32, 50, 100000),
                        new BucketOptions().defaultBucket("other").output(sum("count", 1), push("matches", "$screenSize")))));
    }

    @Test
    void shouldRenderBucketAuto() {
        assertEquals(parse("{$bucketAuto: {groupBy: \"$price\", buckets: 4}}"),
                toBson(bucketAuto("$price", 4)));

        assertEquals(parse("{$bucketAuto: {groupBy: \"$price\", buckets: 4, output: {count: {$sum: 1}, avgPrice: {$avg: \"$price\"}}}}"),
                toBson(bucketAuto("$price", 4, new BucketAutoOptions().output(sum("count", 1), avg("avgPrice", "$price")))));

        assertEquals(parse("{$bucketAuto: {groupBy: \"$price\", buckets: 4,"
                        + " output: {count: {$sum: 1}, avgPrice: {$avg: \"$price\"}}, granularity: \"R5\"}}"),
                toBson(bucketAuto("$price", 4, new BucketAutoOptions().granularity(R5)
                        .output(sum("count", 1), avg("avgPrice", "$price")))));
    }

    @Test
    void shouldRenderCount() {
        assertEquals(parse("{$count: \"count\"}"), toBson(count()));
        assertEquals(parse("{$count: \"count\"}"), toBson(count("count")));
        assertEquals(parse("{$count: \"total\"}"), toBson(count("total")));
    }

    @Test
    void shouldRenderMatch() {
        assertEquals(parse("{ $match : { author : \"dave\" } }"), toBson(match(eq("author", "dave"))));
    }

    @Test
    void shouldRenderProject() {
        assertEquals(parse("{ $project : { title : 1 , author : 1, lastName : \"$author.last\" } }"),
                toBson(project(fields(include("title", "author"), computed("lastName", "$author.last")))));
    }

    @Test
    void shouldRenderReplaceRoot() {
        assertEquals(parse("{$replaceRoot: {newRoot: \"$a1\"}}"), toBson(replaceRoot("$a1")));
        assertEquals(parse("{$replaceRoot: {newRoot: \"$a1.b\"}}"), toBson(replaceRoot("$a1.b")));
    }

    @Test
    void shouldRenderReplaceWith() {
        assertEquals(parse("{$replaceWith: \"$a1\"}"), toBson(replaceWith("$a1")));
        assertEquals(parse("{$replaceWith: \"$a1.b\"}"), toBson(replaceWith("$a1.b")));
    }

    @Test
    void shouldRenderSort() {
        assertEquals(parse("{ $sort : { title : 1 , author : 1 } }"), toBson(sort(ascending("title", "author"))));
    }

    @Test
    void shouldRenderSortByCount() {
        assertEquals(parse("{$sortByCount: \"someField\"}"), toBson(sortByCount("someField")));
        assertEquals(parse("{$sortByCount: {$floor: \"$x\"}}"), toBson(sortByCount(new Document("$floor", "$x"))));
    }

    @Test
    void shouldRenderLimit() {
        assertEquals(parse("{ $limit : 5 }"), toBson(limit(5)));
    }

    @Test
    void shouldRenderLookup() {
        assertEquals(parse("{ $lookup : { from: \"from\", localField: \"localField\","
                        + " foreignField: \"foreignField\", as: \"as\" } }"),
                toBson(lookup("from", "localField", "foreignField", "as")));

        List<Bson> pipeline = asList(match(expr(new Document("$eq", asList("x", "1")))));
        assertEquals(parse("{ $lookup : { from: \"from\", let: { var1: \"expression1\" },"
                        + " pipeline : [{ $match : { $expr: { $eq : [ \"x\" , \"1\" ]}}}], as: \"as\" }}"),
                toBson(lookup("from", asList(new Variable<>("var1", "expression1")), pipeline, "as")));

        assertEquals(parse("{ $lookup : { from: \"from\","
                        + " pipeline : [{ $match : { $expr: { $eq : [ \"x\" , \"1\" ]}}}], as: \"as\" }}"),
                toBson(lookup("from", pipeline, "as")));
    }

    @Test
    void shouldRenderSkip() {
        assertEquals(parse("{ $skip : 5 }"), toBson(skip(5)));
    }

    @Test
    void shouldRenderUnionWith() {
        List<Bson> pipeline = asList(match(expr(new Document("$eq", asList("x", "1")))));
        assertEquals(parse("{ $unionWith : { coll: \"with\", pipeline : [{ $match : { $expr: { $eq : [ \"x\" , \"1\" ]}}}] }}"),
                toBson(unionWith("with", pipeline)));
    }

    @Test
    void shouldRenderUnwind() {
        assertEquals(parse("{ $unwind : \"$sizes\" }"), toBson(unwind("$sizes")));
        assertEquals(parse("{ $unwind : { path : \"$sizes\" } }"),
                toBson(unwind("$sizes", new UnwindOptions().preserveNullAndEmptyArrays(null))));
        assertEquals(parse("{ $unwind : { path : \"$sizes\", preserveNullAndEmptyArrays : false } }"),
                toBson(unwind("$sizes", new UnwindOptions().preserveNullAndEmptyArrays(false))));
        assertEquals(parse("{ $unwind : { path : \"$sizes\", preserveNullAndEmptyArrays : true } }"),
                toBson(unwind("$sizes", new UnwindOptions().preserveNullAndEmptyArrays(true))));
    }

    @Test
    void shouldRenderOut() {
        assertEquals(parse("{ $out : \"authors\" }"), toBson(out("authors")));
        assertEquals(parse("{ $out : { db: \"authorsDB\", coll: \"books\" } }"), toBson(out("authorsDB", "books")));
    }

    @Test
    void shouldRenderMerge() {
        assertEquals(parse("{ $merge : {into: \"authors\" }}"), toBson(merge("authors")));
        assertEquals(parse("{ $merge : {into: {db: \"db1\", coll: \"authors\" }}}"),
                toBson(merge(new MongoNamespace("db1", "authors"))));
        assertEquals(parse("{ $merge : {into: \"authors\", on: \"ssn\" }}"),
                toBson(merge("authors", new MergeOptions().uniqueIdentifier("ssn"))));
        assertEquals(parse("{ $merge : {into: \"authors\", on: [\"ssn\", \"otherId\"] }}"),
                toBson(merge("authors", new MergeOptions().uniqueIdentifier(asList("ssn", "otherId")))));
        assertEquals(parse("{ $merge : {into: \"authors\", whenMatched: \"replace\" }}"),
                toBson(merge("authors", new MergeOptions().whenMatched(MergeOptions.WhenMatched.REPLACE))));
        assertEquals(parse("{ $merge : {into: \"authors\", whenNotMatched: \"discard\" }}"),
                toBson(merge("authors", new MergeOptions().whenNotMatched(MergeOptions.WhenNotMatched.DISCARD))));
    }

    @Test
    void shouldRenderGroup() {
        assertEquals(parse("{ $group : { _id : \"$customerId\" } }"), toBson(group("$customerId")));
        assertEquals(parse("{ $group : { _id : null } }"), toBson(group(null)));
    }

    @Test
    void shouldRenderSample() {
        assertEquals(parse("{ $sample : { size: 5} }"), toBson(sample(5)));
    }

    @Test
    void shouldRenderDensify() {
        assertEquals(parse("{\"$densify\": {\"field\": \"fieldName\", \"range\": { \"bounds\": \"full\", \"step\": 1 }}}"),
                toBson(densify("fieldName", fullRangeWithStep(1))));
    }

    @Test
    void shouldRenderFill() {
        assertEquals(parse("{\"$fill\": {\"output\": {\"fieldName1\": { \"method\" : \"linear\" },"
                        + " \"fieldName2\": { \"method\" : \"locf\" }}, \"sortBy\": { \"fieldName3\": 1 }}}"),
                toBson(fill(fillOptions().sortBy(ascending("fieldName3")),
                        FillOutputField.linear("fieldName1"), FillOutputField.locf("fieldName2"))));
    }

    @Test
    void shouldRenderSearch() {
        assertEquals(parse("{\"$search\": {\"exists\": { \"path\": \"fieldName\" }}}"),
                toBson(search((SearchOperator) exists(fieldPath("fieldName")), searchOptions())));

        assertEquals(parse("{\"$search\": {\"exists\": { \"path\": \"fieldName\" }}}"),
                toBson(search((SearchOperator) exists(fieldPath("fieldName")))));
    }

    @Test
    void shouldRenderSearchMeta() {
        assertEquals(parse("{\"$searchMeta\": {\"exists\": { \"path\": \"fieldName\" }}}"),
                toBson(searchMeta((SearchOperator) exists(fieldPath("fieldName")), searchOptions())));

        assertEquals(parse("{\"$searchMeta\": {\"exists\": { \"path\": \"fieldName\" }}}"),
                toBson(searchMeta((SearchOperator) exists(fieldPath("fieldName")))));
    }

    @Test
    void shouldRenderApproximateVectorSearch() {
        assertEquals(parse("{\"$vectorSearch\": {\"path\": \"fieldName\", \"queryVector\": [1.0, 2.0],"
                        + " \"index\": \"indexName\", \"numCandidates\": {\"$numberLong\": \"2\"},"
                        + " \"limit\": {\"$numberLong\": \"1\"}, \"filter\": {\"fieldName\": {\"$ne\": \"fieldValue\"}}}}"),
                toBson(vectorSearch(fieldPath("fieldName").multi("ignored"), asList(1.0d, 2.0d), "indexName", 1,
                        approximateVectorSearchOptions(2).filter(Filters.ne("fieldName", "fieldValue")))));
    }

    @Test
    void shouldRenderExactVectorSearch() {
        assertEquals(parse("{\"$vectorSearch\": {\"path\": \"fieldName\", \"queryVector\": [1.0, 2.0],"
                        + " \"index\": \"indexName\", \"exact\": true,"
                        + " \"limit\": {\"$numberLong\": \"1\"}, \"filter\": {\"fieldName\": {\"$ne\": \"fieldValue\"}}}}"),
                toBson(vectorSearch(fieldPath("fieldName").multi("ignored"), asList(1.0d, 2.0d), "indexName", 1,
                        exactVectorSearchOptions().filter(Filters.ne("fieldName", "fieldValue")))));
    }

    @Test
    void shouldCreateStringRepresentationForSimpleStages() {
        assertEquals("Stage{name='$match', value={\"x\": 1}}",
                match(new BsonDocument("x", new BsonInt32(1))).toString());
    }

    @Test
    void shouldCreateStringRepresentationForGroupStage() {
        assertEquals("Stage{name='$group', id=_id, fieldAccumulators=["
                        + "BsonField{name='avg', value=Expression{name='$avg', expression=$quantity}}]}",
                group("_id", avg("avg", "$quantity")).toString());
    }

    @Test
    void shouldTestEqualsForSimplePipelineStage() {
        assertTrue(match(eq("author", "dave")).equals(match(eq("author", "dave"))));
        assertTrue(sort(ascending("title", "author")).equals(sort(ascending("title", "author"))));
        assertNotEquals(sort(ascending("title", "author")), sort(descending("title", "author")));
    }

    @Test
    void shouldTestHashCodeForSimplePipelineStage() {
        assertEquals(match(eq("author", "dave")).hashCode(), match(eq("author", "dave")).hashCode());
        assertEquals(sort(ascending("title", "author")).hashCode(), sort(ascending("title", "author")).hashCode());
        assertNotEquals(sort(ascending("title", "author")).hashCode(), sort(descending("title", "author")).hashCode());
    }

    @Test
    void shouldTestEqualsForBucketStage() {
        assertTrue(bucket("$screenSize", asList(0, 24, 32, 50, 100000))
                .equals(bucket("$screenSize", asList(0, 24, 32, 50, 100000))));
    }

    @Test
    void shouldTestEqualsForBucketAutoStage() {
        assertTrue(bucketAuto("$price", 4).equals(bucketAuto("$price", 4)));
    }

    @Test
    void shouldTestEqualsForLookupStage() {
        assertTrue(lookup("from", "localField", "foreignField", "as")
                .equals(lookup("from", "localField", "foreignField", "as")));
    }

    @Test
    void shouldTestEqualsForGroupStage() {
        assertTrue(group("$customerId").equals(group("$customerId")));
        assertTrue(group(null).equals(group(null)));
    }

    @Test
    void shouldTestEqualsForSortByCountStage() {
        assertTrue(sortByCount("someField").equals(sortByCount("someField")));
    }

    @Test
    void shouldTestEqualsForReplaceRootStage() {
        assertTrue(replaceRoot("$a1").equals(replaceRoot("$a1")));
        assertTrue(replaceRoot("$a1.b").equals(replaceRoot("$a1.b")));
    }

    @Test
    void shouldTestEqualsForAddFieldsStage() {
        assertTrue(addFields(new Field<>("newField", null)).equals(addFields(new Field<>("newField", null))));
        assertTrue(addFields(new Field<>("newField", "hello")).equals(addFields(new Field<>("newField", "hello"))));
    }

    @Test
    void shouldTestHashCodeForAddFieldsStage() {
        assertEquals(addFields(new Field<>("newField", null)).hashCode(), addFields(new Field<>("newField", null)).hashCode());
        assertEquals(addFields(new Field<>("newField", "hello")).hashCode(), addFields(new Field<>("newField", "hello")).hashCode());
    }
}
