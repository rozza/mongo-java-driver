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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.OperationFunctionalSpecification;
import com.mongodb.client.model.fill.FillOutputField;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.ClusterFixture.serverVersionLessThan;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.expr;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class AggregatesFunctionalSpecificationTest extends OperationFunctionalSpecification {

    private final Document a = new Document("_id", 1).append("x", 1)
            .append("y", "a").append("z", false)
            .append("a", asList(1, 2, 3))
            .append("a1", asList(new Document("c", 1).append("d", 2), new Document("c", 2).append("d", 3)))
            .append("o", new Document("a", 1));

    private final Document b = new Document("_id", 2).append("x", 2)
            .append("y", "b").append("z", true)
            .append("a", asList(3, 4, 5, 6))
            .append("a1", asList(new Document("c", 2).append("d", 3), new Document("c", 3).append("d", 4)))
            .append("o", new Document("b", 2));

    private final Document c = new Document("_id", 3).append("x", 3)
            .append("y", "c").append("z", true)
            .append("o", new Document("c", 3));

    @BeforeEach
    public void setUp() {
        super.setUp();
        getCollectionHelper().insertDocuments(a, b, c);
    }

    private List<Document> aggregate(final List<Bson> pipeline) {
        return getCollectionHelper().aggregate(pipeline);
    }

    @Test
    void dollar_match() {
        assertEquals(asList(a, b), aggregate(asList(match(Filters.exists("a1")))));
    }

    @Test
    void dollar_project() {
        assertEquals(asList(
                new Document("_id", 1).append("x", 1).append("c", "a"),
                new Document("_id", 2).append("x", 2).append("c", "b"),
                new Document("_id", 3).append("x", 3).append("c", "c")),
                aggregate(asList(project(fields(include("x"), computed("c", "$y"))))));
    }

    @Test
    void dollar_project_exclusion() {
        assertEquals(asList(
                new Document("_id", 1).append("x", 1).append("y", "a"),
                new Document("_id", 2).append("x", 2).append("y", "b"),
                new Document("_id", 3).append("x", 3).append("y", "c")),
                aggregate(asList(project(exclude("a", "a1", "z", "o")))));
    }

    @Test
    void dollar_sort() {
        assertEquals(asList(c, b, a), aggregate(asList(sort(descending("x")))));
    }

    @Test
    void dollar_skip() {
        assertEquals(asList(b, c), aggregate(asList(skip(1))));
    }

    @Test
    void dollar_limit() {
        assertEquals(asList(a, b), aggregate(asList(limit(2))));
    }

    @Test
    void dollar_unwind() {
        assertEquals(asList(
                new Document("a", 1), new Document("a", 2), new Document("a", 3),
                new Document("a", 3), new Document("a", 4), new Document("a", 5), new Document("a", 6)),
                aggregate(asList(project(fields(include("a"), excludeId())), unwind("$a"))));
    }

    @Test
    void dollar_group() {
        assertEquals(asList(new Document("_id", null)), aggregate(asList(group(null))));
        assertTrue(aggregate(asList(group("$z"))).containsAll(
                asList(new Document("_id", true), new Document("_id", false))));
        assertEquals(asList(new Document("_id", null).append("acc", 6)),
                aggregate(asList(group(null, sum("acc", "$x")))));
    }

    @Test
    void dollar_group_with_top_or_bottom_n() {
        assumeTrue(!serverVersionLessThan(5, 2));
        // TODO: Convert from Groovy Spock test: '$group with top or bottom n'
    }

    @Test
    void dollar_out() {
        String outCollectionName = getCollectionName() + ".out";
        aggregate(asList(out(outCollectionName)));
        assertEquals(asList(a, b, c),
                getCollectionHelper(new MongoNamespace(getDatabaseName(), outCollectionName)).find());
    }

    @Test
    void dollar_out_to_specified_database() {
        assumeTrue(!serverVersionLessThan(4, 4));
        // TODO: Convert from Groovy Spock test: '$out to specified database'
    }

    @Test
    void dollar_merge() {
        // TODO: Convert from Groovy Spock test: '$merge'
    }

    @Test
    void dollar_stdDev() {
        // TODO: Convert from Groovy Spock test: '$stdDev'
    }

    @Test
    void dollar_sample() {
        List<Document> result = aggregate(asList(sample(1)));
        assertEquals(1, result.size());
        assertTrue(asList(a, b, c).contains(result.get(0)));
    }

    @Test
    void dollar_lookup() {
        // TODO: Convert from Groovy Spock test: '$lookup'
    }

    @Test
    void dollar_lookup_with_pipeline() {
        // TODO: Convert from Groovy Spock test: '$lookup with pipeline'
    }

    @Test
    void dollar_lookup_with_pipeline_without_variables() {
        // TODO: Convert from Groovy Spock test: '$lookup with pipeline without variables'
    }

    @Test
    void dollar_facet() {
        // TODO: Convert from Groovy Spock test: '$facet'
    }

    @Test
    void dollar_graphLookup() {
        // TODO: Convert from Groovy Spock test: '$graphLookup'
    }

    @Test
    void dollar_graphLookup_with_depth_options() {
        // TODO: Convert from Groovy Spock test: '$graphLookup with depth options'
    }

    @Test
    void dollar_graphLookup_with_query_filter_option() {
        // TODO: Convert from Groovy Spock test: '$graphLookup with query filter option'
    }

    @Test
    void dollar_bucket() {
        // TODO: Convert from Groovy Spock test: '$bucket'
    }

    @Test
    void dollar_bucketAuto() {
        // TODO: Convert from Groovy Spock test: '$bucketAuto'
    }

    @Test
    void dollar_bucketAuto_with_options() {
        // TODO: Convert from Groovy Spock test: '$bucketAuto with options'
    }

    @Test
    void dollar_count() {
        // TODO: Convert from Groovy Spock test: '$count'
    }

    @Test
    void dollar_sortByCount() {
        // TODO: Convert from Groovy Spock test: '$sortByCount'
    }

    @Test
    void dollar_accumulator() {
        assumeTrue(!serverVersionLessThan(4, 4));
        // TODO: Convert from Groovy Spock test: '$accumulator'
    }

    @Test
    void dollar_addFields() {
        // TODO: Convert from Groovy Spock test: '$addFields'
    }

    @Test
    void dollar_set() {
        // TODO: Convert from Groovy Spock test: '$set'
    }

    @Test
    void dollar_replaceRoot() {
        // TODO: Convert from Groovy Spock test: '$replaceRoot'
    }

    @Test
    void dollar_replaceWith() {
        // TODO: Convert from Groovy Spock test: '$replaceWith'
    }

    @Test
    void dollar_unionWith() {
        assumeTrue(!serverVersionLessThan(4, 4));
        // TODO: Convert from Groovy Spock test: '$unionWith'
    }

    @Test
    void dollar_setWindowFields() {
        assumeTrue(!serverVersionLessThan(5, 2));
        // TODO: Convert from Groovy Spock test: '$setWindowFields' (data-driven)
    }

    @Test
    void dollar_setWindowFields_with_multiple_output() {
        assumeTrue(!serverVersionLessThan(5, 0));
        // TODO: Convert from Groovy Spock test: '$setWindowFields with multiple output'
    }

    @Test
    void dollar_setWindowFields_with_empty_output() {
        assumeTrue(!serverVersionLessThan(5, 0));
        // TODO: Convert from Groovy Spock test: '$setWindowFields with empty output'
    }

    @Test
    void dollar_densify() {
        assumeTrue(!serverVersionLessThan(5, 1));
        // TODO: Convert from Groovy Spock test: '$densify' (data-driven)
    }

    @Test
    void dollar_fill() {
        assumeTrue(!serverVersionLessThan(5, 3));
        // TODO: Convert from Groovy Spock test: '$fill' (data-driven)
    }
}
