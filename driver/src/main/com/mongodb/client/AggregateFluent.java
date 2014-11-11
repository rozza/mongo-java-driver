/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.client;

/**
 * Fluent interface for aggregate.
 *
 * @param <T> The aggregation result type.
 * @since 3.0
 */
public interface AggregateFluent<T> extends MongoIterable<T> {

    /**
     * Add a {@code $geoNear} operation to the pipeline.
     *
     * <p>Outputs documents in order of nearest to farthest from a specified point.</p>
     *
     * @param geoNear the geoNear document
     * @return this
     * @mongodb.driver.manual reference/operator/aggregation/geoNear/ $geoNear
     */
    AggregateFluent<T> geoNear(Object geoNear);

    /**
     * Add a {@code $group} operation to the pipeline.
     *
     * <p>Groups documents by some specified expression and outputs to the next stage a document for each distinct grouping.</p>
     *
     * @param group the group operation
     * @return this
     * @mongodb.driver.manual reference/operator/aggregation/group/ $group
     */
    AggregateFluent<T> group(Object group);

    /**
     * Add a {@code $limit} operation to the pipeline.
     *
     * <p>Limits the number of documents passed to the next stage in the pipeline.</p>
     *
     * @param limit the number to limit by
     * @return this
     * @mongodb.driver.manual reference/operator/aggregation/limit/ $limit
     */
    AggregateFluent<T> limit(int limit);

    /**
     * Add a {@code $match} operation to the pipeline.
     *
     * <p>Filters the documents to pass only the documents that match the specified condition(s) to the next pipeline stage.</p>
     *
     * @param match the match pipeline query
     * @return this
     * @mongodb.driver.manual reference/operator/aggregation/limit/ $limit
     */
    AggregateFluent<T> match(Object match);

    /**
     * Add a {@code $out} operation to the pipeline.
     *
     * <p>Takes the documents returned by the aggregation pipeline and writes them to a specified collection.</p>
     * <p><strong>Note:</strong>The $out operator must be the last stage in the pipeline.</p>
     *
     * @param collectionName the collection name string
     * @return this
     * @mongodb.driver.manual reference/operator/aggregation/out/ $out
     */
    AggregateFluent<T> out(String collectionName);

    /**
     * Add a {@code $project} operation to the pipeline.
     *
     * <p>Passes along the documents with only the specified fields to the next stage in the pipeline.</p>
     *
     * @param project the projection specification
     * @return this
     * @mongodb.driver.manual reference/operator/aggregation/project/ $project
     */
    AggregateFluent<T> project(Object project);

    /**
     * Add a {@code $redact} operation to the pipeline.
     *
     * <p>Restricts the contents of the documents based on information stored in the documents themselves.</p>
     *
     * @param redact the redact expression
     * @return this
     * @mongodb.driver.manual reference/operator/aggregation/redact/ $redact
     */
    AggregateFluent<T> redact(Object redact);

    /**
     * Add a {@code $skip} operation to the pipeline.
     *
     * <p>Skips over the specified number of documents that pass into the stage and passes the remaining documents to the next stage in the
     * pipeline.</p>
     *
     * @param skip the number to skip
     * @return this
     * @mongodb.driver.manual reference/operator/aggregation/skip/ $skip
     */
    AggregateFluent<T> skip(int skip);

    /**
     * Add a {@code $sort} operation to the pipeline.
     *
     * <p>Sorts all input documents and returns them to the pipeline in sorted order.</p>
     *
     * @param sort the sort object
     * @return this
     * @mongodb.driver.manual reference/operator/aggregation/sort/ $sort
     */
    AggregateFluent<T> sort(Object sort);

    /**
     * Add a {@code $unwind} operation to the pipeline.
     *
     * <p>Deconstructs an array field from the input documents to output a document for each element.</p>
     *
     * @param fieldPath the field path representation
     * @return this
     * @mongodb.driver.manual reference/operator/aggregation/unwind/ $unwind
     */
    AggregateFluent<T> unwind(String fieldPath);

}
