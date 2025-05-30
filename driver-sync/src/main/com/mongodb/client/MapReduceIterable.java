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

package com.mongodb.client;

import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Reason;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

/**
 * Iterable for map-reduce.
 * <p>
 * By default, the {@code MapReduceIterable} produces the results inline. You can write map-reduce output to a collection by using the
 * {@link #collectionName(String)} and {@link #toCollection()} methods.</p>
 *
 * @param <TResult> The type of the result.
 * @since 3.0
 * @deprecated Superseded by aggregate
 */
@Deprecated
public interface MapReduceIterable<TResult> extends MongoIterable<TResult> {

    /**
     * Aggregates documents to a collection according to the specified map-reduce function with the given options, which must not produce
     * results inline. This method is the preferred alternative to {@link #iterator()}, {@link #cursor()},
     * because this method does what is explicitly requested without executing implicit operations.
     *
     * @throws IllegalStateException if a {@linkplain #collectionName(String) collection name} to write the results to has not been specified
     * @see #collectionName(String)
     * @since 3.4
     */
    void toCollection();

    /**
     * Aggregates documents according to the specified map-reduce function with the given options.
     * <ul>
     *     <li>
     *     If the aggregation produces results inline, then {@linkplain MongoCollection#find() finds all} documents in the
     *     affected namespace and returns a {@link MongoCursor} over them. You may want to use {@link #toCollection()} instead.</li>
     *     <li>
     *     Otherwise, returns a {@link MongoCursor} producing no elements.</li>
     * </ul>
     */
    @Override
    MongoCursor<TResult> iterator();

    /**
     * Aggregates documents according to the specified map-reduce function with the given options.
     * <ul>
     *     <li>
     *     If the aggregation produces results inline, then {@linkplain MongoCollection#find() finds all} documents in the
     *     affected namespace and returns a {@link MongoCursor} over them. You may want to use {@link #toCollection()} instead.</li>
     *     <li>
     *     Otherwise, returns a {@link MongoCursor} producing no elements.</li>
     * </ul>
     */
    @Override
    MongoCursor<TResult> cursor();

    /**
     * Sets the collectionName for the output of the MapReduce
     *
     * <p>The default action is replace the collection if it exists, to change this use {@link #action}.</p>
     *
     * @param collectionName the name of the collection that you want the map-reduce operation to write its output.
     * @return this
     * @see #toCollection()
     */
    MapReduceIterable<TResult> collectionName(String collectionName);

    /**
     * Sets the JavaScript function that follows the reduce method and modifies the output.
     *
     * @param finalizeFunction the JavaScript function that follows the reduce method and modifies the output.
     * @return this
     * @mongodb.driver.manual reference/command/mapReduce/#mapreduce-finalize-cmd Requirements for the finalize Function
     */
    MapReduceIterable<TResult> finalizeFunction(@Nullable String finalizeFunction);

    /**
     * Sets the global variables that are accessible in the map, reduce and finalize functions.
     *
     * @param scope the global variables that are accessible in the map, reduce and finalize functions.
     * @return this
     * @mongodb.driver.manual reference/command/mapReduce mapReduce
     */
    MapReduceIterable<TResult> scope(@Nullable Bson scope);

    /**
     * Sets the sort criteria to apply to the query.
     *
     * @param sort the sort criteria, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/cursor.sort/ Sort
     */
    MapReduceIterable<TResult> sort(@Nullable Bson sort);

    /**
     * Sets the query filter to apply to the query.
     *
     * @param filter the filter to apply to the query.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    MapReduceIterable<TResult> filter(@Nullable Bson filter);

    /**
     * Sets the limit to apply.
     *
     * @param limit the limit, which may be null
     * @return this
     * @mongodb.driver.manual reference/method/cursor.limit/#cursor.limit Limit
     */
    MapReduceIterable<TResult> limit(int limit);

    /**
     * Sets the flag that specifies whether to convert intermediate data into BSON format between the execution of the map and reduce
     * functions. Defaults to false.
     *
     * @param jsMode the flag that specifies whether to convert intermediate data into BSON format between the execution of the map and
     *               reduce functions
     * @return jsMode
     * @mongodb.driver.manual reference/command/mapReduce mapReduce
     */
    MapReduceIterable<TResult> jsMode(boolean jsMode);

    /**
     * Sets whether to include the timing information in the result information.
     *
     * @param verbose whether to include the timing information in the result information.
     * @return this
     */
    MapReduceIterable<TResult> verbose(boolean verbose);

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    MapReduceIterable<TResult> maxTime(long maxTime, TimeUnit timeUnit);

    /**
     * Specify the {@code MapReduceAction} to be used when writing to a collection.
     *
     * @param action an {@link com.mongodb.client.model.MapReduceAction} to perform on the collection
     * @return this
     */
    MapReduceIterable<TResult> action(com.mongodb.client.model.MapReduceAction action);

    /**
     * Sets the name of the database to output into.
     *
     * @param databaseName the name of the database to output into.
     * @return this
     * @mongodb.driver.manual reference/command/mapReduce/#output-to-a-collection-with-an-action output with an action
     */
    MapReduceIterable<TResult> databaseName(@Nullable String databaseName);

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    @Override
    MapReduceIterable<TResult> batchSize(int batchSize);

    /**
     * Sets the bypass document level validation flag.
     *
     * <p>Note: This only applies when an $out stage is specified</p>.
     *
     * @param bypassDocumentValidation If true, allows the write to opt-out of document level validation.
     * @return this
     * @since 3.2
     * @mongodb.driver.manual reference/command/mapReduce mapReduce
     * @mongodb.server.release 3.2
     */
    MapReduceIterable<TResult> bypassDocumentValidation(@Nullable Boolean bypassDocumentValidation);

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    MapReduceIterable<TResult> collation(@Nullable Collation collation);

    /**
     * Sets the timeoutMode for the cursor.
     *
     * <p>
     *     Requires the {@code timeout} to be set, either in the {@link com.mongodb.MongoClientSettings},
     *     via {@link MongoDatabase} or via {@link MongoCollection}
     * </p>
     * @param timeoutMode the timeout mode
     * @return this
     * @since 5.2
     */
    @Alpha(Reason.CLIENT)
    MapReduceIterable<TResult> timeoutMode(TimeoutMode timeoutMode);
}
