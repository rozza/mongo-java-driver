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

import com.mongodb.client.model.TimeoutMode;

import java.util.concurrent.TimeUnit;

/**
 * Iterable for ListIndexes.
 *
 * @param <TResult> The type of the result.
 * @since 3.0
 */
public interface ListIndexesIterable<TResult> extends MongoIterable<TResult> {

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/operator/meta/maxTimeMS/ Max Time
     * @deprecated prefer {@code MongoCollection.withTimeout(long, TimeUnit)} instead
     */
    @Deprecated
    ListIndexesIterable<TResult> maxTime(long maxTime, TimeUnit timeUnit);

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    @Override
    ListIndexesIterable<TResult> batchSize(int batchSize);

    /**
     * Sets the timeout mode.
     *
     * <p>For use with {@code timeoutMS} via {@link MongoCollection#withTimeout(long, TimeUnit)}.</p>
     *
     * @param timeoutMode the timeoutMode type
     * @return this
     * @since 4.x
     */
    @Override
    ListIndexesIterable<TResult> timeoutMode(TimeoutMode timeoutMode);
}
