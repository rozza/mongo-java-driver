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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.lang.Nullable;
import org.reactivestreams.Publisher;

/**
 * A publisher for batch cursors
 *
 * @param <T> The type of documents the cursor contains
 * @since 4.2
 */
public interface BatchCursorPublisher<T> extends Publisher<T> {

    /**
     * Helper to return the first item from the Publisher.
     *
     * @return a publisher only containing the first item from the Publisher
     */
    Publisher<T> first();

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    Publisher<T> batchSize(int batchSize);

    /**
     * Gets the number of documents to return per batch or null if not set.
     *
     * @return the batch size, which may be null
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    @Nullable
    Integer getBatchSize();

    /**
     * Provide the underlying {@link BatchCursor} allowing fine grained control of the cursor.
     * @return the Publisher containing the BatchCursor
     */
    Publisher<? extends BatchCursor<T>> batchCursor();

}
