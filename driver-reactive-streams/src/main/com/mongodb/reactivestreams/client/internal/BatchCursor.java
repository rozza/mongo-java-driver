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

import org.reactivestreams.Publisher;

import java.io.Closeable;
import java.util.List;

/**
 * MongoDB returns query results as batches, and this interface provides an asynchronous iterator over those batches.  The first call to
 * the {@code next} method will return the first batch, and subsequent calls will trigger an asynchronous request to get the next batch
 * of results.  Clients can control the batch size by setting the {@code batchSize} property between calls to {@code next}.
 *
 * @param <T> The type of documents the cursor contains
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#wire-op-get-more OP_GET_MORE
 * @since 4.2
 */
public interface BatchCursor<T> extends Closeable {
    /**
     * Returns the next batch of results.  A tailable cursor will internally block until another batch exists.
     *
     * @return a publisher containing the next elements
     */
    Publisher<List<T>> next();

    /**
     * A special {@code next()} case that returns the next batch if available or just completes.
     *
     * <p>Tailable cursors are an example where this is useful. A call to {@code tryNext()} may return no results, but in the future calling
     * {@code tryNext()} would return a new batch if a document had been added to the capped collection.</p>
     *
     * @mongodb.driver.manual reference/glossary/#term-tailable-cursor Tailable Cursor
     * @return a publisher containing the next elements
     */
    Publisher<List<T>> tryNext();

    /**
     * Sets the batch size to use when requesting the next batch.  This is the number of documents to request in the next batch.
     *
     * @param batchSize the non-negative batch size.  0 means to use the server default.
     */
    void setBatchSize(int batchSize);

    /**
     * Gets the batch size to use when requesting the next batch.  This is the number of documents to request in the next batch.
     *
     * @return the non-negative batch size.  0 means to use the server default.
     */
    int getBatchSize();

    /**
     * Return true if the BatchCursor has been closed
     *
     * @return true if the BatchCursor has been closed
     */
    boolean isClosed();

    @Override
    void close();
}
