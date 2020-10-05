package com.mongodb.reactivestreams.client;

import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.lang.Nullable;
import org.reactivestreams.Publisher;

/**
 * Operations that allow asynchronous iteration over a collection view.
 *
 * @param <T> the result type
 */
public interface MongoPublisher<T> extends Publisher<T> {

    /**
     * Helper to return the first item in the iterator or null.
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
     * Provide the underlying {@link AsyncBatchCursor} allowing fine grained control of the cursor.
     */
    Publisher<AsyncBatchCursor<T>> batchCursor();

}
