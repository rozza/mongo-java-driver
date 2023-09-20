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

package com.mongodb.internal.connection;

import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.time.Timeout;
import com.mongodb.internal.time.TimePoint;
import org.bson.types.ObjectId;
import com.mongodb.lang.Nullable;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

/**
 * An instance of an implementation must be created in the {@linkplain #invalidate(Throwable) paused} state.
 */
@ThreadSafe
interface ConnectionPool extends Closeable {
    /**
     * Is equivalent to {@link #get(OperationIdContext, long, TimeUnit)} called with {@link ConnectionPoolSettings#getMaxWaitTime(TimeUnit)}.
     */
    InternalConnection get(OperationIdContext operationIdContext) throws MongoConnectionPoolClearedException;

    /**
     * @param operationIdContext operation context
     * @param timeout          See {@link Timeout#started(long, TimeUnit, TimePoint)}.
     * @throws MongoConnectionPoolClearedException If detects that the pool is {@linkplain #invalidate(Throwable) paused}.
     */
    InternalConnection get(OperationIdContext operationIdContext, long timeout, TimeUnit timeUnit) throws MongoConnectionPoolClearedException;

    /**
     * Completes the {@code callback} with a {@link MongoConnectionPoolClearedException}
     * if detects that the pool is {@linkplain #invalidate(Throwable) paused}.
     */
    void getAsync(OperationIdContext operationIdContext, SingleResultCallback<InternalConnection> callback);

    /**
     * Mark the pool as paused, unblock all threads waiting in {@link #get(OperationIdContext) get…} methods, unless they are blocked
     * doing an IO operation, increment {@linkplain #getGeneration() generation} to lazily clear all connections managed by the pool
     * (this is done via {@link #get(OperationIdContext) get…} and {@linkplain InternalConnection#close() check in} methods, and may also be done
     * by a background task). In the paused state, connections can be created neither in the background
     * nor via {@link #get(OperationIdContext) get…} methods.
     * If the pool is paused, the method does nothing except for recording the specified {@code cause}.
     *
     * @see #ready()
     */
    void invalidate(@Nullable Throwable cause);

    /**
     * Unlike {@link #invalidate(Throwable)}, this method neither marks the pool as paused,
     * nor affects the {@linkplain #getGeneration() generation}.
     *
     * @param generation The expected service-specific generation.
     *                   If the expected {@code generation} does not match the current generation for the service
     *                   identified by {@code serviceId}, the method does nothing.
     */
    void invalidate(ObjectId serviceId, int generation);

    /**
     * Mark the pool as ready, allowing connections to be created in the background and via {@link #get(OperationIdContext) get…} methods.
     * If the pool is ready, the method does nothing.
     *
     * @see #invalidate(Throwable)
     */
    void ready();

    /**
     * Mark the pool as closed, release the underlying resources and render the pool unusable.
     */
    void close();

    int getGeneration();
}
