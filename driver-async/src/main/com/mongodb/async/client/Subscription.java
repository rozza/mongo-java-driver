/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.async.client;

/**
 * A {@link Subscription} represents a one-to-one lifecycle of a {@link Observer} subscribing to a Observable.
 * <p>
 * It can only be used once by a single {@link Observer}.
 * </p>
 * <p>
 * It is used to both signal desire for data and to allow for unsubscribing.
 * </p>
 *
 * @since 3.1
 */
public interface Subscription {

    /**
     * No operation will be sent to MongoDB from the Observable until demand is signaled via this method.
     * <p>
     * It can be called however often and whenever needed—but the outstanding cumulative demand must never exceed Long.MAX_VALUE.
     * An outstanding cumulative demand of Long.MAX_VALUE may be treated by the Observable as "effectively unbounded".
     * </p>
     * <p>
     * Whatever has been requested can be sent by the Observable so only signal demand for what can be safely handled.
     * <p>
     * A Observable can send less than is requested if the stream ends but then must emit either {@link Observer#onError(Throwable)} or
     * {@link Observer#onComplete()}.
     * </p>
     * @param n the strictly positive number of elements to requests to the upstream Observable
     */
    void request(long n);

    /**
     * Request the Observable to stop sending data and clean up resources.
     * <p>
     * Data may still be sent to meet previously signalled demand after calling cancel as this request is asynchronous.
     * </p>
     */
    void unsubscribe();

    /**
     * Indicates whether this {@code Subscription} is currently unsubscribed.
     *
     * @return {@code true} if this {@code Subscription} is currently unsubscribed, {@code false} otherwise
     */
    boolean isUnsubscribed();

}
