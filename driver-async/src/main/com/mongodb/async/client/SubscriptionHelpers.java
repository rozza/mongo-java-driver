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

import com.mongodb.Block;
import com.mongodb.async.SingleResultCallback;

import java.util.List;


/**
 * Subscription helpers.
 * <p/>
 * <p>Allows async methods to be converted into event-based style observables.</p>
 *
 * @since 3.1
 */
public final class SubscriptionHelpers {

    /**
     * Subscribe to a {@link MongoIterable} with the supplied {@link Observer}.
     *
     * <p>Returns a {@link Subscription} that is used to signal demand from the {@code MongoIterable}.</p>
     *
     * @param mongoIterable the MongoIterable to subscribe to
     * @param observer      the observer for the results of the MongoIterable
     * @param <TResult>     The type of result being observed
     * @return a subscription
     */
    public static <TResult> Subscription subscribeTo(final MongoIterable<TResult> mongoIterable, final Observer<TResult> observer) {
        return new MongoIterableSubscription<TResult>(mongoIterable, observer);
    }

    /**
     * Subscribes to a {@link Block}  with the supplied {@link Observer}.
     *
     * <p>Allows the conversion of {@link SingleResultCallback} based operations into an observable.</p>
     * <p>Requires a {@link Block} that is passed the callback to be used with the operation.
     * This is required to make sure that the operation only occurs once the {@link Subscription} signals for data.</p>
     * <p>
     * A typical example would be when wrapping callback based methods to make them observable. <br>
     * For example, converting {@link MongoCollection#count(SingleResultCallback)} into a {@link Block} style operation:
     * <pre>
     * {@code
     *    new Block<SingleResultCallback<Long>>() {
     *        public void apply(final SingleResultCallback<Long> callback) {
     *            collection.count(callback);
     *        }
     *    };
     * }
     * </pre>
     *
     * <p>Returns a {@link Subscription} that is used to signal demand and calls the operation that will apply the callback.</p>
     *
     * @param operation the block that implements the operation.
     * @param observer  the observer for the results of the operation
     * @param <TResult> The type of result being observed
     * @return a subscription
     */
    public static <TResult> Subscription subscribeTo(final Block<SingleResultCallback<TResult>> operation,
                                                     final Observer<TResult> observer) {
        return new SingleResultCallbackSubscription<TResult>(operation, observer);
    }

    /**
     * Subscribes to a {@link Block} and flattens the results before being emitted to the supplied {@link Observer}.
     *
     * <p>Allows the conversion of {@link SingleResultCallback} based operations and flattens the results emitted. For usage example
     * see: {@link #subscribeTo(Block, Observer)}</p>
     *
     * <p>Returns a {@link Subscription} that is used to signal demand and calls the operation that will apply the callback.</p>
     *
     * @param operation the operation that is passed a callback and is used to delay execution of an operation until demanded.
     * @param observer  the observer for the results of the operation
     * @param <TResult> The type of result being observed
     * @return a subscription
     */
    public static <TResult> Subscription subscribeToAndFlatten(final Block<SingleResultCallback<List<TResult>>> operation,
                                                               final Observer<TResult> observer) {
        return new FlatteningSingleResultCallbackSubscription<TResult>(operation, observer);
    }

    private SubscriptionHelpers() {
    }
}
