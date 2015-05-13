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

import java.util.Collection;

final class ObserverHelpers {

    static <T> Observer<T> forEach(final Block<? super T> block, final SingleResultCallback<Void> callback,
                                   final Integer batchSize) {
        return new FetchAllObserver<T>(new Observer<T>() {

            @Override
            public void onSubscribe(final Subscription s) {
            }

            @Override
            public void onNext(final T t) {
                block.apply(t);
            }

            @Override
            public void onError(final Throwable e) {
                callback.onResult(null, e);
            }

            @Override
            public void onComplete() {
                callback.onResult(null, null);
            }
        }, batchSize);
    }

    static <T, A extends Collection<? super T>> Observer<T> into(final A target, final SingleResultCallback<A> callback,
                                                                 final Integer batchSize) {
        return new FetchAllObserver<T>(new Observer<T>() {

            @Override
            public void onSubscribe(final Subscription s) {
            }

            @Override
            public void onNext(final T t) {
                target.add(t);
            }

            @Override
            public void onError(final Throwable e) {
                callback.onResult(null, e);
            }

            @Override
            public void onComplete() {
                callback.onResult(target, null);
            }
        }, batchSize);
    }

    private ObserverHelpers() {
    }
}
