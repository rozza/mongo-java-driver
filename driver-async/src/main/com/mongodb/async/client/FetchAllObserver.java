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

class FetchAllObserver<TResult> implements Observer<TResult> {

    private final Observer<TResult> delegate;
    private final long batchSize;
    private long counter = 0;
    private Subscription subscription;

    public FetchAllObserver(final Observer<TResult> delegate, final Integer batchSize) {
        this.delegate = delegate;
        this.batchSize = adjustedBatchSize(batchSize);
    }

    int adjustedBatchSize(final Integer batchSize) {
        if (batchSize == null || batchSize == 0) {
            return Integer.MAX_VALUE;
        } else {
            return batchSize;
        }
    }

    void request() {
        subscription.request(batchSize);
    }

    @Override
    public void onSubscribe(final Subscription s) {
        this.subscription = s;
        delegate.onSubscribe(s);
        request();
    }

    @Override
    public void onNext(final TResult result) {
        try {
            delegate.onNext(result);
        } catch (Throwable t) {
            onError(t);
        }
        counter++;
        if (counter == batchSize) {
            counter = 0;
            request();
        }
    }

    @Override
    public void onError(final Throwable e) {
        delegate.onError(e);
    }

    @Override
    public void onComplete() {
        delegate.onComplete();
    }

}
