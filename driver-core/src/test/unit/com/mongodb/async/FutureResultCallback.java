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

package com.mongodb.async;

import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A SingleResultCallback Future implementation.
 *
 * <p>The result of the callback is stored internally and is accessible via {@link #get}, which will either return the successful result
 * of the callback or if the callback returned an error the error will be throw.</p>
 *
 * @param <T> the result type
 * @since 3.0
 */
public class FutureResultCallback<T> implements SingleResultCallback<T>, Future<T> {
    private final CountDownLatch latch;
    private final CallbackResultHolder<T> result;

    public FutureResultCallback() {
        latch = new CountDownLatch(1);
        result = new CallbackResultHolder<T>();
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return result.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        latch.await();
        if (result.hasError()) {
            throw MongoException.fromThrowable(result.getError());
        } else {
            return result.getResult();
        }
    }

    @Override
    public T get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (!latch.await(timeout, unit)) {
            throw new MongoTimeoutException("Callback timed out");
        }
        if (result.hasError()) {
            throw (RuntimeException) result.getError();
        } else {
            return result.getResult();
        }
    }

    @Override
    public void onResult(final T result, final Throwable t) {
        this.result.onResult(result, t);
        latch.countDown();
    }
}
