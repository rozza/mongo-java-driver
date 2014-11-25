/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.connection;

import java.util.concurrent.CountDownLatch;

class FutureAsyncCompletionHandler<T> implements AsyncCompletionHandler<T> {
    private final CountDownLatch latch;
    private T result = null;
    private Throwable error = null;

    public FutureAsyncCompletionHandler(final CountDownLatch latch) {
        this.latch = latch;
    }

    @Override
    public void completed(final T result) {
        latch.countDown();
        this.result = result;
    }

    @Override
    public void failed(final Throwable t) {
        latch.countDown();
        this.error = t;
    }

    public T getResult() {
        return result;
    }

    public boolean hasError() {
        return error != null;
    }

    public Throwable getError() {
        return error;
    }
}
