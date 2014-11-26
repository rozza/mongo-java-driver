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

import com.mongodb.MongoInternalException;
import com.mongodb.MongoInterruptedException;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

class FutureAsyncCompletionHandler<T> implements AsyncCompletionHandler<T> {
    private final CountDownLatch latch = new CountDownLatch(1);;
    private T result;
    private Throwable error;

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

    public void getWrite() throws IOException {
        get(false);
    }

    public T getRead() throws IOException {
        return get(true);
    }

    private T get(final boolean reading) throws IOException {
        String prefix = reading ? "Reading from" : "Writing to";
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new MongoInterruptedException(prefix + " the AsynchronousSocketChannelStream failed", e);

        }
        if (error != null) {
            if (error instanceof IOException) {
                throw (IOException) error;
            } else {
                throw new MongoInternalException(prefix + " the AsynchronousSocketChannelStream failed", error);
            }
        }
        return result;
    }

}
