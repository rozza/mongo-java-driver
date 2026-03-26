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

package com.mongodb.client.async;

import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.async.FutureResultCallback;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FutureResultCallbackTest {

    @Test
    void shouldReturnFalseIfTriedToCancel() {
        FutureResultCallback<Object> futureResultCallback = new FutureResultCallback<>();
        assertFalse(futureResultCallback.cancel(false));
        assertFalse(futureResultCallback.cancel(true));
        assertFalse(futureResultCallback.isCancelled());
    }

    @Test
    void shouldReturnTrueIfDone() {
        FutureResultCallback<Object> futureResultCallback = new FutureResultCallback<>();
        futureResultCallback.onResult(null, null);
        assertTrue(futureResultCallback.isDone());
    }

    @Test
    void shouldReturnResultOnGet() throws Exception {
        FutureResultCallback<Boolean> futureResultCallback = new FutureResultCallback<>();
        futureResultCallback.onResult(true, null);
        assertTrue(futureResultCallback.get());

        FutureResultCallback<Boolean> futureResultCallback2 = new FutureResultCallback<>();
        futureResultCallback2.onResult(null, new MongoException("failed"));
        assertThrows(MongoException.class, futureResultCallback2::get);

        FutureResultCallback<Boolean> futureResultCallback3 = new FutureResultCallback<>();
        futureResultCallback3.onResult(true, null);
        assertTrue(futureResultCallback3.get(1, SECONDS));

        FutureResultCallback<Boolean> futureResultCallback4 = new FutureResultCallback<>();
        futureResultCallback4.onResult(null, new MongoException("failed"));
        assertThrows(MongoException.class, () -> futureResultCallback4.get(1, SECONDS));
    }

    @Test
    void shouldTimeoutWhenNoResultAndCalledGet() {
        FutureResultCallback<Object> futureResultCallback = new FutureResultCallback<>();
        assertThrows(MongoTimeoutException.class, () -> futureResultCallback.get(1, MILLISECONDS));
    }

    @Test
    void shouldThrowErrorIfOnResultCalledMoreThanOnce() throws Exception {
        FutureResultCallback<Boolean> futureResultCallback = new FutureResultCallback<>();
        futureResultCallback.onResult(true, null);
        assertTrue(futureResultCallback.get());
        assertThrows(IllegalStateException.class, () -> futureResultCallback.onResult(false, null));
    }
}
