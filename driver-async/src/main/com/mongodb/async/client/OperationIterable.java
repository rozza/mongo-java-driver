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

package com.mongodb.async.client;

import com.mongodb.Block;
import com.mongodb.Function;
import com.mongodb.ReadPreference;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.operation.AsyncBatchCursor;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.AsyncReadOperation;

import java.util.Collection;

class OperationIterable<T> implements MongoIterable<T> {
    private final AsyncReadOperation<? extends AsyncBatchCursor<T>> operation;
    private final ReadPreference readPreference;
    private final AsyncOperationExecutor executor;

    OperationIterable(final AsyncReadOperation<? extends AsyncBatchCursor<T>> operation, final ReadPreference readPreference,
                      final AsyncOperationExecutor executor) {
        this.operation = operation;
        this.readPreference = readPreference;
        this.executor = executor;
    }

    @Override
    public void forEach(final Block<? super T> block, final SingleResultCallback<Void> callback) {
        execute(new SingleResultCallback<AsyncBatchCursor<T>>() {
            @Override
            public void onResult(final AsyncBatchCursor<T> result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    new AsyncBatchCursorIterable<T>(result).forEach(block, callback);
                }
            }
        });
    }

    @Override
    public <A extends Collection<? super T>> void into(final A target, final SingleResultCallback<A> callback) {
        execute(new SingleResultCallback<AsyncBatchCursor<T>>() {
            @Override
            public void onResult(final AsyncBatchCursor<T> result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    new AsyncBatchCursorIterable<T>(result).into(target, callback);
                }
            }
        });
    }

    @Override
    public void first(final SingleResultCallback<T> callback) {
        execute(new SingleResultCallback<AsyncBatchCursor<T>>() {
            @Override
            public void onResult(final AsyncBatchCursor<T> result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    new AsyncBatchCursorIterable<T>(result).first(callback);
                }
            }
        });
    }

    @Override
    public <U> MongoIterable<U> map(final Function<T, U> mapper) {
        return new MappingIterable<T, U>(this, mapper);
    }

    @SuppressWarnings("unchecked")
    private void execute(final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        executor.execute((AsyncReadOperation<AsyncBatchCursor<T>>) operation, readPreference, callback);
    }

}
