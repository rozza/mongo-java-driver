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
import com.mongodb.async.SingleResultCallback;
import com.mongodb.operation.AsyncBatchCursor;

import java.util.Collection;
import java.util.List;

class AsyncBatchCursorIterable<T> implements MongoIterable<T> {
    private final AsyncBatchCursor<T> batchCursor;
    private volatile boolean isClosed;

    AsyncBatchCursorIterable(final AsyncBatchCursor<T> batchCursor) {
        this.batchCursor = batchCursor;
    }

    @Override
    public void forEach(final Block<? super T> block, final SingleResultCallback<Void> callback) {
        if (isClosed) {
            callback.onResult(null, new IllegalStateException("The iterable has already been used"));
        } else {
            isClosed = true;
            loopCursor(block, callback);
        }
    }

    @Override
    public <A extends Collection<? super T>> void into(final A target, final SingleResultCallback<A> callback) {
        if (isClosed) {
            callback.onResult(null, new IllegalStateException("The iterable has already been used"));
        } else {
            isClosed = true;
            loopCursor(new Block<T>() {
                @Override
                public void apply(final T t) {
                    target.add(t);
                }
            }, new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        callback.onResult(target, null);
                    }
                }
            });
        }
    }

    @Override
    public void first(final SingleResultCallback<T> callback) {
        if (isClosed) {
            callback.onResult(null, new IllegalStateException("The iterable has already been used"));
        } else {
            isClosed = true;
            batchCursor.setBatchSize(1);
            batchCursor.next(new SingleResultCallback<List<T>>() {
                @Override
                public void onResult(final List<T> results, final Throwable t) {
                    batchCursor.close();
                    if (t != null) {
                        callback.onResult(null, t);
                    } else if (results == null) {
                        callback.onResult(null, null);
                    } else {
                        callback.onResult(results.get(0), null);
                    }
                }
            });
        }
    }

    @Override
    public <U> MongoIterable<U> map(final Function<T, U> mapper) {
        return new MappingIterable<T, U>(this, mapper);
    }

    private void loopCursor(final Block<? super T> block, final SingleResultCallback<Void> callback) {
        batchCursor.next(new SingleResultCallback<List<T>>() {
            @Override
            public void onResult(final List<T> results, final Throwable t) {
                if (t != null || results == null) {
                    batchCursor.close();
                    callback.onResult(null, t);
                } else {
                    try {
                        for (T result : results) {
                            block.apply(result);
                        }
                        loopCursor(block, callback);
                    } catch (Throwable tr) {
                        batchCursor.close();
                        callback.onResult(null, tr);
                    }
                }
            }
        });
    }
}
