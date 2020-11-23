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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.internal.async.AsyncAggregateResponseBatchCursor;
import com.mongodb.reactivestreams.client.AggregateBatchCursor;
import com.mongodb.reactivestreams.client.BatchCursor;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.reactivestreams.Publisher;

import java.util.List;

class AggregateBatchCursorImpl<T> implements AggregateBatchCursor<T> {

    private final BatchCursorImpl<T> wrapped;

    AggregateBatchCursorImpl(final BatchCursor<T> wrapped) {
        this.wrapped = (BatchCursorImpl<T>) wrapped;
    }

    @Override
    public BsonDocument getPostBatchResumeToken() {
        return getAsyncBatchCursor().getPostBatchResumeToken();
    }

    @Override
    public BsonTimestamp getOperationTime() {
        return getAsyncBatchCursor().getOperationTime();
    }

    @Override
    public boolean isFirstBatchEmpty() {
        return getAsyncBatchCursor().isFirstBatchEmpty();
    }

    @Override
    public int getMaxWireVersion() {
        return getAsyncBatchCursor().getMaxWireVersion();
    }

    @Override
    public Publisher<List<T>> next() {
        return wrapped.next();
    }

    @Override
    public Publisher<List<T>> tryNext() {
        return wrapped.tryNext();
    }

    @Override
    public void setBatchSize(final int batchSize) {
        wrapped.setBatchSize(batchSize);
    }

    @Override
    public int getBatchSize() {
        return wrapped.getBatchSize();
    }

    @Override
    public boolean isClosed() {
        return wrapped.isClosed();
    }

    @Override
    public void close() {
        wrapped.close();
    }

    AsyncAggregateResponseBatchCursor<T> getAsyncBatchCursor() {
        return (AsyncAggregateResponseBatchCursor<T>) this.wrapped.getAsyncBatchCursor();
    }

}
