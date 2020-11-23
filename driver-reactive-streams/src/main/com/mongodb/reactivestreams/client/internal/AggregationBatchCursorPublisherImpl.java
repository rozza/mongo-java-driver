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

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.client.OperationExecutor;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.AggregateBatchCursor;
import com.mongodb.reactivestreams.client.AggregationBatchCursorPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Mono;

public abstract class AggregationBatchCursorPublisherImpl<T> implements AggregationBatchCursorPublisher<T> {

    private final BatchCursorPublisherImpl<T> wrapped;

    AggregationBatchCursorPublisherImpl(
            @Nullable final ClientSession clientSession, final OperationExecutor executor, final ReadConcern readConcern,
            final ReadPreference readPreference, final boolean retryReads) {
        wrapped = new BatchCursorPublisherImpl<T>(clientSession, executor, readConcern, readPreference, retryReads) {
            @Override
            AsyncReadOperation<AsyncBatchCursor<T>> asAsyncReadOperation() {
                return AggregationBatchCursorPublisherImpl.this.asAsyncReadOperation();
            }
        };
    }

    abstract AsyncReadOperation<AsyncBatchCursor<T>> asAsyncReadOperation();

    @Nullable
    public ClientSession getClientSession() {
        return wrapped.getClientSession();
    }

    public OperationExecutor getExecutor() {
        return wrapped.getExecutor();
    }

    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
    }

    public ReadConcern getReadConcern() {
        return wrapped.getReadConcern();
    }

    public boolean getRetryReads() {
        return wrapped.getRetryReads();
    }

    @Override
    public Publisher<T> first() {
        return wrapped.first();
    }

    @Override
    public AggregationBatchCursorPublisher<T> batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    @Override
    @Nullable
    public Integer getBatchSize() {
        return wrapped.getBatchSize();
    }

    @Override
    public void subscribe(final Subscriber<? super T> subscriber) {
        wrapped.subscribe(subscriber);
    }

    @Override
    public Publisher<AggregateBatchCursor<T>> batchCursor() {
        return Mono.from(wrapped.batchCursor()).map(AggregateBatchCursorImpl::new);
    }
}
