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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.internal.operation.Operations;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import org.bson.codecs.configuration.CodecRegistry;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Mono;

public abstract class AggregationBatchCursorPublisherImpl<T> implements BatchCursorPublisher<T> {

    private final BatchCursorPublisherImpl<T> wrapped;

    AggregationBatchCursorPublisherImpl(
            @Nullable final ClientSession clientSession, final MongoOperationPublisher<T> mongoOperationPublisher) {
        wrapped = new BatchCursorPublisherImpl<T>(clientSession, mongoOperationPublisher) {
            @Override
            AsyncReadOperation<AsyncBatchCursor<T>> asAsyncReadOperation() {
                return AggregationBatchCursorPublisherImpl.this.asAsyncReadOperation();
            }
        };
    }

    abstract AsyncReadOperation<AsyncBatchCursor<T>> asAsyncReadOperation();

    // TODO CLEANUPO

    @Nullable
    public ClientSession getClientSession() {
        return wrapped.getClientSession();
    }

    MongoNamespace getNamespace() {
        return wrapped.getNamespace();
    }

    ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
    }

    CodecRegistry getCodecRegistry() {
        return wrapped.getCodecRegistry();
    }

    ReadConcern getReadConcern() {
        return wrapped.getReadConcern();
    }

    WriteConcern getWriteConcern() {
        return wrapped.getWriteConcern();
    }

    boolean getRetryWrites() {
        return wrapped.getRetryWrites();
    }

    boolean getRetryReads() {
        return wrapped.getRetryReads();
    }

    Class<T> getDocumentClass() {
        return wrapped.getDocumentClass();
    }

    Operations<T> getOperations() {
        return wrapped.getOperations();
    }

    MongoOperationPublisher<T> getMongoOperationPublisher() {
        return wrapped.getMongoOperationPublisher();
    }

    @Override
    public Publisher<T> first() {
        return wrapped.first();
    }

    @Override
    public Publisher<T> batchSize(final int batchSize) {
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
    public Mono<AggregateBatchCursor<T>> batchCursor() {
        return Mono.from(wrapped.batchCursor()).map(AggregateBatchCursorImpl::new);
    }
}
