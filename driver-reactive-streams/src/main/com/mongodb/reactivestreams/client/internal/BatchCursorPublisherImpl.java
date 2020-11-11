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
import com.mongodb.reactivestreams.client.BatchCursor;
import com.mongodb.reactivestreams.client.BatchCursorPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.sinkToCallback;

public abstract class BatchCursorPublisherImpl<T> implements BatchCursorPublisher<T> {
    private final ClientSession clientSession;
    private final ReadConcern readConcern;
    private final OperationExecutor executor;
    private final ReadPreference readPreference;
    private final boolean retryReads;
    private Integer batchSize;

    BatchCursorPublisherImpl(@Nullable final ClientSession clientSession, final OperationExecutor executor,
                              final ReadConcern readConcern, final ReadPreference readPreference, final boolean retryReads) {
        this.clientSession = clientSession;
        this.executor = notNull("executor", executor);
        this.readConcern = notNull("readConcern", readConcern);
        this.readPreference = notNull("readPreference", readPreference);
        this.retryReads = retryReads;
    }

    abstract AsyncReadOperation<AsyncBatchCursor<T>> asAsyncReadOperation();

    @Nullable
    public ClientSession getClientSession() {
        return clientSession;
    }

    OperationExecutor getExecutor() {
        return executor;
    }

    ReadPreference getReadPreference() {
        return readPreference;
    }

    ReadConcern getReadConcern() {
        return readConcern;
    }

    boolean getRetryReads() {
        return retryReads;
    }

    @Nullable
    public Integer getBatchSize() {
        return batchSize;
    }

    @Override
    public Publisher<T> batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public Publisher<T> first() {
        return batchCursor()
                .flatMap(batchCursor -> Mono.create(sink -> {
                    batchCursor.setBatchSize(1);
                    Mono.from(batchCursor.next())
                            .doOnTerminate(batchCursor::close)
                            .doOnError(sink::error)
                            .doOnSuccess(results -> {
                                if (results == null || results.isEmpty()) {
                                    sink.success();
                                } else {
                                    sink.success(results.get(0));
                                }
                            })
                            .subscribe();
                }));
    }

    @Override
    public void subscribe(final Subscriber<? super T> subscriber) {
        batchCursor()
                .flatMapMany(batchCursor -> {
                    AtomicBoolean inProgress = new AtomicBoolean(false);
                    if (batchSize != null) {
                        batchCursor.setBatchSize(batchSize);
                    }
                    return Flux.create((FluxSink<T> sink) -> {
                        sink.onRequest(value -> recurseCursor(sink, batchCursor, inProgress));
                        recurseCursor(sink, batchCursor, inProgress);
                    }, FluxSink.OverflowStrategy.BUFFER)
                    .doOnCancel(batchCursor::close);
                })
                .subscribe(subscriber);
    }

    void recurseCursor(final FluxSink<T> sink, final BatchCursor<T> batchCursor, final AtomicBoolean inProgress){
        if (!sink.isCancelled() && sink.requestedFromDownstream() > 0 && inProgress.compareAndSet(false, true)) {
            if (batchCursor.isClosed()) {
                sink.complete();
            } else {
                Mono.from(batchCursor.next())
                        .doOnCancel(batchCursor::close)
                        .doOnError((e) -> {
                            batchCursor.close();
                            sink.error(e);
                        })
                        .doOnSuccess(results -> {
                            if (results != null) {
                                results.forEach(sink::next);
                            }
                            if (batchCursor.isClosed()) {
                                sink.complete();
                            } else {
                                inProgress.compareAndSet(true, false);
                                recurseCursor(sink, batchCursor, inProgress);
                            }
                        })
                        .subscribe();
            }
        }
    }

    @Override
    public Mono<BatchCursor<T>> batchCursor() {
        return Mono.<AsyncBatchCursor<T>>create(sink ->
                    executor.execute(asAsyncReadOperation(), readPreference, readConcern,
                                     clientSession != null ? clientSession.getWrapped() : null,
                                     sinkToCallback(sink)))
                        .map(BatchCursorImpl::new);
    }
}
