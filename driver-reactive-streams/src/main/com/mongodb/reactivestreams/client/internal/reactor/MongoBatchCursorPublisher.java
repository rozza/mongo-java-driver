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

package com.mongodb.reactivestreams.client.internal.reactor;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.client.OperationExecutor;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoPublisher;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.mongodb.assertions.Assertions.notNull;


abstract class MongoBatchCursorPublisher<T> implements MongoPublisher<T> {
    private final ClientSession clientSession;
    private final ReadConcern readConcern;
    private final OperationExecutor executor;
    private final ReadPreference readPreference;
    private final boolean retryReads;
    private Integer batchSize;

    MongoBatchCursorPublisher(@Nullable final ClientSession clientSession, final OperationExecutor executor,
                              final ReadConcern readConcern, final ReadPreference readPreference, final boolean retryReads) {
        this.clientSession = clientSession;
        this.executor = notNull("executor", executor);
        this.readConcern = notNull("readConcern", readConcern);
        this.readPreference = notNull("readPreference", readPreference);
        this.retryReads = retryReads;
    }

    abstract AsyncReadOperation<AsyncBatchCursor<T>> asAsyncReadOperation();

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
    public MongoPublisher<T> batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public Publisher<T> first() {
        return batchCursor().flatMap(batchCursor -> Mono.create(callback -> {
            batchCursor.setBatchSize(1);
            batchCursor.next((results, t) -> {
                batchCursor.close();
                if (t != null) {
                    callback.error(t);
                } else if (results == null) {
                    callback.success();
                } else {
                    callback.success(results.get(0));
                }
            });
        }));
    }

    @Override
    public void subscribe(final Subscriber<? super T> subscriber) {
        AtomicBoolean inProgress = new AtomicBoolean(false);
        batchCursor().map(batchCursor ->
                Flux.using(() -> {
                            if (batchSize != null) {
                                batchCursor.setBatchSize(batchSize);
                            }
                            return batchCursor;
                        }, (Function<AsyncBatchCursor<T>, Flux<T>>) cursor ->
                                Flux.create(sink -> {
                                    sink.onRequest(value -> recurseCursor(sink, cursor, inProgress));
                                    recurseCursor(sink, cursor, inProgress);
                                }, FluxSink.OverflowStrategy.BUFFER),
                        AsyncBatchCursor::close)
        ).flatMapMany(flux -> flux).subscribe(subscriber);
    }

    @Override
    public Mono<AsyncBatchCursor<T>> batchCursor() {
        return Mono.create(callback ->
                executor.execute(asAsyncReadOperation(), readPreference, readConcern,
                        clientSession != null ? clientSession.getWrapped() : null,
                        ((AsyncBatchCursor<T> batchCursor, Throwable t) -> {
                            if (t != null) {
                                callback.error(t);
                            } else {
                                callback.success(batchCursor);
                            }
                        })
                )
        );
    }

    void recurseCursor(final FluxSink<T> sink, final AsyncBatchCursor<T> cursor, final AtomicBoolean inProgress){
        if (!sink.isCancelled() && sink.requestedFromDownstream() > 0 && inProgress.compareAndSet(false, true)) {
            if (cursor.isClosed()) {
                sink.complete();
            } else {
                cursor.next((results, t) -> {
                    if (t != null) {
                        cursor.close();
                        sink.error(t);
                    } else {
                        if (results != null) {
                            results.forEach(sink::next);
                        }
                        if (cursor.isClosed()) {
                            sink.complete();
                        } else {
                            inProgress.compareAndSet(true, false);
                            recurseCursor(sink, cursor, inProgress);
                        }
                    }
                });
            }
        }
    }

}
