package com.mongodb.reactivestreams.client.internal.reactor.gridfs;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.util.annotation.NonNull;

import java.nio.ByteBuffer;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

class ResizingByteBufferFlux extends Flux<ByteBuffer> {

    private final Publisher<ByteBuffer> internal;
    private final int chunkSize;

    ResizingByteBufferFlux(final Publisher<ByteBuffer> internal, final int chunkSize) {
        notNull("internal must not be null", internal);
        isTrue("'chunkSize' must be a positive number", chunkSize >= 0);
        this.internal = internal;
        this.chunkSize = chunkSize;
    }

    @Override
    public void subscribe(final CoreSubscriber<? super ByteBuffer> actual) {
        Flux.<ByteBuffer>create(sink -> {
            BaseSubscriber<ByteBuffer> subscriber = new BaseSubscriber<ByteBuffer>() {
                Subscription upstreamSubscription;
                ByteBuffer remainder;

                @Override
                protected void hookOnSubscribe(Subscription subscription) {
                    upstreamSubscription = subscription;
                    sink.onRequest(n -> upstreamSubscription.request(n));
                    sink.onCancel(subscription::cancel);
                }

                @Override
                protected void hookOnNext(@NonNull final ByteBuffer value) {
                    if (remainder != null && remainder.remaining() > 0) {
                        ByteBuffer newBuffer = ByteBuffer.allocate(remainder.remaining() + value.remaining());
                        newBuffer.put(remainder.asReadOnlyBuffer());
                        newBuffer.put(value.asReadOnlyBuffer());
                        newBuffer.flip();
                        remainder = newBuffer;
                    } else {
                        remainder = value;
                    }

                    while (remainder.remaining() >= chunkSize && sink.requestedFromDownstream() > 0) {
                        int newLimit = remainder.position() + chunkSize;
                        sink.next(remainder.duplicate().limit(newLimit));
                        remainder.position(newLimit);
                    }

                    if (sink.requestedFromDownstream() > 0) {
                        upstreamSubscription.request(1);
                    }
                }

                @Override
                protected void hookOnComplete() {
                    if (remainder != null && remainder.remaining() > 0) {
                        sink.next(remainder);
                    }
                    sink.complete();
                }

                @Override
                protected void hookOnError(@NonNull final Throwable throwable) {
                    sink.error(throwable);
                }
            };

            internal.subscribe(subscriber);
        }).subscribe(actual);
    }
}
