package com.mongodb.reactivestreams.reactor;

import com.mongodb.internal.async.AsyncBatchCursor;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class ReactiveCursor<T, R> implements Processor<T, R> {

    private  AsyncBatchCursor<R> cursor;


    @Override
    public void subscribe(final Subscriber<? super R> subscriber) {

    }


    @Override
    public void onSubscribe(final Subscription subscription) {

    }

    @Override
    public void onNext(final T t) {

    }


    @Override
    public void onError(final Throwable throwable) {

    }

    @Override
    public void onComplete() {

    }

}
