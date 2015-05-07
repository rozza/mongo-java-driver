/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.async.client;

import com.mongodb.Block;
import com.mongodb.MongoException;
import com.mongodb.async.SingleResultCallback;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

class SingleResultCallbackSubscription<TResult> implements Subscription {

    private final Observer<? super TResult> observer;
    private final Block<SingleResultCallback<List<TResult>>> block;

    /* protected by `this` */
    private boolean requestedResults;
    private boolean gotResults;
    private boolean isProcessing;
    private long requested = 0;
    private boolean terminated = false;
    private boolean isUnsubscribed = false;
    /* protected by `this` */


    private final ConcurrentLinkedQueue<TResult> resultsQueue = new ConcurrentLinkedQueue<TResult>();

    public SingleResultCallbackSubscription(final Block<SingleResultCallback<List<TResult>>> block,
                                            final Observer<? super TResult> observer) {
        this.block = block;
        this.observer = observer;
        observer.onSubscribe(this);

    }

    @Override
    public void request(final long n) {
        if (n < 1) {
            throw new IllegalArgumentException("Number requested cannot be negative: " + n);
        }

        boolean mustRequestResults = false;
        synchronized (this) {
            requested += n;
            if (!requestedResults) {
                requestedResults = true;
                mustRequestResults = true;
            }
        }

        if (mustRequestResults) {
            block.apply(getCallback());
        } else {
            processResultsQueue();
        }
    }

    @Override
    public void unsubscribe() {
        synchronized (this) {
            if (!isUnsubscribed) {
                isUnsubscribed = true;
                terminated = true;
            }
        }
    }

    @Override
    public boolean isUnsubscribed() {
        return isUnsubscribed;
    }

    private SingleResultCallback<List<TResult>> getCallback() {
        final SingleResultCallbackSubscription<TResult> subscription = this;
        return new SingleResultCallback<List<TResult>>() {
            @Override
            public void onResult(final List<TResult> result, final Throwable t) {
                if (t != null) {
                    onError(t);
                } else if (result != null) {

                    boolean processResults = false;
                    synchronized (subscription) {
                        gotResults = true;
                        processResults = !isUnsubscribed;
                    }

                    if (processResults) {
                        resultsQueue.addAll(result);
                        processResultsQueue();
                    }
                } else {
                    onError(new MongoException("Unexpected error, no AsyncBatchCursor returned from the MongoIterable."));
                }
            }
        };
    }

    private void processResultsQueue() {
        boolean mustProcess = false;

        synchronized (this) {
            if (!isProcessing && !isUnsubscribed) {
                isProcessing = true;
                mustProcess = true;
            }
        }

        if (mustProcess) {
            long processedCount = 0;
            boolean completed = false;
            while (true) {
                long localWanted = 0;

                synchronized (this) {
                    requested -= processedCount;
                    if (gotResults && resultsQueue.isEmpty()) {
                        isProcessing = false;
                        completed = true;
                        break;
                    } else if (requested == 0) {
                        isProcessing = false;
                        break;
                    }
                    localWanted = requested;
                }
                processedCount = 0;

                while (localWanted > 0) {
                    TResult item = resultsQueue.poll();
                    if (item == null) {
                        break;
                    } else {
                        onNext(item);
                        localWanted -= 1;
                        processedCount += 1;
                    }
                }
            }

            if (completed) {
                onComplete();
            }
        }
    }

    private void onError(final Throwable t) {
        if (terminalAction()) {
            observer.onError(t);
        }
    }

    private void onNext(final TResult next) {
        boolean isTerminated = false;
        synchronized (this) {
            isTerminated = terminated;
        }

        if (!isTerminated) {
            try {
                observer.onNext(next);
            } catch (Throwable t) {
                onError(t);
            }
        }
    }

    private void onComplete() {
        if (terminalAction()) {
            observer.onComplete();
        }
    }

    private boolean terminalAction() {
        boolean isTerminal = false;
        synchronized (this) {
            if (!terminated) {
                terminated = true;
                isTerminal = true;
            }
        }
        return isTerminal;
    }

}
