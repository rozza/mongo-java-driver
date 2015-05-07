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

import com.mongodb.MongoException;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;


final class MongoIterableSubscription<TResult> implements Subscription {

    private final MongoIterable<TResult> mongoIterable;
    private final Observer<? super TResult> observer;

    /* protected by `this` */
    private boolean requestedBatchCursor;
    private boolean isReading;
    private boolean isProcessing;
    private boolean cursorCompleted;
    private long requested = 0;
    private boolean isUnsubscribed = false;
    private boolean terminated = false;
    /* protected by `this` */

    private volatile AsyncBatchCursor<TResult> batchCursor;
    private final ConcurrentLinkedQueue<TResult> resultsQueue = new ConcurrentLinkedQueue<TResult>();

    public MongoIterableSubscription(final MongoIterable<TResult> mongoIterable, final Observer<TResult> observer) {
        this.mongoIterable = mongoIterable;
        this.observer = observer;
        observer.onSubscribe(this);
    }

    @Override
    public void unsubscribe() {
        boolean unsubscribe = false;

        synchronized (this) {
             if (!isUnsubscribed) {
                 unsubscribe = true;
                 isUnsubscribed = true;
                 terminated = true;
             }
        }

        if (unsubscribe && batchCursor != null) {
            batchCursor.close();
        }
    }

    @Override
    public boolean isUnsubscribed() {
        return isUnsubscribed;
    }

    @Override
    public void request(final long n) {
        if (n < 1) {
            throw new IllegalArgumentException("Number requested cannot be negative: " + n);
        }

        boolean mustGetCursor = false;
        synchronized (this) {
            requested += n;
            if (!requestedBatchCursor) {
                requestedBatchCursor = true;
                mustGetCursor = true;
            }
        }

        if (mustGetCursor) {
            getBatchCursor();
        } else {
            processResultsQueue();
        }
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
            boolean getNextBatch = false;

            long processedCount = 0;
            boolean completed = false;
            while (true) {
                long localWanted = 0;

                synchronized (this) {
                    requested -= processedCount;
                    if (resultsQueue.isEmpty()) {
                        completed = cursorCompleted;
                        getNextBatch = requested > 0;
                        isProcessing = false;
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
            } else if (getNextBatch) {
                getNextBatch();
            }
        }
    }

    private void getNextBatch() {
        boolean mustRead = false;
        synchronized (this) {
            if (!isReading && !terminated && batchCursor != null) {
                isReading = true;
                mustRead = true;
            }
        }

        if (mustRead) {
            final MongoIterableSubscription<TResult> iterableObservable = this;
            batchCursor.setBatchSize(getBatchSize());
            batchCursor.next(new SingleResultCallback<List<TResult>>() {
                @Override
                public void onResult(final List<TResult> result, final Throwable t) {

                    synchronized (iterableObservable) {
                        isReading = false;
                        if (t == null && result == null) {
                            cursorCompleted = true;
                        }
                    }

                    if (t != null) {
                        onError(t);
                    } else {
                        if (result != null) {
                            resultsQueue.addAll(result);
                        }
                        if (checkSubscription()) {
                            processResultsQueue();
                        }
                    }
                }
            });
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

    private boolean checkSubscription() {
        boolean subscribed = true;
        synchronized (this) {
            if (isUnsubscribed) {
                subscribed = false;
                if (batchCursor != null && !batchCursor.isClosed()) {
                    batchCursor.close();
                }
            }
        }
        return subscribed;
    }

    private void getBatchCursor() {
        mongoIterable.batchSize(getBatchSize());
        mongoIterable.batchCursor(new SingleResultCallback<AsyncBatchCursor<TResult>>() {
            @Override
            public void onResult(final AsyncBatchCursor<TResult> result, final Throwable t) {
                if (t != null) {
                    onError(t);
                } else if (result != null) {
                    batchCursor = result;
                    if (checkSubscription()) {
                        getNextBatch();
                    }
                } else {
                    onError(new MongoException("Unexpected error, no AsyncBatchCursor returned from the MongoIterable."));
                }
            }
        });
    }

    /**
     * Returns the batchSize to be used with the cursor.
     *
     * <p>Anything less than 2 would close the cursor so that is the minimum batchSize and `Integer.MAX_VALUE` is the maximum
     * batchSize.</p>
     *
     * @return the batchSize to use
     */
    private int getBatchSize() {
        long requested = this.requested;
        if (requested <= 1) {
            return 2;
        } else if (requested < Integer.MAX_VALUE) {
            return (int) requested;
        } else {
            return Integer.MAX_VALUE;
        }
    }
}
