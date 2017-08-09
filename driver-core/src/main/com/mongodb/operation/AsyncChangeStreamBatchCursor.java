/*
 * Copyright 2017 MongoDB, Inc.
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

package com.mongodb.operation;

import com.mongodb.MongoChangeStreamException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoServerException;
import com.mongodb.ServerCursor;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncReadBinding;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static java.util.Collections.singletonList;

final class AsyncChangeStreamBatchCursor<T> implements AsyncBatchCursor<T> {
    private static final BsonDocumentCodec BSON_DOCUMENT_CODEC = new BsonDocumentCodec();
    private final AsyncReadBinding binding;
    private final ChangeStreamOperation<T> changeStreamOperation;
    private final AtomicBoolean inFlight = new AtomicBoolean();

    private volatile AsyncQueryBatchCursor<RawBsonDocument> wrapped;
    private volatile BsonDocument resumeToken;

    AsyncChangeStreamBatchCursor(final ChangeStreamOperation<T> changeStreamOperation,
                                 final AsyncReadBinding binding,
                                 final AsyncQueryBatchCursor<RawBsonDocument> wrapped) {
        this.changeStreamOperation = changeStreamOperation;
        this.resumeToken = changeStreamOperation.getResumeToken();
        this.wrapped = wrapped;
        this.binding = binding;
    }

    AsyncQueryBatchCursor<RawBsonDocument> getWrapped() {
        return wrapped;
    }

    ServerCursor getServerCursor() {
        return wrapped.getServerCursor();
    }

    @Override
    public void next(final SingleResultCallback<List<T>> callback) {
        resumeAbleOperation(new AsyncBlock() {
            @Override
            public void apply(final AsyncQueryBatchCursor<RawBsonDocument> cursor,
                              final SingleResultCallback<List<RawBsonDocument>> callback) {
                cursor.next(callback);
            }
        }, convertResultsCallback(callback));
    }

    @Override
    public void tryNext(final SingleResultCallback<List<T>> callback) {
        resumeAbleOperation(new AsyncBlock() {
            @Override
            public void apply(final AsyncQueryBatchCursor<RawBsonDocument> cursor,
                              final SingleResultCallback<List<RawBsonDocument>> callback) {
                cursor.tryNext(callback);
            }
        }, convertResultsCallback(callback));
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

    private interface AsyncBlock {
        void apply(AsyncQueryBatchCursor<RawBsonDocument> cursor, SingleResultCallback<List<RawBsonDocument>> callback);
    }

    private void resumeAbleOperation(final AsyncBlock asyncBlock, final SingleResultCallback<List<RawBsonDocument>> callback) {
        boolean msgInFlight = inFlight.getAndSet(true);
        if (msgInFlight) {
            callback.onResult(null,
                    new MongoChangeStreamException("Another asynchronous operation on this Changes Stream is already in progress"));
            return;
        }

        asyncBlock.apply(wrapped, new SingleResultCallback<List<RawBsonDocument>>() {
            @Override
            public void onResult(final List<RawBsonDocument> result, final Throwable t) {
                inFlight.set(false);
                if (t == null || !changeStreamOperation.isResumable()) {
                    callback.onResult(result, t);
                } else if (t instanceof MongoServerException && !(t instanceof MongoNotPrimaryException
                        || t instanceof MongoQueryException)) {
                    callback.onResult(null, t);
                } else {
                    killCursorAndRetryOperation(asyncBlock, callback);
                }
            }
        });
    }

    private void killCursorAndRetryOperation(final AsyncBlock asyncBlock, final SingleResultCallback<List<RawBsonDocument>> callback) {
        ServerCursor serverCursor = wrapped.getServerCursor();
        if (serverCursor == null) {
            retryOperation(asyncBlock, callback);
        } else {
            MongoNamespace namespace = changeStreamOperation.getNamespace();
            BsonDocument command = new BsonDocument("killCursors", new BsonString(namespace.getCollectionName()))
                    .append("cursors", new BsonArray(singletonList(new BsonInt64(serverCursor.getId()))));
            executeWrappedCommandProtocolAsync(binding, namespace.getDatabaseName(), command, BSON_DOCUMENT_CODEC,
                    new SingleResultCallback<BsonDocument>() {
                        @Override
                        public void onResult(final BsonDocument result, final Throwable t) {
                            // Ignore any exceptions killing old cursors
                            retryOperation(asyncBlock, callback);
                        }
                    });
        }
    }

    private void retryOperation(final AsyncBlock asyncBlock, final SingleResultCallback<List<RawBsonDocument>> callback) {
        new ChangeStreamOperation<T>(changeStreamOperation, resumeToken)
                .executeAsync(binding, new SingleResultCallback<AsyncBatchCursor<T>>() {
                    @Override
                    public void onResult(final AsyncBatchCursor<T> result, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            wrapped = ((AsyncChangeStreamBatchCursor<T>) result).getWrapped();
                            asyncBlock.apply(wrapped, callback);
                        }
                    }
                });
    }

    private SingleResultCallback<List<RawBsonDocument>> convertResultsCallback(final SingleResultCallback<List<T>> callback) {
        return errorHandlingCallback(new SingleResultCallback<List<RawBsonDocument>>() {
            @Override
            public void onResult(final List<RawBsonDocument> rawDocuments, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else if (rawDocuments != null) {
                    List<T> results = new ArrayList<T>();
                    for (RawBsonDocument rawDocument : rawDocuments) {
                        resumeToken = rawDocument.getDocument("_id");
                        results.add(rawDocument.decode(changeStreamOperation.getCodec()));
                    }
                    callback.onResult(results, null);
                } else {
                    callback.onResult(null, null);
                }
            }
        }, LOGGER);
    }
}
