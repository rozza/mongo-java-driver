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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.client.OperationExecutor;
import com.mongodb.internal.operation.AsyncOperations;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListIndexesPublisher;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class ListIndexesPublisherImpl<T> extends MongoBatchCursorPublisher<T> implements ListIndexesPublisher<T> {

    private AsyncOperations<BsonDocument> operations;
    private final Class<T> resultClass;
    private long maxTimeMS;

    ListIndexesPublisherImpl(@Nullable final ClientSession clientSession, final MongoNamespace namespace, final Class<T> resultClass,
                             final CodecRegistry codecRegistry, final ReadPreference readPreference,
                             final OperationExecutor executor, final boolean retryReads) {
        super(clientSession, executor, ReadConcern.DEFAULT, readPreference, retryReads);
        this.operations = new AsyncOperations<>(namespace, BsonDocument.class, readPreference, codecRegistry, retryReads);
        this.resultClass = notNull("clazz", resultClass);
    }
    
    public ListIndexesPublisherImpl<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    public ListIndexesPublisherImpl<T> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }


    AsyncReadOperation<AsyncBatchCursor<T>> asAsyncReadOperation() {
        return operations.listIndexes(resultClass, getBatchSize(), maxTimeMS);
    }
}