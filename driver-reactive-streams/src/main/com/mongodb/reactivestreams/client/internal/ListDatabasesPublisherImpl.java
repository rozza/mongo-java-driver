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
import com.mongodb.internal.operation.AsyncOperations;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListDatabasesPublisher;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class ListDatabasesPublisherImpl<T> extends BatchCursorPublisherImpl<T> implements ListDatabasesPublisher<T> {

    private final Class<T> resultClass;
    private final AsyncOperations<BsonDocument> operations;
    private long maxTimeMS;
    private Bson filter;
    private Boolean nameOnly;
    private Boolean authorizedDatabasesOnly;

    ListDatabasesPublisherImpl(@Nullable final ClientSession clientSession, final Class<T> resultClass,
                               final CodecRegistry codecRegistry, final ReadPreference readPreference,
                               final OperationExecutor executor, final boolean retryReads) {
        super(clientSession, executor, ReadConcern.DEFAULT, readPreference, retryReads);
        this.operations = new AsyncOperations<>(BsonDocument.class, readPreference, codecRegistry, retryReads);
        this.resultClass = notNull("clazz", resultClass);
    }

    public ListDatabasesPublisherImpl<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    public ListDatabasesPublisherImpl<T> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    public ListDatabasesPublisherImpl<T> filter(@Nullable final Bson filter) {
        this.filter = filter;
        return this;
    }

    public ListDatabasesPublisherImpl<T> nameOnly(@Nullable final Boolean nameOnly) {
        this.nameOnly = nameOnly;
        return this;
    }

    public ListDatabasesPublisherImpl<T> authorizedDatabasesOnly(@Nullable final Boolean authorizedDatabasesOnly) {
        this.authorizedDatabasesOnly = authorizedDatabasesOnly;
        return this;
    }

    AsyncReadOperation<AsyncBatchCursor<T>> asAsyncReadOperation() {
        return operations.listDatabases(resultClass, filter, nameOnly, maxTimeMS, authorizedDatabasesOnly);
    }
}
