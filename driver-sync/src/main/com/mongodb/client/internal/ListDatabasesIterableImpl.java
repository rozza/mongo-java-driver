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
package com.mongodb.client.internal;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.model.TimeoutMode;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.SyncOperations;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public class ListDatabasesIterableImpl<TResult> extends MongoIterableImpl<TResult> implements ListDatabasesIterable<TResult> {
    private final SyncOperations<BsonDocument> operations;
    private final Class<TResult> resultClass;

    private long maxTimeMS;
    private Bson filter;
    private Boolean nameOnly;
    private Boolean authorizedDatabasesOnly;

    ListDatabasesIterableImpl(@Nullable final ClientSession clientSession, final Class<TResult> resultClass,
                              final CodecRegistry codecRegistry, final ReadPreference readPreference,
                              final OperationExecutor executor, @Nullable final Long timeoutMS) {
        this(clientSession, resultClass, codecRegistry, readPreference, executor, true, timeoutMS);
    }

    public ListDatabasesIterableImpl(@Nullable final ClientSession clientSession, final Class<TResult> resultClass,
                                     final CodecRegistry codecRegistry, final ReadPreference readPreference,
                                     final OperationExecutor executor, final boolean retryReads, @Nullable final Long timeoutMS) {
        super(clientSession, executor, ReadConcern.DEFAULT, readPreference, retryReads, timeoutMS);
        this.operations = new SyncOperations<BsonDocument>(BsonDocument.class, readPreference, codecRegistry, retryReads);
        this.resultClass = notNull("clazz", resultClass);
    }

    @Deprecated
    @Override
    public ListDatabasesIterableImpl<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> timeoutMode(final TimeoutMode timeoutMode) {
        super.timeoutMode(timeoutMode);
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> filter(@Nullable final Bson filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> nameOnly(@Nullable final Boolean nameOnly) {
        this.nameOnly = nameOnly;
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> authorizedDatabasesOnly(@Nullable final Boolean authorizedDatabasesOnly) {
        this.authorizedDatabasesOnly = authorizedDatabasesOnly;
        return this;
    }

    @Override
    public ReadOperation<BatchCursor<TResult>> asReadOperation() {
        return operations.listDatabases(getClientSideOperationTimeout(maxTimeMS), resultClass, filter, nameOnly, authorizedDatabasesOnly);
    }
}
