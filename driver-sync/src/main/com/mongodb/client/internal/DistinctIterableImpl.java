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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.ClientSideOperationTimeoutFactories;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.SyncOperations;
import com.mongodb.lang.Nullable;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

class DistinctIterableImpl<TDocument, TResult> extends MongoIterableImpl<TResult> implements DistinctIterable<TResult> {
    private final SyncOperations<TDocument> operations;

    private final Class<TResult> resultClass;
    private final String fieldName;

    private Bson filter;
    private long maxTimeMS;

    private Collation collation;

    DistinctIterableImpl(@Nullable final ClientSession clientSession, final MongoNamespace namespace, final Class<TDocument> documentClass,
                         final Class<TResult> resultClass, final CodecRegistry codecRegistry, final ReadPreference readPreference,
                         final ReadConcern readConcern, final OperationExecutor executor, final String fieldName, final Bson filter,
                         @Nullable final Long timeoutMS) {
        this(clientSession, namespace, documentClass, resultClass, codecRegistry, readPreference, readConcern, executor, fieldName,
                filter, true, timeoutMS);
    }

    DistinctIterableImpl(@Nullable final ClientSession clientSession, final MongoNamespace namespace, final Class<TDocument> documentClass,
                         final Class<TResult> resultClass, final CodecRegistry codecRegistry, final ReadPreference readPreference,
                         final ReadConcern readConcern, final OperationExecutor executor, final String fieldName, final Bson filter,
                         final boolean retryReads, @Nullable final Long timeoutMS) {
        super(clientSession, executor, readConcern, readPreference, retryReads, timeoutMS);
        this.operations = new SyncOperations<TDocument>(namespace, documentClass, readPreference, codecRegistry, retryReads);
        this.resultClass = notNull("resultClass", resultClass);
        this.fieldName = notNull("mapFunction", fieldName);
        this.filter = filter;
    }

    @Override
    public DistinctIterable<TResult> filter(@Nullable final Bson filter) {
        this.filter = filter;
        return this;
    }

    @Deprecated
    @Override
    public DistinctIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public DistinctIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public DistinctIterable<TResult> collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public ReadOperation<BatchCursor<TResult>> asReadOperation() {
        return operations.distinct(ClientSideOperationTimeoutFactories.create(getTimeoutMS(), maxTimeMS), fieldName, filter, resultClass,
                                   collation);
    }
}
