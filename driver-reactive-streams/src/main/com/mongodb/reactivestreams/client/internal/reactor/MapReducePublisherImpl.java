/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.MapReduceAction;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.client.AsyncClientSession;
import com.mongodb.internal.async.client.OperationExecutor;
import com.mongodb.internal.async.client.WriteOperationThenCursorReadOperation;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.client.model.FindOptions;
import com.mongodb.internal.operation.AsyncOperations;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.internal.operation.AsyncWriteOperation;
import com.mongodb.internal.operation.MapReduceAsyncBatchCursor;
import com.mongodb.internal.operation.MapReduceStatistics;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MapReducePublisher;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;

final class MapReducePublisherImpl<D, T> extends MongoBatchCursorPublisher<T> implements MapReducePublisher<T> {

    private final AsyncOperations<D> operations;
    private final MongoNamespace namespace;
    private final Class<T> resultClass;
    private final String mapFunction;
    private final String reduceFunction;

    private boolean inline = true;
    private String collectionName;
    private String finalizeFunction;
    private Bson scope;
    private Bson filter;
    private Bson sort;
    private int limit;
    private boolean jsMode;
    private boolean verbose = true;
    private long maxTimeMS;
    private MapReduceAction action = MapReduceAction.REPLACE;
    private String databaseName;
    private boolean sharded;
    private boolean nonAtomic;
    private Boolean bypassDocumentValidation;
    private Collation collation;

    MapReducePublisherImpl(@Nullable final ClientSession clientSession,
                           final MongoNamespace namespace,
                           final Class<D> documentClass,
                           final Class<T> resultClass,
                           final CodecRegistry codecRegistry,
                           final ReadPreference readPreference,
                           final ReadConcern readConcern,
                           final WriteConcern writeConcern,
                           final OperationExecutor executor,
                           final String mapFunction,
                           final String reduceFunction) {
        super(clientSession, executor, readConcern, readPreference, false);
        this.operations = new AsyncOperations<>(namespace, documentClass, readPreference, codecRegistry, readConcern, writeConcern,
                false, false);
        this.namespace = notNull("namespace", namespace);
        this.resultClass = notNull("resultClass", resultClass);
        this.mapFunction = notNull("mapFunction", mapFunction);
        this.reduceFunction = notNull("reduceFunction", reduceFunction);
    }

    @Override
    public MapReducePublisher<T> collectionName(final String collectionName) {
        this.collectionName = notNull("collectionName", collectionName);
        this.inline = false;
        return this;
    }

    @Override
    public MapReducePublisher<T> finalizeFunction(@Nullable final String finalizeFunction) {
        this.finalizeFunction = finalizeFunction;
        return this;
    }

    @Override
    public MapReducePublisher<T> scope(@Nullable final Bson scope) {
        this.scope = scope;
        return this;
    }

    @Override
    public MapReducePublisher<T> sort(@Nullable final Bson sort) {
        this.sort = sort;
        return this;
    }

    @Override
    public MapReducePublisher<T> filter(@Nullable final Bson filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public MapReducePublisher<T> limit(final int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public MapReducePublisher<T> jsMode(final boolean jsMode) {
        this.jsMode = jsMode;
        return this;
    }

    @Override
    public MapReducePublisher<T> verbose(final boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    @Override
    public MapReducePublisher<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public MapReducePublisher<T> action(final MapReduceAction action) {
        this.action = action;
        return this;
    }

    @Override
    public MapReducePublisher<T> databaseName(@Nullable final String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    @Deprecated
    @Override
    public MapReducePublisher<T> sharded(final boolean sharded) {
        this.sharded = sharded;
        return this;
    }

    @Deprecated
    @Override
    public MapReducePublisher<T> nonAtomic(final boolean nonAtomic) {
        this.nonAtomic = nonAtomic;
        return this;
    }

    @Override
    public MapReducePublisher<T> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public MapReducePublisher<T> bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public Publisher<Void> toCollection() {
        if (inline) {
            throw new IllegalStateException("The options must specify a non-inline result");
        }
        return Mono.create(callback -> {
            AsyncClientSession session = getClientSession() != null ? getClientSession().getWrapped() : null;
            getExecutor().execute(createMapReduceToCollectionOperation().getOperation(), getReadConcern(), session,
                    ((MapReduceStatistics result, Throwable t) -> {
                        if (t != null) {
                            callback.error(t);
                        } else {
                            callback.success();
                        }
                    }));
        });
    }

    @Override
    public MapReducePublisher<T> collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    ReadPreference getReadPreference() {
        if (inline) {
            return super.getReadPreference();
        } else {
            return primary();
        }
    }

    @Override
    AsyncReadOperation<AsyncBatchCursor<T>> asAsyncReadOperation() {
        if (inline) {
            return createMapReduceInlineOperation();
        } else {
            return new WriteOperationThenCursorReadOperation<>(createMapReduceToCollectionOperation(), createFindOperation());
        }
    }

    private WrappedMapReduceReadOperation<T> createMapReduceInlineOperation() {
        return new WrappedMapReduceReadOperation<T>(operations.mapReduce(mapFunction, reduceFunction, finalizeFunction,
                resultClass, filter, limit, maxTimeMS, jsMode, scope, sort, verbose, collation));
    }

    private WrappedMapReduceWriteOperation createMapReduceToCollectionOperation() {
        return new WrappedMapReduceWriteOperation(operations.mapReduceToCollection(databaseName, collectionName, mapFunction,
                reduceFunction, finalizeFunction, filter, limit, maxTimeMS, jsMode, scope, sort, verbose, action, nonAtomic, sharded,
                bypassDocumentValidation, collation));
    }

    private AsyncReadOperation<AsyncBatchCursor<T>> createFindOperation() {
        String dbName = databaseName != null ? databaseName : namespace.getDatabaseName();
        FindOptions findOptions = new FindOptions().collation(collation);
        Integer batchSize = getBatchSize();
        if (batchSize != null) {
            findOptions.batchSize(batchSize);
        }
        return operations.find(new MongoNamespace(dbName, collectionName), new BsonDocument(), resultClass, findOptions);
    }

    // this could be inlined, but giving it a name so that it's unit-testable
    static class WrappedMapReduceReadOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>> {
        private final AsyncReadOperation<MapReduceAsyncBatchCursor<T>> operation;

        AsyncReadOperation<MapReduceAsyncBatchCursor<T>> getOperation() {
            return operation;
        }

        WrappedMapReduceReadOperation(final AsyncReadOperation<MapReduceAsyncBatchCursor<T>> operation) {
            this.operation = operation;
        }

        @Override
        public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
            operation.executeAsync(binding, callback::onResult);
        }
    }

    static class WrappedMapReduceWriteOperation implements AsyncWriteOperation<Void> {
        private final AsyncWriteOperation<MapReduceStatistics> operation;

        AsyncWriteOperation<MapReduceStatistics> getOperation() {
            return operation;
        }

        WrappedMapReduceWriteOperation(final AsyncWriteOperation<MapReduceStatistics> operation) {
            this.operation = operation;
        }

        @Override
        public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
            operation.executeAsync(binding, (result, t) -> callback.onResult(null, t));
        }
    }
}
