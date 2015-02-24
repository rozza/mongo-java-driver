/*
 * Copyright 2015 MongoDB, Inc.
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
package com.mongodb;

import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.internal.codecs.RootCodecRegistry;
import com.mongodb.operation.ListDatabasesOperation;
import com.mongodb.operation.OperationExecutor;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.Codec;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


final class ListDatabasesIterableImpl<T> implements ListDatabasesIterable<T> {
    private final Class<T> clazz;
    private final ReadPreference readPreference;
    private final RootCodecRegistry codecRegistry;
    private final OperationExecutor executor;

    private long maxTimeMS;

    ListDatabasesIterableImpl(final Class<T> clazz, final RootCodecRegistry codecRegistry,
                              final ReadPreference readPreference, final OperationExecutor executor) {
        this.clazz = notNull("clazz", clazz);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.executor = notNull("executor", executor);
    }

    @Override
    public ListDatabasesIterableImpl<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public MongoCursor<T> iterator() {
        return execute().iterator();
    }

    @Override
    public T first() {
        return execute().first();
    }

    @Override
    public <U> MongoIterable<U> map(final Function<T, U> mapper) {
        return new MappingIterable<T, U>(this, mapper);
    }

    @Override
    public void forEach(final Block<? super T> block) {
        execute().forEach(block);
    }

    @Override
    public <A extends Collection<? super T>> A into(final A target) {
        return execute().into(target);
    }

    @Override
    public ListDatabasesIterable<T> batchSize(final int batchSize) {
        // Noop - not supported by listDatabasesIterable
        return this;
    }

    private MongoIterable<T> execute() {
        return new OperationIterable<T>(createListCollectionsOperation(), readPreference, executor);
    }

    private <C> Codec<C> getCodec(final Class<C> clazz) {
        return codecRegistry.get(clazz);
    }

    private ListDatabasesOperation<T> createListCollectionsOperation() {
        return new ListDatabasesOperation<T>(getCodec(clazz)).maxTime(maxTimeMS, MILLISECONDS);
    }

    private BsonDocument asBson(final Object document) {
        return BsonDocumentWrapper.asBsonDocument(document, codecRegistry);
    }

}
