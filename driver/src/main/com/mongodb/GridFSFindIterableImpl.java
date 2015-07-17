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

package com.mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.GridFSFindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import org.bson.conversions.Bson;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

class GridFSFindIterableImpl<TResult> implements GridFSFindIterable<TResult> {
    private final FindIterable<TResult> underlying;

    public GridFSFindIterableImpl(final FindIterable<TResult> underlying) {
        this.underlying = underlying;
    }

    @Override
    public GridFSFindIterable<TResult> sort(final Bson sort) {
        underlying.sort(sort);
        return this;
    }

    @Override
    public GridFSFindIterable<TResult> skip(final int skip) {
        underlying.skip(skip);
        return this;
    }

    @Override
    public GridFSFindIterable<TResult> limit(final int limit) {
        underlying.limit(limit);
        return this;
    }

    @Override
    public GridFSFindIterable<TResult> filter(final Bson filter) {
        underlying.filter(filter);
        return this;
    }

    @Override
    public GridFSFindIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        underlying.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public GridFSFindIterable<TResult> batchSize(final int batchSize) {
        underlying.batchSize(batchSize);
        return this;
    }

    @Override
    public GridFSFindIterable<TResult> noCursorTimeout(final boolean noCursorTimeout) {
        underlying.noCursorTimeout(noCursorTimeout);
        return this;
    }

    @Override
    public MongoCursor<TResult> iterator() {
        return underlying.iterator();
    }

    @Override
    public TResult first() {
        return underlying.first();
    }

    @Override
    public <U> MongoIterable<U> map(final Function<TResult, U> mapper) {
        return underlying.map(mapper);
    }

    @Override
    public void forEach(final Block<? super TResult> block) {
        underlying.forEach(block);
    }

    @Override
    public <A extends Collection<? super TResult>> A into(final A target) {
        return underlying.into(target);
    }
}
