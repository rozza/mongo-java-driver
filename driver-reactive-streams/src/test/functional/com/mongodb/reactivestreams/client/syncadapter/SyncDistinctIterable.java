/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.reactivestreams.client.syncadapter;

import com.mongodb.client.DistinctIterable;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.DistinctPublisher;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

class SyncDistinctIterable<T> extends SyncMongoIterable<T> implements DistinctIterable<T> {
    private final com.mongodb.reactivestreams.client.DistinctPublisher<T> wrapped;

    SyncDistinctIterable(final DistinctPublisher<T> wrapped) {
        super(wrapped);
        this.wrapped = wrapped;
    }

    @Override
    public DistinctIterable<T> filter(@Nullable final Bson filter) {
        wrapped.filter(filter);
        return this;
    }

    @Override
    public DistinctIterable<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public DistinctIterable<T> batchSize(final int batchSize) {
        wrapped.batchSize(batchSize);
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public DistinctIterable<T> collation(@Nullable final Collation collation) {
        wrapped.collation(collation);
        return this;
    }

    @Override
    public DistinctIterable<T> comment(@Nullable final String comment) {
        wrapped.comment(comment);
        return this;
    }

    @Override
    public DistinctIterable<T> comment(@Nullable final BsonValue comment) {
        wrapped.comment(comment);
        return this;
    }

    @Override
    public DistinctIterable<T> hint(@Nullable final Bson hint) {
        wrapped.hint(hint);
        return this;
    }

    @Override
    public DistinctIterable<T> hintString(@Nullable final String hint) {
        wrapped.hintString(hint);
        return this;
    }

    @Override
    public DistinctIterable<T> timeoutMode(final TimeoutMode timeoutMode) {
        wrapped.timeoutMode(timeoutMode);
        return this;
    }
}
