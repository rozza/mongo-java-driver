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
import com.mongodb.internal.async.client.OperationExecutor;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Mono;

import static com.mongodb.reactivestreams.client.internal.PublisherCreator.getSession;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.sinkToCallback;

public class ReadOperationPublisher<T> implements Publisher<T> {

    private final AsyncReadOperation<T> operation;
    private @Nullable
    final ClientSession clientSession;
    private final ReadPreference readPreference;
    private final ReadConcern readConcern;
    private final OperationExecutor executor;

    public ReadOperationPublisher(
            final AsyncReadOperation<T> operation,
            @Nullable final ClientSession clientSession,
            final ReadPreference readPreference,
            final ReadConcern readConcern,
            final OperationExecutor executor) {
        this.operation = operation;
        this.clientSession = clientSession;
        this.readPreference = readPreference;
        this.readConcern = readConcern;
        this.executor = executor;
    }

    @Override
    public void subscribe(final Subscriber<? super T> subscriber) {
        Mono.<T>create(sink -> executor.execute(operation, readPreference, readConcern, getSession(clientSession), sinkToCallback(sink)))
                .subscribe(subscriber);
    }

}
