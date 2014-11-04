/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.async.rx.client;

import com.mongodb.async.MongoFuture;
import com.mongodb.async.client.MongoCollectionOptions;
import com.mongodb.async.client.MongoDatabaseOptions;
import org.bson.Document;
import org.bson.codecs.Codec;
import rx.Observable;

class MongoDatabaseImpl implements MongoDatabase {
    private final com.mongodb.async.client.MongoDatabase wrapped;

    public MongoDatabaseImpl(final com.mongodb.async.client.MongoDatabase wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public String getName() {
        return wrapped.getName();
    }

    @Override
    public MongoCollection<Document> getCollection(final String name) {
        return getCollection(name, MongoCollectionOptions.builder().build());
    }

    @Override
    public MongoCollection<Document> getCollection(final String name, final MongoCollectionOptions mongoCollectionOptions) {
        return new MongoCollectionImpl<Document>(wrapped.getCollection(name, mongoCollectionOptions));
    }

    @Override
    public <T> MongoCollection<T> getCollection(final String name, final Codec<T> codec, final MongoCollectionOptions options) {
        return new MongoCollectionImpl<T>(wrapped.getCollection(name, codec, options));
    }

    @Override
    public Observable<Document> executeCommand(final Document commandDocument) {
        return Observable.create(new OnSubscribeAdapter<Document>(new OnSubscribeAdapter.FutureFunction<Document>() {
            @Override
            public MongoFuture<Document> apply() {
                return wrapped.executeCommand(commandDocument);
            }
        }));
    }

    @Override
    public MongoDatabaseOptions getOptions() {
        return wrapped.getOptions();
    }

    @Override
    public DatabaseAdministration tools() {
        return new DatabaseAdministrationImpl(wrapped.tools());
    }
}
