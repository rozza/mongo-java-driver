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

import com.mongodb.MongoNamespace;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.client.MongoCollectionOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import org.mongodb.Document;
import rx.Observable;
import rx.functions.Func1;

import java.util.List;

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
        return new MongoCollectionImpl<Document>(wrapped.getCollection(name));
    }

    @Override
    public MongoCollection<Document> getCollection(final String name, final MongoCollectionOptions options) {
        return new MongoCollectionImpl<Document>(wrapped.getCollection(name, options));
    }

    @Override
    public <T> MongoCollection<T> getCollection(final String name, final Class<T> clazz, final MongoCollectionOptions options) {
        return new MongoCollectionImpl<T>(wrapped.getCollection(name, clazz, options));
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
    public Observable<Void> dropDatabase() {
        return Observable.create(new OnSubscribeAdapter<Void>(new OnSubscribeAdapter.FutureFunction<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.dropDatabase();
            }
        }));
    }

    @Override
    public Observable<String> getCollectionNames() {
        return Observable.concat(Observable.create(
                new OnSubscribeAdapter<List<String>>(
                           new OnSubscribeAdapter.FutureFunction<List<String>>() {
                               @Override
                               public MongoFuture<List<String>> apply() {
                                   return wrapped.getCollectionNames();
                               }
                           })
                ).map(new Func1<List<String>, Observable<String>>() {
                            @Override
                            public Observable<String> call(final List<String> strings) {
                                return Observable.from(strings);
                            }
                     })
        );
    }

    @Override
    public Observable<Void> createCollection(final String collectionName) {
        return Observable.create(new OnSubscribeAdapter<Void>(new OnSubscribeAdapter.FutureFunction<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.createCollection(collectionName);
            }
        }));
    }

    @Override
    public Observable<Void> createCollection(final String collectionName, final CreateCollectionOptions createCollectionOptions) {
        return Observable.create(new OnSubscribeAdapter<Void>(new OnSubscribeAdapter.FutureFunction<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.createCollection(collectionName, createCollectionOptions);
            }
        }));
    }

    @Override
    public Observable<Void> renameCollection(final MongoNamespace originalNamespace, final MongoNamespace newNamespace) {
        return Observable.create(new OnSubscribeAdapter<Void>(new OnSubscribeAdapter.FutureFunction<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.renameCollection(originalNamespace, newNamespace);
            }
        }));
    }

    @Override
    public Observable<Void> renameCollection(final MongoNamespace originalNamespace, final MongoNamespace newNamespace,
                                             final RenameCollectionOptions renameCollectionOptions) {
        return Observable.create(new OnSubscribeAdapter<Void>(new OnSubscribeAdapter.FutureFunction<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.renameCollection(originalNamespace, newNamespace, renameCollectionOptions);
            }
        }));
    }

}