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
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import org.mongodb.Document;
import rx.Observable;
import rx.functions.Func1;

import java.util.List;

/**
 * Provides the functionality for a collection that is useful for administration, but not necessarily in the course of normal use of a
 * collection.
 *
 * @since 3.0
 */
public class CollectionAdministrationImpl implements CollectionAdministration {

    private final com.mongodb.async.client.CollectionAdministration wrapped;

    CollectionAdministrationImpl(final com.mongodb.async.client.CollectionAdministration wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public Observable<Void> createIndex(final Document key) {
        return createIndex(key, new CreateIndexOptions());
    }

    @Override
    public Observable<Void> createIndex(final Document key, final CreateIndexOptions createIndexOptions) {
        return Observable.create(new OnSubscribeAdapter<Void>(new OnSubscribeAdapter.FutureFunction<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.createIndex(key, createIndexOptions);
            }
        }));
    }

    @Override
    public Observable<Document> getIndexes() {
        return Observable.concat(
            Observable.create(
                 new OnSubscribeAdapter<List<Document>>(
                     new OnSubscribeAdapter.FutureFunction<List<Document>>() {
                         @Override
                         public MongoFuture<List<Document>>
                         apply() {
                             return wrapped.getIndexes();
                         }
                     }
                 )
            ).map(new Func1<List<Document>, Observable<Document>>() {
                @Override
                public Observable<Document> call(final List<Document> documents) {
                    return Observable.from(documents);
                }
            })
        );
    }

    @Override
    public Observable<Void> drop() {
        return Observable.create(new OnSubscribeAdapter<Void>(new OnSubscribeAdapter.FutureFunction<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.drop();
            }
        }));
    }

    @Override
    public Observable<Void> dropIndex(final String indexName) {
        return Observable.create(new OnSubscribeAdapter<Void>(new OnSubscribeAdapter.FutureFunction<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.dropIndex(indexName);
            }
        }));
    }

    @Override
    public Observable<Void> dropIndexes() {
        return Observable.create(new OnSubscribeAdapter<Void>(new OnSubscribeAdapter.FutureFunction<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.dropIndexes();
            }
        }));
    }

    @Override
    public Observable<Void> renameCollection(final MongoNamespace newCollectionNamespace) {
        return renameCollection(newCollectionNamespace, new RenameCollectionOptions());
    }

    @Override
    public Observable<Void> renameCollection(final MongoNamespace newCollectionNamespace,
                                             final RenameCollectionOptions renameCollectionOptions) {
        return Observable.create(new OnSubscribeAdapter<Void>(new OnSubscribeAdapter.FutureFunction<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.renameCollection(newCollectionNamespace, renameCollectionOptions);
            }
        }));
    }
}
