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
import rx.Observable;
import rx.functions.Func1;

import java.util.List;

class MongoClientImpl implements MongoClient {

    private final com.mongodb.async.client.MongoClient wrapped;

    public MongoClientImpl(final com.mongodb.async.client.MongoClient wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public MongoDatabase getDatabase(final String name) {
        return new MongoDatabaseImpl(wrapped.getDatabase(name));
    }

    @Override
    public void close() {
        wrapped.close();
    }

    @Override
    public Observable<String> getDatabaseNames() {
        return Observable.concat(Observable.create(new OnSubscribeAdapter<List<String>>(
                                                     new OnSubscribeAdapter.FutureFunction<List<String>>() {
                                                         @Override
                                                         public MongoFuture<List<String>> apply() {
                                                             return wrapped.getDatabaseNames();
                                                         }
                                                     })
                                                   ).map(new Func1<List<String>, Observable<String>>() {
                                                            @Override
                                                            public Observable<String> call(final List<String> strings) {
                                                                return Observable.from(strings);
                                                            }
                                                        }));
    }
}
