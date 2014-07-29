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

package org.mongodb.async.rxjava;

import org.bson.codecs.Codec;
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MongoCollectionOptions;
import org.mongodb.MongoFuture;
import rx.Observable;

class MongoDatabaseImpl implements MongoDatabase {
    private final org.mongodb.async.MongoDatabase wrapped;

    public MongoDatabaseImpl(final org.mongodb.async.MongoDatabase wrapped) {
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
    public <T> MongoCollection<T> getCollection(final String name, final Codec<T> codec, final MongoCollectionOptions options) {
        return new MongoCollectionImpl<T>(wrapped.getCollection(name, codec, options));
    }

    @Override
    public void requestStart() {
        wrapped.requestStart();
    }

    @Override
    public void requestDone() {
        wrapped.requestDone();
    }

    @Override
    public Observable<CommandResult> executeCommand(final Document commandDocument) {
         return Observable.create(new OnSubscribeAdapter<CommandResult>(new OnSubscribeAdapter.FutureFunction<CommandResult>() {
             @Override
             public MongoFuture<CommandResult> apply() {
                 return wrapped.executeCommand(commandDocument);
             }
         }));
    }

    @Override
    public DatabaseAdministration tools() {
        return new DatabaseAdministrationImpl(wrapped.tools());
    }
}
