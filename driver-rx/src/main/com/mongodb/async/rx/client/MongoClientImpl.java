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

import com.mongodb.async.client.MongoClientOptions;
import com.mongodb.async.client.MongoDatabaseOptions;

class MongoClientImpl implements MongoClient {

    private final com.mongodb.async.client.MongoClient wrapped;

    public MongoClientImpl(final com.mongodb.async.client.MongoClient wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public MongoDatabase getDatabase(final String name) {
        return getDatabase(name, MongoDatabaseOptions.builder().build());
    }

    @Override
    public MongoDatabase getDatabase(final String name, final MongoDatabaseOptions mongoDatabaseOptions) {
        return new MongoDatabaseImpl(wrapped.getDatabase(name, mongoDatabaseOptions));
    }

    @Override
    public void close() {
        wrapped.close();
    }

    @Override
    public MongoClientOptions getOptions() {
        return wrapped.getOptions();
    }

    @Override
    public ClientAdministration tools() {
        return new ClientAdministrationImpl(wrapped.tools());
    }
}
