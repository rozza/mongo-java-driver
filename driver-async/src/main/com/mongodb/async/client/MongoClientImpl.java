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

package com.mongodb.async.client;

import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.binding.AsyncClusterBinding;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.AsyncReadWriteBinding;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.connection.Cluster;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.AsyncReadOperation;
import com.mongodb.operation.AsyncWriteOperation;
import com.mongodb.operation.GetDatabaseNamesOperation;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;

class MongoClientImpl implements MongoClient {
    private final Cluster cluster;
    private final MongoClientSettings settings;

    MongoClientImpl(final MongoClientSettings settings, final Cluster cluster) {
        this.settings = settings;
        this.cluster = cluster;
    }

    @Override
    public MongoDatabase getDatabase(final String name) {
        return getDatabase(name, MongoDatabaseOptions.builder()
                                                     .readPreference(settings.getReadPreference())
                                                     .writeConcern(settings.getWriteConcern())
                                                     .build()
                                                     .withDefaults(settings));
    }


    @Override
    public MongoDatabase getDatabase(final String databaseName, final MongoDatabaseOptions options) {
        return new MongoDatabaseImpl(databaseName, options, executor);
    }

    @Override
    public void close() {
        cluster.close();
    }

    @Override
    public MongoFuture<List<String>> getDatabaseNames() {
        return executor.execute(new GetDatabaseNamesOperation(), primary());
    }

    Cluster getCluster() {
        return cluster;
    }

    <V> MongoFuture<V> execute(final AsyncWriteOperation<V> writeOperation) {
        final SingleResultFuture<V> future = new SingleResultFuture<V>();
        final AsyncWriteBinding binding = getWriteBinding();
        writeOperation.executeAsync(binding).register(new SingleResultCallback<V>() {
            @Override
            public void onResult(final V result, final MongoException e) {
                try {
                    if (e != null) {
                        future.init(null, e);
                    } else {
                        future.init(result, null);
                    }
                } finally {
                    binding.release();
                }
            }
        });
        return future;
    }

    <V> MongoFuture<V> execute(final AsyncReadOperation<V> readOperation, final ReadPreference readPreference) {
        final SingleResultFuture<V> future = new SingleResultFuture<V>();
        final AsyncReadBinding binding = getReadBinding(readPreference);
        readOperation.executeAsync(binding).register(new SingleResultCallback<V>() {
            @Override
            public void onResult(final V result, final MongoException e) {
                try {
                    if (e != null) {
                        future.init(null, e);
                    } else {
                        future.init(result, null);
                    }
                } finally {
                    binding.release();
                }
            }
        });
        return future;
    }

    private AsyncOperationExecutor executor = new AsyncOperationExecutor() {
        @Override
        public <T> MongoFuture<T> execute(final AsyncReadOperation<T> operation, final ReadPreference readPreference) {
            return MongoClientImpl.this.execute(operation, readPreference);
        }

        @Override
        public <T> MongoFuture<T> execute(final AsyncWriteOperation<T> operation) {
            return MongoClientImpl.this.execute(operation);
        }
    };

    private AsyncWriteBinding getWriteBinding() {
        return getReadWriteBinding(ReadPreference.primary());
    }

    private AsyncReadBinding getReadBinding(final ReadPreference readPreference) {
        return getReadWriteBinding(readPreference);
    }

    private AsyncReadWriteBinding getReadWriteBinding(final ReadPreference readPreference) {
        notNull("readPreference", readPreference);
        return new AsyncClusterBinding(cluster, readPreference, settings.getConnectionPoolSettings().getMaxWaitTime(TimeUnit.MILLISECONDS),
                                       TimeUnit.MILLISECONDS);
    }

}
