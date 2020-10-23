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

package com.mongodb.reactivestreams.client.internal.reactor;

import com.mongodb.ClientSessionOptions;
import com.mongodb.TransactionOptions;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.client.AsyncClientSession;
import com.mongodb.internal.async.client.OperationExecutor;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getAny;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getAnyPrimaryOrSecondary;

public class ClientSessionHelper {
    private final MongoClientImpl mongoClient;
    private final ServerSessionPool serverSessionPool;

    public ClientSessionHelper(final MongoClientImpl mongoClient, final ServerSessionPool serverSessionPool) {
        this.mongoClient = mongoClient;
        this.serverSessionPool = serverSessionPool;
    }

    void withClientSession(@Nullable final AsyncClientSession clientSessionFromOperation, final OperationExecutor executor,
                           final SingleResultCallback<AsyncClientSession> callback) {
        if (clientSessionFromOperation != null) {
            isTrue("ClientSession from same MongoClient", clientSessionFromOperation.getOriginator() == mongoClient);
            callback.onResult(clientSessionFromOperation, null);
        } else {
            Mono.from(createClientSession(ClientSessionOptions.builder().causallyConsistent(false).build(), executor))
                    .subscribe(clientSession -> callback.onResult(clientSession.getWrapped(), null),
                            throwable -> callback.onResult(null, throwable));
        }
    }

    Publisher<ClientSession> createClientSession(final ClientSessionOptions options, final OperationExecutor executor) {
        ClusterDescription clusterDescription = mongoClient.getCluster().getCurrentDescription();
        if (!getServerDescriptionListToConsiderForSessionSupport(clusterDescription).isEmpty()
                && clusterDescription.getLogicalSessionTimeoutMinutes() != null) {
            return createClientSessionPublisher(options, executor);
        } else {
            return Mono
                    .create((MonoSink<Publisher<ClientSession>> sink) -> {
                        mongoClient.getCluster().selectServerAsync(this::getServerDescriptionListToConsiderForSessionSupport,
                                (server, t) -> {
                                    if (t != null) {
                                        sink.success();
                                    } else if (server.getDescription().getLogicalSessionTimeoutMinutes() == null) {
                                        sink.success();
                                    } else {
                                        sink.success(createClientSessionPublisher(options, executor));
                                    }
                                });
                    })
                    .flatMap(Mono::from);
        }
    }

    private Publisher<ClientSession> createClientSessionPublisher(final ClientSessionOptions options, final OperationExecutor executor) {
        return Mono.fromCallable(() -> {
            ClientSessionOptions mergedOptions = ClientSessionOptions.builder(options)
                    .defaultTransactionOptions(
                            TransactionOptions.merge(
                                    options.getDefaultTransactionOptions(),
                                    TransactionOptions.builder()
                                            .readConcern(mongoClient.getSettings().getReadConcern())
                                            .writeConcern(mongoClient.getSettings().getWriteConcern())
                                            .readPreference(mongoClient.getSettings().getReadPreference())
                                            .build()))
                    .build();
            return new ClientSessionPublisherImpl(serverSessionPool, mongoClient, mergedOptions, executor);
        });
    }

    private List<ServerDescription> getServerDescriptionListToConsiderForSessionSupport(final ClusterDescription clusterDescription) {
        if (clusterDescription.getConnectionMode() == ClusterConnectionMode.SINGLE) {
            return getAny(clusterDescription);
        } else {
            return getAnyPrimaryOrSecondary(clusterDescription);
        }
    }
}
