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

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.async.client.OperationExecutor;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListDatabasesPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.internal.crypt.Crypt;
import com.mongodb.reactivestreams.client.internal.crypt.Crypts;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static org.bson.internal.CodecRegistryHelper.createRegistry;


/**
 * The internal MongoClient implementation.
 *
 * <p>This should not be considered a part of the public API.</p>
 */
public final class MongoClientImpl implements MongoClient {

    private static final Logger LOGGER = Loggers.getLogger("client");
    private final Cluster cluster;
    private final MongoClientSettings settings;
    private final OperationExecutor executor;
    private final Closeable externalResourceCloser;
    private final ServerSessionPool serverSessionPool;
    private final ClientSessionHelper clientSessionHelper;
    private final CodecRegistry codecRegistry;
    private final Crypt crypt;

    public MongoClientImpl(final MongoClientSettings settings, final Cluster cluster, @Nullable final Closeable externalResourceCloser) {
        this(settings, cluster, null, externalResourceCloser);
    }

    public MongoClientImpl(final MongoClientSettings settings, final Cluster cluster, @Nullable final OperationExecutor executor) {
        this(settings, cluster, executor, null);
    }

    private MongoClientImpl(final MongoClientSettings settings, final Cluster cluster,
                            @Nullable final OperationExecutor executor,
                            @Nullable final Closeable externalResourceCloser) {
        this.settings = notNull("settings", settings);
        this.cluster = notNull("cluster", cluster);
        this.serverSessionPool = new ServerSessionPool(cluster);
        this.clientSessionHelper = new ClientSessionHelper(this, serverSessionPool);
        AutoEncryptionSettings autoEncryptSettings = settings.getAutoEncryptionSettings();
        this.crypt = autoEncryptSettings != null ? Crypts.createCrypt(this, autoEncryptSettings) : null;
        if (executor == null) {
            this.executor = new OperationExecutorImpl(this, clientSessionHelper);
        } else {
            this.executor = executor;
        }
        this.externalResourceCloser = externalResourceCloser;
        this.codecRegistry = createRegistry(settings.getCodecRegistry(), settings.getUuidRepresentation());
    }


    public Cluster getCluster() {
        return cluster;
    }

    Closeable getExternalResourceCloser() {
        return externalResourceCloser;
    }

    public ServerSessionPool getServerSessionPool() {
        return serverSessionPool;
    }

    ClientSessionHelper getClientSessionHelper() {
        return clientSessionHelper;
    }

    CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    @Nullable
    Crypt getCrypt() {
        return crypt;
    }

    MongoClientSettings getSettings() {
        return settings;
    }

    OperationExecutor getExecutor() {
        return executor;
    }

    @Override
    public MongoDatabase getDatabase(final String name) {
        return new MongoDatabaseImpl(name, getCodecRegistry(), getSettings().getReadPreference(),
                                     getSettings().getReadConcern(), getSettings().getWriteConcern(), getExecutor(),
                                     getSettings().getRetryReads(),
                                     getSettings().getRetryWrites(), getSettings().getUuidRepresentation());
    }

    @Override
    public void close() {
        if (crypt != null) {
            crypt.close();
        }
        serverSessionPool.close();
        cluster.close();
        if (externalResourceCloser != null) {
            try {
                externalResourceCloser.close();
            } catch (IOException e) {
                LOGGER.warn("Exception closing resource", e);
            }
        }
    }

    @Override
    public Publisher<String> listDatabaseNames() {
        return Flux.from(listDatabases().nameOnly(true)).map(d -> d.getString("name"));
    }

    @Override
    public Publisher<String> listDatabaseNames(final ClientSession clientSession) {
        return Flux.from(listDatabases(clientSession).nameOnly(true)).map(d -> d.getString("name"));
    }

    @Override
    public ListDatabasesPublisher<Document> listDatabases() {
        return listDatabases(Document.class);
    }

    @Override
    public <T> ListDatabasesPublisher<T> listDatabases(final Class<T> clazz) {
        return PublisherCreator.createListDatabasesPublisher(null, clazz, getCodecRegistry(),
                                                             ReadPreference.primary(), getExecutor(), getSettings().getRetryReads());
    }

    @Override
    public ListDatabasesPublisher<Document> listDatabases(final ClientSession clientSession) {
        return listDatabases(clientSession, Document.class);
    }

    @Override
    public <T> ListDatabasesPublisher<T> listDatabases(final ClientSession clientSession, final Class<T> clazz) {
        return PublisherCreator.createListDatabasesPublisher(notNull("clientSession", clientSession), clazz, getCodecRegistry(),
                                                             ReadPreference.primary(), getExecutor(), getSettings().getRetryReads());
    }

    @Override
    public ChangeStreamPublisher<Document> watch() {
        return watch(Collections.emptyList());
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(final Class<T> resultClass) {
        return watch(Collections.emptyList(), resultClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final List<? extends Bson> pipeline) {
        return watch(pipeline, Document.class);
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(final List<? extends Bson> pipeline, final Class<T> resultClass) {
        return PublisherCreator.createChangeStreamPublisher(null, "admin", resultClass,
                                                            getSettings().getCodecRegistry(), getSettings().getReadPreference(),
                                                            getSettings().getReadConcern(), getExecutor(), pipeline,
                                                            ChangeStreamLevel.CLIENT, getSettings().getRetryReads());
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final ClientSession clientSession) {
        return watch(clientSession, Collections.emptyList(), Document.class);
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(final ClientSession clientSession, final Class<T> resultClass) {
        return watch(clientSession, Collections.emptyList(), resultClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return watch(clientSession, pipeline, Document.class);
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                              final Class<T> resultClass) {
        return PublisherCreator.createChangeStreamPublisher(notNull("clientSession", clientSession), "admin", resultClass,
                                                            getSettings().getCodecRegistry(), getSettings().getReadPreference(),
                                                            getSettings().getReadConcern(), getExecutor(), pipeline,
                                                            ChangeStreamLevel.CLIENT, getSettings().getRetryReads());
    }

    @Override
    public Publisher<ClientSession> startSession() {
        return startSession(ClientSessionOptions.builder().build());
    }

    @Override
    public Publisher<ClientSession> startSession(final ClientSessionOptions options) {
        return clientSessionHelper.createClientSessionMono(notNull("options", options), executor)
                .switchIfEmpty(Mono.create(sink -> sink.error(
                        new MongoClientException("Sessions are not supported by the MongoDB cluster to which this client is connected"))));
    }

    @Override
    public ClusterDescription getClusterDescription() {
        return getCluster().getCurrentDescription();
    }

}
