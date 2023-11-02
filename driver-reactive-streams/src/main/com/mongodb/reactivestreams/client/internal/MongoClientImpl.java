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
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListDatabasesPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.internal.crypt.Crypt;
import com.mongodb.reactivestreams.client.internal.crypt.Crypts;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.ClientMetadataHelper.createClientMetadataDocument;
import static java.lang.String.format;
import static org.bson.codecs.configuration.CodecRegistries.withUuidRepresentation;


/**
 * The internal MongoClient implementation.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class MongoClientImpl implements MongoClient {

    private static final Logger LOGGER = Loggers.getLogger("client");
    private final Cluster cluster;
    private final MongoClientSettings settings;
    private final OperationExecutor executor;
    private final Closeable externalResourceCloser;
    private final ServerSessionPool serverSessionPool;
    private final ClientSessionHelper clientSessionHelper;
    private final MongoOperationPublisher<Document> mongoOperationPublisher;
    private final Crypt crypt;
    private final AtomicBoolean closed;

    public MongoClientImpl(final MongoClientSettings settings, final MongoDriverInformation mongoDriverInformation, final Cluster cluster,
            @Nullable final Closeable externalResourceCloser) {
        this(settings, mongoDriverInformation, cluster, null, externalResourceCloser);
    }

    public MongoClientImpl(final MongoClientSettings settings, final MongoDriverInformation mongoDriverInformation, final Cluster cluster,
            @Nullable final OperationExecutor executor) {
        this(settings, mongoDriverInformation, cluster, executor, null);
    }

    private MongoClientImpl(final MongoClientSettings settings, final MongoDriverInformation mongoDriverInformation, final Cluster cluster,
                            @Nullable final OperationExecutor executor, @Nullable final Closeable externalResourceCloser) {
        this.settings = notNull("settings", settings);
        this.cluster = notNull("cluster", cluster);
        TimeoutSettings timeoutSettings = TimeoutSettings.create(settings);
        this.serverSessionPool = new ServerSessionPool(cluster, timeoutSettings, settings.getServerApi());
        this.clientSessionHelper = new ClientSessionHelper(this, serverSessionPool);
        AutoEncryptionSettings autoEncryptSettings = settings.getAutoEncryptionSettings();
        this.crypt = autoEncryptSettings != null ? Crypts.createCrypt(this, autoEncryptSettings) : null;
        if (executor == null) {
            this.executor = new OperationExecutorImpl(this, clientSessionHelper);
        } else {
            this.executor = executor;
        }
        this.externalResourceCloser = externalResourceCloser;
        this.mongoOperationPublisher = new MongoOperationPublisher<>(Document.class,
                                                                     withUuidRepresentation(settings.getCodecRegistry(),
                                                                     settings.getUuidRepresentation()),
                                                                     settings.getReadPreference(),
                                                                     settings.getReadConcern(), settings.getWriteConcern(),
                                                                     settings.getRetryWrites(), settings.getRetryReads(),
                                                                     settings.getUuidRepresentation(),
                                                                     settings.getAutoEncryptionSettings(),
                                                                     timeoutSettings,
                                                                     this.executor);
        this.closed = new AtomicBoolean();
        BsonDocument clientMetadataDocument = createClientMetadataDocument(settings.getApplicationName(), mongoDriverInformation);
        LOGGER.info(format("MongoClient with metadata %s created with settings %s", clientMetadataDocument.toJson(), settings));
    }

    Cluster getCluster() {
        return cluster;
    }

    public ServerSessionPool getServerSessionPool() {
        return serverSessionPool;
    }

    MongoOperationPublisher<Document> getMongoOperationPublisher() {
        return mongoOperationPublisher;
    }

    @Nullable
    Crypt getCrypt() {
        return crypt;
    }

    public MongoClientSettings getSettings() {
        return settings;
    }

    @Override
    public MongoDatabase getDatabase(final String name) {
        return new MongoDatabaseImpl(mongoOperationPublisher.withDatabase(name));
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
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
        return new ListDatabasesPublisherImpl<>(null, mongoOperationPublisher.withDocumentClass(clazz));
    }

    @Override
    public ListDatabasesPublisher<Document> listDatabases(final ClientSession clientSession) {
        return listDatabases(clientSession, Document.class);
    }

    @Override
    public <T> ListDatabasesPublisher<T> listDatabases(final ClientSession clientSession, final Class<T> clazz) {
        return new ListDatabasesPublisherImpl<>(notNull("clientSession", clientSession), mongoOperationPublisher.withDocumentClass(clazz));
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
        return new ChangeStreamPublisherImpl<>(null, mongoOperationPublisher.withDatabase("admin"),
                                               resultClass, pipeline, ChangeStreamLevel.CLIENT);
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
        return new ChangeStreamPublisherImpl<>(notNull("clientSession", clientSession), mongoOperationPublisher.withDatabase("admin"),
                                               resultClass, pipeline, ChangeStreamLevel.CLIENT);
    }

    @Override
    public Publisher<ClientSession> startSession() {
        return startSession(ClientSessionOptions.builder().build());
    }

    @Override
    public Publisher<ClientSession> startSession(final ClientSessionOptions options) {
        notNull("options", options);
        return Mono.fromCallable(() -> clientSessionHelper.createClientSession(options, executor));
    }

    @Override
    public ClusterDescription getClusterDescription() {
        return getCluster().getCurrentDescription();
    }

    TimeoutSettings getTimeoutSettings() {
        return mongoOperationPublisher.getTimeoutSettings();
    }

}
