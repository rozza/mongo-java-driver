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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.internal.async.client.OperationExecutor;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListCollectionsPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.Collections;
import java.util.List;

import static com.mongodb.MongoNamespace.checkDatabaseNameValidity;
import static com.mongodb.assertions.Assertions.notNull;
import static org.bson.internal.CodecRegistryHelper.createRegistry;


/**
 * The internal MongoDatabase implementation.
 *
 * <p>This should not be considered a part of the public API.</p>
 */
public final class MongoDatabaseImpl implements MongoDatabase {
    private final String name;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final WriteConcern writeConcern;
    private final boolean retryWrites;
    private final boolean retryReads;
    private final ReadConcern readConcern;
    private final UuidRepresentation uuidRepresentation;
    private final OperationExecutor executor;

    MongoDatabaseImpl(final String name,
                      final CodecRegistry codecRegistry,
                      final ReadPreference readPreference,
                      final ReadConcern readConcern,
                      final WriteConcern writeConcern,
                      final OperationExecutor executor,
                      final boolean retryReads,
                      final boolean retryWrites,
                      final UuidRepresentation uuidRepresentation) {
        checkDatabaseNameValidity(name);
        this.name = notNull("name", name);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.readConcern = notNull("readConcern", readConcern);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.executor = notNull("executor", executor);
        this.retryReads = retryReads;
        this.retryWrites = retryWrites;
        this.uuidRepresentation = notNull("uuidRepresentation", uuidRepresentation);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    public OperationExecutor getExecutor() {
        return executor;
    }

    public boolean getRetryReads() {
        return retryReads;
    }

    boolean getRetryWrites() {
        return retryWrites;
    }

    UuidRepresentation getUuidRepresentation() {
        return uuidRepresentation;
    }

    @Override
    public MongoDatabase withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoDatabaseImpl(name, createRegistry(codecRegistry, uuidRepresentation), readPreference, readConcern, writeConcern, executor, retryReads, retryWrites, uuidRepresentation);
    }

    @Override
    public MongoDatabase withReadPreference(final ReadPreference readPreference) {
        return new MongoDatabaseImpl(name, codecRegistry,readPreference, readConcern, writeConcern, executor, retryReads, retryWrites, uuidRepresentation);
    }

    @Override
    public MongoDatabase withWriteConcern(final WriteConcern writeConcern) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, readConcern, writeConcern, executor, retryReads, retryWrites, uuidRepresentation);
    }

    @Override
    public MongoDatabase withReadConcern(final ReadConcern readConcern) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, readConcern, writeConcern, executor, retryReads, retryWrites, uuidRepresentation);
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName) {
        return getCollection(collectionName, Document.class);
    }

    @Override
    public <T> MongoCollection<T> getCollection(final String collectionName, final Class<T> clazz) {
        return new MongoCollectionImpl<>(new MongoNamespace(getName(), collectionName), clazz, getCodecRegistry(),
        getReadPreference(), getReadConcern(), getWriteConcern(), getExecutor(), getRetryReads(), getRetryWrites(),
        getUuidRepresentation());
    }

    @Override
    public Publisher<Document> runCommand(final Bson command) {
        return runCommand(command, Document.class);
    }

    @Override
    public Publisher<Document> runCommand(final Bson command, final ReadPreference readPreference) {
        return runCommand(command, readPreference, Document.class);
    }

    @Override
    public <T> Publisher<T> runCommand(final Bson command, final Class<T> clazz) {
        return runCommand(command, getReadPreference(), clazz);
    }

    @Override
    public <T> Publisher<T> runCommand(final Bson command, final ReadPreference readPreference, final Class<T> clazz) {
        return PublisherCreator.createRunCommandPublisher(null,
                getName(), clazz, getCodecRegistry(), getReadPreference(), getReadConcern(), getExecutor(), notNull("command", command));
    }

    @Override
    public Publisher<Document> runCommand(final ClientSession clientSession, final Bson command) {
        return runCommand(clientSession, command, Document.class);
    }

    @Override
    public Publisher<Document> runCommand(final ClientSession clientSession, final Bson command, final ReadPreference readPreference) {
        return runCommand(clientSession, command, readPreference, Document.class);
    }

    @Override
    public <T> Publisher<T> runCommand(final ClientSession clientSession, final Bson command, final Class<T> clazz) {
        return runCommand(clientSession, command, getReadPreference(), clazz);
    }

    @Override
    public <T> Publisher<T> runCommand(final ClientSession clientSession, final Bson command,
                                       final ReadPreference readPreference, final Class<T> clazz) {
        return PublisherCreator.createRunCommandPublisher(notNull("clientSession", clientSession),
                getName(), clazz, getCodecRegistry(), getReadPreference(), getReadConcern(), getExecutor(), notNull("command", command));
    }

    @Override
    public Publisher<Void> drop() {
        return PublisherCreator.createDropDatabasePublisher(null, getName(), getReadConcern(), getWriteConcern(), getExecutor());
    }

    @Override
    public Publisher<Void> drop(final ClientSession clientSession) {
        return PublisherCreator.createDropDatabasePublisher(notNull("clientSession", clientSession), getName(), getReadConcern(), getWriteConcern(), getExecutor());
    }

    @Override
    public Publisher<String> listCollectionNames() {
        return PublisherCreator.createListCollectionsPublisher(null, getName(), String.class, getCodecRegistry(),
                getReadPreference(), getExecutor(), getRetryReads(), true);
    }

    @Override
    public Publisher<String> listCollectionNames(final ClientSession clientSession) {
        return PublisherCreator.createListCollectionsPublisher(notNull("clientSession", clientSession), getName(), String.class, getCodecRegistry(),
                getReadPreference(), getExecutor(), getRetryReads(), true);
    }

    @Override
    public ListCollectionsPublisher<Document> listCollections() {
        return listCollections(Document.class);
    }

    @Override
    public <C> ListCollectionsPublisher<C> listCollections(final Class<C> clazz) {
        return PublisherCreator.createListCollectionsPublisher(null, getName(), clazz, getCodecRegistry(),
                getReadPreference(), getExecutor(), getRetryReads(), false);
    }

    @Override
    public ListCollectionsPublisher<Document> listCollections(final ClientSession clientSession) {
        return listCollections(clientSession, Document.class);
    }

    @Override
    public <C> ListCollectionsPublisher<C> listCollections(final ClientSession clientSession, final Class<C> clazz) {
        return PublisherCreator.createListCollectionsPublisher(notNull("clientSession", clientSession), getName(), clazz, getCodecRegistry(),
                getReadPreference(), getExecutor(), getRetryReads(), false);
    }

    @Override
    public Publisher<Void> createCollection(final String collectionName) {
        return createCollection(collectionName, new CreateCollectionOptions());
    }

    @Override
    public Publisher<Void> createCollection(final String collectionName, final CreateCollectionOptions options) {
        return PublisherCreator.createCreateCollectionPublisher(null,
        new MongoNamespace(getName(), collectionName), getCodecRegistry(), getReadConcern(), getWriteConcern(), getExecutor(), options);
    }

    @Override
    public Publisher<Void> createCollection(final ClientSession clientSession, final String collectionName) {
        return createCollection(clientSession, collectionName, new CreateCollectionOptions());
    }

    @Override
    public Publisher<Void> createCollection(final ClientSession clientSession, final String collectionName,
                                               final CreateCollectionOptions options) {
        return PublisherCreator.createCreateCollectionPublisher(notNull("clientSession", clientSession),
                new MongoNamespace(getName(), collectionName), getCodecRegistry(), getReadConcern(), getWriteConcern(),
                getExecutor(), options);
    }

    @Override
    public Publisher<Void> createView(final String viewName, final String viewOn, final List<? extends Bson> pipeline) {
        return createView(viewName, viewOn, pipeline, new CreateViewOptions());
    }

    @Override
    public Publisher<Void> createView(final String viewName, final String viewOn, final List<? extends Bson> pipeline,
                                         final CreateViewOptions createViewOptions) {
        return PublisherCreator.createViewPublisher(null,
                new MongoNamespace(getName(), viewOn), getCodecRegistry(), getReadConcern(), getWriteConcern(),
                getExecutor(), viewName, pipeline, createViewOptions);
    }

    @Override
    public Publisher<Void> createView(final ClientSession clientSession, final String viewName, final String viewOn,
                                         final List<? extends Bson> pipeline) {
        return createView(clientSession, viewName, viewOn, pipeline, new CreateViewOptions());
    }

    @Override
    public Publisher<Void> createView(final ClientSession clientSession, final String viewName, final String viewOn,
                                         final List<? extends Bson> pipeline, final CreateViewOptions createViewOptions) {
        return PublisherCreator.createViewPublisher(notNull("clientSession", clientSession),
                new MongoNamespace(getName(), viewOn), getCodecRegistry(), getReadConcern(), getWriteConcern(),
                getExecutor(), viewName, pipeline, createViewOptions);
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
                getCodecRegistry(), getReadPreference(), getReadConcern(), getExecutor(), pipeline,
                ChangeStreamLevel.CLIENT, getRetryReads());
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
                getCodecRegistry(), getReadPreference(), getReadConcern(), getExecutor(), pipeline,
                ChangeStreamLevel.CLIENT, getRetryReads());
    }

    @Override
    public AggregatePublisher<Document> aggregate(final List<? extends Bson> pipeline) {
        return aggregate(pipeline, Document.class);
    }

    @Override
    public <T> AggregatePublisher<T> aggregate(final List<? extends Bson> pipeline, final Class<T> resultClass) {
        return PublisherCreator.createAggregatePublisher(null, getName(), Document.class,
                resultClass, getCodecRegistry(), getReadPreference(), getReadConcern(), getWriteConcern(), getExecutor(), pipeline,
                AggregationLevel.DATABASE, getRetryReads());
    }

    @Override
    public AggregatePublisher<Document> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return aggregate(clientSession, pipeline, Document.class);
    }

    @Override
    public <T> AggregatePublisher<T> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                           final Class<T> resultClass) {
        return PublisherCreator.createAggregatePublisher(notNull("clientSession", clientSession), getName(), Document.class,
                resultClass, getCodecRegistry(), getReadPreference(), getReadConcern(), getWriteConcern(), getExecutor(), pipeline,
                AggregationLevel.DATABASE, getRetryReads());
    }

}
