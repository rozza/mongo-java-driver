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

package com.mongodb.async.client;

import com.mongodb.Function;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.client.model.IndexOptionDefaults;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.CommandReadOperation;
import com.mongodb.operation.CreateCollectionOperation;
import com.mongodb.operation.CreateViewOperation;
import com.mongodb.operation.DropDatabaseOperation;
import com.mongodb.session.ClientSession;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.MongoNamespace.checkDatabaseNameValidity;
import static com.mongodb.assertions.Assertions.notNull;

class MongoDatabaseImpl implements MongoDatabase {
    private final String name;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final WriteConcern writeConcern;
    private final boolean retryWrites;
    private final ReadConcern readConcern;
    private final AsyncOperationExecutor executor;

    MongoDatabaseImpl(final String name, final CodecRegistry codecRegistry, final ReadPreference readPreference,
                      final WriteConcern writeConcern, final boolean retryWrites, final ReadConcern readConcern,
                      final AsyncOperationExecutor executor) {
        checkDatabaseNameValidity(name);
        this.name = notNull("name", name);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.retryWrites = retryWrites;
        this.readConcern = notNull("readConcern", readConcern);
        this.executor = notNull("executor", executor);
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

    @Override
    public MongoDatabase withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, retryWrites, readConcern, executor);
    }

    @Override
    public MongoDatabase withReadPreference(final ReadPreference readPreference) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, retryWrites, readConcern, executor);
    }

    @Override
    public MongoDatabase withWriteConcern(final WriteConcern writeConcern) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, retryWrites, readConcern, executor);
    }

    @Override
    public MongoDatabase withReadConcern(final ReadConcern readConcern) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, retryWrites, readConcern, executor);
    }

    @Override
    public MongoIterable<String> listCollectionNames() {
        return createListCollectionNamesIterable(null);
    }

    @Override
    public MongoIterable<String> listCollectionNames(final ClientSession clientSession) {
        notNull("clientSession", clientSession);
        return createListCollectionNamesIterable(clientSession);
    }

    private MongoIterable<String> createListCollectionNamesIterable(final ClientSession clientSession) {
        return createListCollectionsIterable(clientSession, BsonDocument.class)
                .map(new Function<BsonDocument, String>() {
                    @Override
                    public String apply(final BsonDocument result) {
                        return result.getString("name").getValue();
                    }
                });
    }

    @Override
    public ListCollectionsIterable<Document> listCollections() {
        return listCollections(Document.class);
    }

    @Override
    public <TResult> ListCollectionsIterable<TResult> listCollections(final Class<TResult> resultClass) {
        return createListCollectionsIterable(null, resultClass);
    }

    @Override
    public ListCollectionsIterable<Document> listCollections(final ClientSession clientSession) {
        return listCollections(clientSession, Document.class);
    }

    @Override
    public <TResult> ListCollectionsIterable<TResult> listCollections(final ClientSession clientSession, final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createListCollectionsIterable(clientSession, resultClass);
    }

    private <TResult> ListCollectionsIterable<TResult> createListCollectionsIterable(final ClientSession clientSession,
                                                                                     final Class<TResult> resultClass) {
        return new ListCollectionsIterableImpl<TResult>(clientSession, name, resultClass, codecRegistry, ReadPreference.primary(),
                executor);
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName) {
        return getCollection(collectionName, Document.class);
    }

    @Override
    public <TDocument> MongoCollection<TDocument> getCollection(final String collectionName, final Class<TDocument> documentClass) {
        return new MongoCollectionImpl<TDocument>(new MongoNamespace(name, collectionName), documentClass, codecRegistry, readPreference,
                                                  writeConcern, retryWrites, readConcern, executor);
    }

    @Override
    public void runCommand(final Bson command, final SingleResultCallback<Document> callback) {
        runCommand(command, Document.class, callback);
    }

    @Override
    public void runCommand(final Bson command, final ReadPreference readPreference, final SingleResultCallback<Document> callback) {
        runCommand(command, readPreference, Document.class, callback);
    }

    @Override
    public <TResult> void runCommand(final Bson command, final Class<TResult> resultClass,
                                     final SingleResultCallback<TResult> callback) {
        runCommand(command, ReadPreference.primary(), resultClass, callback);
    }

    @Override
    public <TResult> void runCommand(final Bson command, final ReadPreference readPreference, final Class<TResult> resultClass,
                                     final SingleResultCallback<TResult> callback) {
        executeCommand(null, command, readPreference, resultClass, callback);
    }

    @Override
    public void runCommand(final ClientSession clientSession, final Bson command, final SingleResultCallback<Document> callback) {
        runCommand(clientSession, command, Document.class, callback);
    }

    @Override
    public void runCommand(final ClientSession clientSession, final Bson command, final ReadPreference readPreference,
                           final SingleResultCallback<Document> callback) {
        runCommand(clientSession, command, readPreference, Document.class, callback);
    }

    @Override
    public <TResult> void runCommand(final ClientSession clientSession, final Bson command, final Class<TResult> resultClass,
                                     final SingleResultCallback<TResult> callback) {
        runCommand(clientSession, command, ReadPreference.primary(), resultClass, callback);
    }

    @Override
    public <TResult> void runCommand(final ClientSession clientSession, final Bson command, final ReadPreference readPreference,
                                     final Class<TResult> resultClass, final SingleResultCallback<TResult> callback) {
        notNull("clientSession", clientSession);
        executeCommand(clientSession, command, readPreference, resultClass, callback);
    }

    private <TResult> void executeCommand(final ClientSession clientSession, final Bson command, final ReadPreference readPreference,
                                          final Class<TResult> resultClass, final SingleResultCallback<TResult> callback) {
        notNull("command", command);
        notNull("readPreference", readPreference);
        executor.execute(new CommandReadOperation<TResult>(getName(), toBsonDocument(command), codecRegistry.get(resultClass)),
                readPreference, clientSession, callback);
    }

    @Override
    public void drop(final SingleResultCallback<Void> callback) {
        executeDrop(null, callback);
    }

    @Override
    public void drop(final ClientSession clientSession, final SingleResultCallback<Void> callback) {
        notNull("clientSession", clientSession);
        executeDrop(clientSession, callback);
    }

    private void executeDrop(final ClientSession clientSession, final SingleResultCallback<Void> callback) {
        executor.execute(new DropDatabaseOperation(name, writeConcern), clientSession, callback);
    }

    @Override
    public void createCollection(final String collectionName, final SingleResultCallback<Void> callback) {
        executeCreateCollection(null, collectionName, new CreateCollectionOptions(), callback);
    }

    @Override
    public void createCollection(final String collectionName, final CreateCollectionOptions createCollectionOptions,
                                 final SingleResultCallback<Void> callback) {
        executeCreateCollection(null, collectionName, createCollectionOptions, callback);
    }

    @Override
    public void createCollection(final ClientSession clientSession, final String collectionName,
                                 final SingleResultCallback<Void> callback) {
        createCollection(clientSession, collectionName, new CreateCollectionOptions(), callback);
    }

    @Override
    public void createCollection(final ClientSession clientSession, final String collectionName, final CreateCollectionOptions options,
                                 final SingleResultCallback<Void> callback) {
        notNull("clientSession", clientSession);
        executeCreateCollection(clientSession, collectionName, options, callback);
    }

    @SuppressWarnings("deprecation")
    private void executeCreateCollection(final ClientSession clientSession, final String collectionName,
                                         final CreateCollectionOptions options, final SingleResultCallback<Void> callback) {
        CreateCollectionOperation operation = new CreateCollectionOperation(name, collectionName, writeConcern)
                .capped(options.isCapped())
                .sizeInBytes(options.getSizeInBytes())
                .autoIndex(options.isAutoIndex())
                .maxDocuments(options.getMaxDocuments())
                .usePowerOf2Sizes(options.isUsePowerOf2Sizes())
                .storageEngineOptions(toBsonDocument(options.getStorageEngineOptions()))
                .collation(options.getCollation());

        IndexOptionDefaults indexOptionDefaults = options.getIndexOptionDefaults();
        if (indexOptionDefaults.getStorageEngine() != null) {
            operation.indexOptionDefaults(new BsonDocument("storageEngine", toBsonDocument(indexOptionDefaults.getStorageEngine())));
        }
        ValidationOptions validationOptions = options.getValidationOptions();
        if (validationOptions.getValidator() != null) {
            operation.validator(toBsonDocument(validationOptions.getValidator()));
        }
        if (validationOptions.getValidationLevel() != null) {
            operation.validationLevel(validationOptions.getValidationLevel());
        }
        if (validationOptions.getValidationAction() != null) {
            operation.validationAction(validationOptions.getValidationAction());
        }
        executor.execute(operation, clientSession, callback);
    }

    @Override
    public void createView(final String viewName, final String viewOn, final List<? extends Bson> pipeline,
                           final SingleResultCallback<Void> callback) {
        createView(viewName, viewOn, pipeline, new CreateViewOptions(), callback);
    }

    @Override
    public void createView(final String viewName, final String viewOn, final List<? extends Bson> pipeline,
                           final CreateViewOptions createViewOptions, final SingleResultCallback<Void> callback) {
        executeCreateView(null, viewName, viewOn, pipeline, createViewOptions, callback);
    }

    @Override
    public void createView(final ClientSession clientSession, final String viewName, final String viewOn,
                           final List<? extends Bson> pipeline, final SingleResultCallback<Void> callback) {
        createView(clientSession, viewName, viewOn, pipeline, new CreateViewOptions(), callback);
    }

    @Override
    public void createView(final ClientSession clientSession, final String viewName, final String viewOn,
                           final List<? extends Bson> pipeline, final CreateViewOptions createViewOptions,
                           final SingleResultCallback<Void> callback) {
        notNull("clientSession", clientSession);
        executeCreateView(clientSession, viewName, viewOn, pipeline, createViewOptions, callback);
    }

    private void executeCreateView(final ClientSession clientSession, final String viewName, final String viewOn,
                                   final List<? extends Bson> pipeline, final CreateViewOptions createViewOptions,
                                   final SingleResultCallback<Void> callback) {
        notNull("createViewOptions", createViewOptions);
        executor.execute(new CreateViewOperation(name, viewName, viewOn, createBsonDocumentList(pipeline), writeConcern)
                .collation(createViewOptions.getCollation()), clientSession, callback);
    }

    private List<BsonDocument> createBsonDocumentList(final List<? extends Bson> pipeline) {
        notNull("pipeline", pipeline);
        List<BsonDocument> bsonDocumentPipeline = new ArrayList<BsonDocument>(pipeline.size());
        for (Bson obj : pipeline) {
            if (obj == null) {
                throw new IllegalArgumentException("pipeline can not contain a null value");
            }
            bsonDocumentPipeline.add(obj.toBsonDocument(BsonDocument.class, codecRegistry));
        }
        return bsonDocumentPipeline;
    }

    private BsonDocument toBsonDocument(final Bson document) {
        return document == null ? null : document.toBsonDocument(BsonDocument.class, codecRegistry);
    }
}
