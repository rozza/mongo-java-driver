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

import com.mongodb.Function;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.CommandReadOperation;
import com.mongodb.operation.CommandWriteOperation;
import com.mongodb.operation.CreateCollectionOperation;
import com.mongodb.operation.DropDatabaseOperation;
import com.mongodb.operation.ListCollectionsOperation;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static org.bson.BsonDocumentWrapper.asBsonDocument;

class MongoDatabaseImpl implements MongoDatabase {
    private final String name;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final WriteConcern writeConcern;
    private final AsyncOperationExecutor executor;

    MongoDatabaseImpl(final String name, final CodecRegistry codecRegistry, final ReadPreference readPreference,
                      final WriteConcern writeConcern, final AsyncOperationExecutor executor) {
        this.name = notNull("name", name);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.executor = notNull("executor", executor);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public MongoDatabase withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, executor);
    }

    @Override
    public MongoDatabase withReadPreference(final ReadPreference readPreference) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, executor);
    }

    @Override
    public MongoDatabase withWriteConcern(final WriteConcern writeConcern) {
        return new MongoDatabaseImpl(name, codecRegistry, readPreference, writeConcern, executor);
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName) {
        return new MongoCollectionImpl<Document>(new MongoNamespace(name, collectionName), Document.class, codecRegistry, readPreference,
                writeConcern, executor);
    }

    @Override
    public void executeCommand(final Object command, final SingleResultCallback<Document> callback) {
        executeCommand(command, Document.class, callback);
    }

    @Override
    public void executeCommand(final Object command, final ReadPreference readPreference, final SingleResultCallback<Document> callback) {
        executeCommand(command, readPreference, Document.class, callback);
    }

    @Override
    public <T> void executeCommand(final Object command, final Class<T> clazz, final SingleResultCallback<T> callback) {
        notNull("command", command);
        executor.execute(new CommandWriteOperation<T>(getName(), asBson(command), codecRegistry.get(clazz)), callback);
    }

    @Override
    public <T> void executeCommand(final Object command, final ReadPreference readPreference, final Class<T> clazz,
                                   final SingleResultCallback<T> callback) {
        notNull("command", command);
        notNull("readPreference", readPreference);
        executor.execute(new CommandReadOperation<T>(getName(), asBson(command), codecRegistry.get(clazz)), readPreference,
                callback);
    }

    @Override
    public void dropDatabase(final SingleResultCallback<Void> callback) {
        executor.execute(new DropDatabaseOperation(name), callback);
    }

    @Override
    public void getCollectionNames(final SingleResultCallback<List<String>> callback) {
        new OperationIterable<Document>(new ListCollectionsOperation<Document>(name, new DocumentCodec()), primary(), executor)
        .map(new Function<Document, String>() {
            @Override
            public String apply(final Document document) {
                return (String) document.get("name");
            }
        }).into(new ArrayList<String>(), callback);
    }

    @Override
    public void createCollection(final String collectionName, final SingleResultCallback<Void> callback) {
        createCollection(collectionName, new CreateCollectionOptions(), callback);
    }

    @Override
    public void createCollection(final String collectionName, final CreateCollectionOptions createCollectionOptions,
                                 final SingleResultCallback<Void> callback) {
        executor.execute(new CreateCollectionOperation(name, collectionName)
                         .capped(createCollectionOptions.isCapped())
                         .sizeInBytes(createCollectionOptions.getSizeInBytes())
                         .autoIndex(createCollectionOptions.isAutoIndex())
                         .maxDocuments(createCollectionOptions.getMaxDocuments())
                         .usePowerOf2Sizes(createCollectionOptions.isUsePowerOf2Sizes())
                         .storageEngineOptions(asBson(createCollectionOptions.getStorageEngineOptions())), callback);
    }

    private BsonDocument asBson(final Object document) {
        return asBsonDocument(document, codecRegistry);
    }
}
