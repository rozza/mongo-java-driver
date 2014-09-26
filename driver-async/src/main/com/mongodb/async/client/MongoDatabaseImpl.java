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

import com.mongodb.MongoNamespace;
import com.mongodb.async.MongoFuture;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.RenameCollectionModel;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.codecs.DocumentCodec;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.CommandWriteOperation;
import com.mongodb.operation.CreateCollectionOperation;
import com.mongodb.operation.DropDatabaseOperation;
import com.mongodb.operation.GetCollectionNamesOperation;
import com.mongodb.operation.RenameCollectionOperation;
import org.bson.BsonDocumentWrapper;
import org.mongodb.Document;

import java.util.List;

import static com.mongodb.ReadPreference.primary;

class MongoDatabaseImpl implements MongoDatabase {
    private final String name;
    private final MongoDatabaseOptions options;
    private final AsyncOperationExecutor executor;

    public MongoDatabaseImpl(final String name, final MongoDatabaseOptions options, final AsyncOperationExecutor executor) {
        this.name = name;
        this.options = options;
        this.executor = executor;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public MongoCollection<Document> getCollection(final String name) {
        return getCollection(name, MongoCollectionOptions.builder().build().withDefaults(options));
    }

    @Override
    public MongoCollection<Document> getCollection(final String name, final MongoCollectionOptions options) {
        return getCollection(name, Document.class, options);
    }


    @Override
    public <T> MongoCollection<T> getCollection(final String name, final Class<T> clazz, final MongoCollectionOptions options) {
        return new MongoCollectionImpl<T>(new MongoNamespace(this.name, name), clazz, options, executor);
    }

    @Override
    public MongoFuture<Document> executeCommand(final Document commandDocument) {
        return executor.execute(new CommandWriteOperation<Document>(name, new BsonDocumentWrapper<Document>(commandDocument,
                                                                                                            new DocumentCodec()),
                                                                    new DocumentCodec()));
    }

    @Override
    public MongoFuture<Void> dropDatabase() {
        return executor.execute(new DropDatabaseOperation(name));
    }

    @Override
    public MongoFuture<List<String>> getCollectionNames() {
        return executor.execute(new GetCollectionNamesOperation(name), primary());
    }

    @Override
    public MongoFuture<Void> createCollection(final String collectionName) {
        return createCollection(collectionName, new CreateCollectionOptions());
    }

    @Override
    public MongoFuture<Void> createCollection(final String collectionName, final CreateCollectionOptions createCollectionOptions) {
        return executor.execute(new CreateCollectionOperation(name, collectionName)
                                    .capped(createCollectionOptions.isCapped())
                                    .sizeInBytes(createCollectionOptions.getSizeInBytes())
                                    .autoIndex(createCollectionOptions.isAutoIndex())
                                    .maxDocuments(createCollectionOptions.getMaxDocuments())
                                    .usePowerOf2Sizes(createCollectionOptions.isUsePowerOf2Sizes()));
    }

    @Override
    public MongoFuture<Void>  renameCollection(final MongoNamespace originalNamespace, final MongoNamespace newNamespace) {
        return renameCollection(new RenameCollectionModel(originalNamespace, newNamespace, new RenameCollectionOptions()));
    }

    @Override
    public MongoFuture<Void> renameCollection(final MongoNamespace originalNamespace, final MongoNamespace newNamespace,
                                              final RenameCollectionOptions renameCollectionOptions) {
        return renameCollection(new RenameCollectionModel(originalNamespace, newNamespace, renameCollectionOptions));
    }

    private MongoFuture<Void> renameCollection(final RenameCollectionModel model) {
        return executor.execute(new RenameCollectionOperation(model.getOriginalNamespace(),
                                                              model.getNewNamespace()).dropTarget(model.getOptions().isDropTarget()));
    }

}
