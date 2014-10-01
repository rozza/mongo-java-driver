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

package com.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCollectionOptions;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoDatabaseOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.RenameCollectionModel;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.operation.CommandReadOperation;
import com.mongodb.operation.CommandWriteOperation;
import com.mongodb.operation.CreateCollectionOperation;
import com.mongodb.operation.DropDatabaseOperation;
import com.mongodb.operation.GetCollectionNamesOperation;
import com.mongodb.operation.OperationExecutor;
import com.mongodb.operation.RenameCollectionOperation;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.mongodb.Document;

import java.util.List;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;

class MongoDatabaseImpl implements MongoDatabase {
    private final MongoDatabaseOptions options;
    private final String name;
    private final OperationExecutor executor;

    MongoDatabaseImpl(final String name, final MongoDatabaseOptions options, final OperationExecutor executor) {
        this.name = name;
        this.executor = executor;
        this.options = options;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName) {
        return getCollection(collectionName, MongoCollectionOptions.builder().build().withDefaults(options));
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName, final MongoCollectionOptions options) {
        return getCollection(collectionName, Document.class, options);
    }

    @Override
    public <T> MongoCollection<T> getCollection(final String collectionName, final Class<T> clazz) {
        return getCollection(collectionName, clazz, MongoCollectionOptions.builder().build().withDefaults(options));
    }

    @Override
    public <T> MongoCollection<T> getCollection(final String collectionName, final Class<T> clazz, final MongoCollectionOptions options) {
        return new MongoCollectionImpl<T>(new MongoNamespace(name, collectionName), clazz, options.withDefaults(this.options), executor);
    }

    @Override
    public void dropDatabase() {
        executor.execute(new DropDatabaseOperation(name));
    }

    @Override
    public List<String> getCollectionNames() {
        return executor.execute(new GetCollectionNamesOperation(name), primary());
    }

    @Override
    public void createCollection(final String collectionName) {
        createCollection(collectionName, new CreateCollectionOptions());
    }

    @Override
    public void createCollection(final String collectionName, final CreateCollectionOptions createCollectionOptions) {
        executor.execute(new CreateCollectionOperation(name, collectionName)
                             .capped(createCollectionOptions.isCapped())
                             .sizeInBytes(createCollectionOptions.getSizeInBytes())
                             .autoIndex(createCollectionOptions.isAutoIndex())
                             .maxDocuments(createCollectionOptions.getMaxDocuments())
                             .usePowerOf2Sizes(createCollectionOptions.isUsePowerOf2Sizes()));
    }

    @Override
    public void renameCollection(final MongoNamespace originalNamespace, final MongoNamespace newNamespace) {
        renameCollection(new RenameCollectionModel(originalNamespace, newNamespace, new RenameCollectionOptions()));
    }

    @Override
    public void renameCollection(final MongoNamespace originalNamespace, final MongoNamespace newNamespace,
                                 final RenameCollectionOptions renameCollectionOptions) {
        renameCollection(new RenameCollectionModel(originalNamespace, newNamespace, renameCollectionOptions));
    }

    private void renameCollection(final RenameCollectionModel model) {
        executor.execute(new RenameCollectionOperation(model.getOriginalNamespace(),
                                                       model.getNewNamespace()).dropTarget(model.getOptions().isDropTarget()));
    }

    @Override
    public Document executeCommand(final Document command) {
        return executor.execute(new CommandWriteOperation<Document>(getName(), wrap(command),
                                                                    options.getCodecRegistry().get(Document.class)));
    }

    @Override
    public Document executeCommand(final Document command, final ReadPreference readPreference) {
        notNull("readPreference", readPreference);
        return executor.execute(new CommandReadOperation<Document>(getName(), wrap(command),
                                                                   options.getCodecRegistry().get(Document.class)),
                                readPreference);
    }

    @Override
    public MongoDatabaseOptions getOptions() {
        return options;
    }

    private BsonDocument wrap(final Document command) {
        return new BsonDocumentWrapper<Document>(command, options.getCodecRegistry().get(Document.class));
    }
}
