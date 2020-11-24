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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.internal.async.client.OperationExecutor;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.operation.AsyncOperations;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.DistinctPublisher;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.ListIndexesPublisher;
import com.mongodb.reactivestreams.client.MapReducePublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static org.bson.internal.CodecRegistryHelper.createRegistry;


final class MongoCollectionImpl<T> implements MongoCollection<T> {
    private final MongoNamespace namespace;
    private final Class<T> documentClass;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;
    private final WriteConcern writeConcern;
    private final boolean retryWrites;
    private final boolean retryReads;
    private final ReadConcern readConcern;
    private final UuidRepresentation uuidRepresentation;
    private final OperationExecutor executor;
    private final AsyncOperations<T> operations;
    private final PublisherHelper<T> publisherHelper;

    MongoCollectionImpl(final MongoNamespace namespace,
                        final Class<T> documentClass,
                        final CodecRegistry codecRegistry,
                        final ReadPreference readPreference,
                        final ReadConcern readConcern,
                        final WriteConcern writeConcern,
                        final OperationExecutor executor,
                        final boolean retryReads,
                        final boolean retryWrites,
                        final UuidRepresentation uuidRepresentation) {
        this.namespace = notNull("namespace", namespace);
        this.documentClass = notNull("documentClass", documentClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.retryWrites = retryWrites;
        this.retryReads = retryReads;
        this.readConcern = notNull("readConcern", readConcern);
        this.uuidRepresentation = notNull("uuidRepresentation", uuidRepresentation);
        this.executor = notNull("executor", executor);
        this.operations = new AsyncOperations<>(namespace, documentClass, readPreference, codecRegistry, readConcern, writeConcern,
                                                retryWrites, retryReads);
        this.publisherHelper = new PublisherHelper<>(namespace, documentClass, readPreference, codecRegistry, readConcern, writeConcern,
                                                     retryWrites, retryReads, executor);

    }

    @Override
    public MongoNamespace getNamespace() {
        return namespace;
    }

    @Override
    public Class<T> getDocumentClass() {
        return documentClass;
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

    OperationExecutor getExecutor() {
        return executor;
    }

    boolean getRetryReads() {
        return retryReads;
    }

    public AsyncOperations<T> getOperations() {
        return operations;
    }

    PublisherHelper<T> getPublisherHelper() {
        return publisherHelper;
    }

    @Override
    public <D> MongoCollection<D> withDocumentClass(final Class<D> newDocumentClass) {
        return new MongoCollectionImpl<>(namespace, newDocumentClass, codecRegistry,
                                         readPreference, readConcern, writeConcern, executor, retryReads, retryWrites, uuidRepresentation);
    }

    @Override
    public MongoCollection<T> withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoCollectionImpl<>(namespace, documentClass, createRegistry(codecRegistry, uuidRepresentation),
                                         readPreference, readConcern, writeConcern, executor, retryReads, retryWrites, uuidRepresentation);
    }

    @Override
    public MongoCollection<T> withReadPreference(final ReadPreference readPreference) {
        return new MongoCollectionImpl<>(namespace, documentClass, codecRegistry,
                                         readPreference, readConcern, writeConcern, executor, retryReads, retryWrites, uuidRepresentation);
    }

    @Override
    public MongoCollection<T> withWriteConcern(final WriteConcern writeConcern) {
        return new MongoCollectionImpl<>(namespace, documentClass, codecRegistry,
                                         readPreference, readConcern, writeConcern, executor, retryReads, retryWrites, uuidRepresentation);
    }

    @Override
    public MongoCollection<T> withReadConcern(final ReadConcern readConcern) {
        return new MongoCollectionImpl<>(namespace, documentClass, codecRegistry,
                                         readPreference, readConcern, writeConcern, executor, retryReads, retryWrites, uuidRepresentation);
    }

    @Override
    public Publisher<Long> estimatedDocumentCount() {
        return estimatedDocumentCount(new EstimatedDocumentCountOptions());
    }

    @Override
    public Publisher<Long> estimatedDocumentCount(final EstimatedDocumentCountOptions options) {
        return publisherHelper.estimatedDocumentCount(options);
    }

    @Override
    public Publisher<Long> countDocuments() {
        return countDocuments(new BsonDocument());
    }

    @Override
    public Publisher<Long> countDocuments(final Bson filter) {
        return countDocuments(filter, new CountOptions());
    }

    @Override
    public Publisher<Long> countDocuments(final Bson filter, final CountOptions options) {
        return publisherHelper.countDocuments(null, filter, options);
    }

    @Override
    public Publisher<Long> countDocuments(final ClientSession clientSession) {
        return countDocuments(clientSession, new BsonDocument());
    }

    @Override
    public Publisher<Long> countDocuments(final ClientSession clientSession, final Bson filter) {
        return countDocuments(clientSession, filter, new CountOptions());
    }

    @Override
    public Publisher<Long> countDocuments(final ClientSession clientSession, final Bson filter, final CountOptions options) {
        return publisherHelper.countDocuments(notNull("clientSession", clientSession), filter, options);
    }

    @Override
    public <TResult> DistinctPublisher<TResult> distinct(final String fieldName, final Class<TResult> resultClass) {
        return distinct(fieldName, new BsonDocument(), resultClass);
    }

    @Override
    public <TResult> DistinctPublisher<TResult> distinct(final String fieldName, final Bson filter, final Class<TResult> resultClass) {
        return new DistinctPublisherImpl<>(null, namespace, documentClass, resultClass, codecRegistry, readPreference, readConcern,
                                           executor, fieldName, filter, retryReads);
    }

    @Override
    public <TResult> DistinctPublisher<TResult> distinct(final ClientSession clientSession, final String fieldName,
                                                         final Class<TResult> resultClass) {
        return distinct(clientSession, fieldName, new BsonDocument(), resultClass);
    }

    @Override
    public <TResult> DistinctPublisher<TResult> distinct(final ClientSession clientSession, final String fieldName, final Bson filter,
                                                         final Class<TResult> resultClass) {
        return new DistinctPublisherImpl<>(notNull("clientSession", clientSession), namespace, documentClass, resultClass,
                                           codecRegistry, readPreference, readConcern, executor, fieldName, filter, retryReads);
    }

    @Override
    public FindPublisher<T> find() {
        return find(new BsonDocument(), getDocumentClass());
    }

    @Override
    public <TResult> FindPublisher<TResult> find(final Class<TResult> clazz) {
        return find(new BsonDocument(), clazz);
    }

    @Override
    public FindPublisher<T> find(final Bson filter) {
        return find(filter, getDocumentClass());
    }

    @Override
    public <TResult> FindPublisher<TResult> find(final Bson filter, final Class<TResult> clazz) {
        return new FindPublisherImpl<>(null, namespace, documentClass, clazz, codecRegistry,
                                       readPreference, readConcern, executor, filter, retryReads);
    }

    @Override
    public FindPublisher<T> find(final ClientSession clientSession) {
        return find(clientSession, new BsonDocument(), getDocumentClass());
    }

    @Override
    public <TResult> FindPublisher<TResult> find(final ClientSession clientSession, final Class<TResult> clazz) {
        return find(clientSession, new BsonDocument(), clazz);
    }

    @Override
    public FindPublisher<T> find(final ClientSession clientSession, final Bson filter) {
        return find(clientSession, filter, getDocumentClass());
    }

    @Override
    public <TResult> FindPublisher<TResult> find(final ClientSession clientSession, final Bson filter, final Class<TResult> clazz) {
        return new FindPublisherImpl<>(notNull("clientSession", clientSession), namespace, documentClass, clazz, codecRegistry,
                                       readPreference, readConcern, executor, filter, retryReads);
    }

    @Override
    public AggregatePublisher<T> aggregate(final List<? extends Bson> pipeline) {
        return aggregate(pipeline, getDocumentClass());
    }

    @Override
    public <TResult> AggregatePublisher<TResult> aggregate(final List<? extends Bson> pipeline, final Class<TResult> clazz) {
        return new AggregatePublisherImpl<>(null, namespace, documentClass, clazz, codecRegistry, readPreference,
                                            readConcern, writeConcern, executor, pipeline, AggregationLevel.COLLECTION, retryReads);
    }

    @Override
    public AggregatePublisher<T> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return aggregate(clientSession, pipeline, getDocumentClass());
    }

    @Override
    public <TResult> AggregatePublisher<TResult> aggregate(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                           final Class<TResult> clazz) {
        return new AggregatePublisherImpl<>(notNull("clientSession", clientSession), namespace, documentClass, clazz, codecRegistry,
                                            readPreference, readConcern, writeConcern, executor, pipeline, AggregationLevel.COLLECTION,
                                            retryReads);
    }

    @Override
    public ChangeStreamPublisher<Document> watch() {
        return watch(Document.class);
    }

    @Override
    public <TResult> ChangeStreamPublisher<TResult> watch(final Class<TResult> resultClass) {
        return watch(Collections.emptyList(), resultClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final List<? extends Bson> pipeline) {
        return watch(pipeline, Document.class);
    }

    @Override
    public <TResult> ChangeStreamPublisher<TResult> watch(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return new ChangeStreamPublisherImpl<>(null, namespace, resultClass, codecRegistry, readPreference,
                                               readConcern, executor, pipeline, ChangeStreamLevel.COLLECTION, retryReads);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final ClientSession clientSession) {
        return watch(clientSession, Document.class);
    }

    @Override
    public <TResult> ChangeStreamPublisher<TResult> watch(final ClientSession clientSession, final Class<TResult> resultClass) {
        return watch(clientSession, Collections.emptyList(), resultClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return watch(clientSession, pipeline, Document.class);
    }

    @Override
    public <TResult> ChangeStreamPublisher<TResult> watch(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                          final Class<TResult> resultClass) {
        return new ChangeStreamPublisherImpl<>(notNull("clientSession", clientSession), namespace, resultClass, codecRegistry,
                                               readPreference, readConcern, executor, pipeline, ChangeStreamLevel.COLLECTION, retryReads);
    }

    @Override
    public MapReducePublisher<T> mapReduce(final String mapFunction, final String reduceFunction) {
        return mapReduce(mapFunction, reduceFunction, getDocumentClass());
    }

    @Override
    public <TResult> MapReducePublisher<TResult> mapReduce(final String mapFunction, final String reduceFunction,
                                                           final Class<TResult> clazz) {
        return new MapReducePublisherImpl<>(null, namespace, documentClass, clazz, codecRegistry, readPreference,
                                            readConcern, writeConcern, executor, mapFunction, reduceFunction);
    }

    @Override
    public MapReducePublisher<T> mapReduce(final ClientSession clientSession, final String mapFunction,
                                           final String reduceFunction) {
        return mapReduce(clientSession, mapFunction, reduceFunction, getDocumentClass());
    }

    @Override
    public <TResult> MapReducePublisher<TResult> mapReduce(final ClientSession clientSession, final String mapFunction,
                                                           final String reduceFunction, final Class<TResult> clazz) {
        return new MapReducePublisherImpl<>(notNull("clientSession", clientSession), namespace, documentClass, clazz, codecRegistry,
                                            readPreference, readConcern, writeConcern, executor, mapFunction, reduceFunction);
    }

    @Override
    public Publisher<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends T>> requests) {
        return bulkWrite(requests, new BulkWriteOptions());
    }

    @Override
    public Publisher<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends T>> requests,
                                                final BulkWriteOptions options) {
        return publisherHelper.bulkWrite(null, requests, options);
    }

    @Override
    public Publisher<BulkWriteResult> bulkWrite(final ClientSession clientSession,
                                                final List<? extends WriteModel<? extends T>> requests) {
        return bulkWrite(clientSession, requests, new BulkWriteOptions());
    }

    @Override
    public Publisher<BulkWriteResult> bulkWrite(final ClientSession clientSession,
                                                final List<? extends WriteModel<? extends T>> requests,
                                                final BulkWriteOptions options) {
        return publisherHelper.bulkWrite(notNull("clientSession", clientSession), requests, options);
    }

    @Override
    public Publisher<InsertOneResult> insertOne(final T document) {
        return insertOne(document, new InsertOneOptions());
    }

    @Override
    public Publisher<InsertOneResult> insertOne(final T document, final InsertOneOptions options) {
        return publisherHelper.insertOne(null, document, options);
    }

    @Override
    public Publisher<InsertOneResult> insertOne(final ClientSession clientSession, final T document) {
        return insertOne(clientSession, document, new InsertOneOptions());
    }

    @Override
    public Publisher<InsertOneResult> insertOne(final ClientSession clientSession, final T document,
                                                final InsertOneOptions options) {
        return publisherHelper.insertOne(notNull("clientSession", clientSession), document, options);
    }

    @Override
    public Publisher<InsertManyResult> insertMany(final List<? extends T> documents) {
        return insertMany(documents, new InsertManyOptions());
    }

    @Override
    public Publisher<InsertManyResult> insertMany(final List<? extends T> documents, final InsertManyOptions options) {
        return publisherHelper.insertMany(null, documents, options);
    }

    @Override
    public Publisher<InsertManyResult> insertMany(final ClientSession clientSession, final List<? extends T> documents) {
        return insertMany(clientSession, documents, new InsertManyOptions());
    }

    @Override
    public Publisher<InsertManyResult> insertMany(final ClientSession clientSession, final List<? extends T> documents,
                                                  final InsertManyOptions options) {
        return publisherHelper.insertMany(notNull("clientSession", clientSession), documents, options);
    }

    @Override
    public Publisher<DeleteResult> deleteOne(final Bson filter) {
        return deleteOne(filter, new DeleteOptions());
    }

    @Override
    public Publisher<DeleteResult> deleteOne(final Bson filter, final DeleteOptions options) {
        return publisherHelper.deleteOne(null, filter, options);
    }

    @Override
    public Publisher<DeleteResult> deleteOne(final ClientSession clientSession, final Bson filter) {
        return deleteOne(clientSession, filter, new DeleteOptions());
    }

    @Override
    public Publisher<DeleteResult> deleteOne(final ClientSession clientSession, final Bson filter, final DeleteOptions options) {
        return publisherHelper.deleteOne(notNull("clientSession", clientSession), filter, options);
    }

    @Override
    public Publisher<DeleteResult> deleteMany(final Bson filter) {
        return deleteMany(filter, new DeleteOptions());
    }

    @Override
    public Publisher<DeleteResult> deleteMany(final Bson filter, final DeleteOptions options) {
        return publisherHelper.deleteMany(null, filter, options);
    }

    @Override
    public Publisher<DeleteResult> deleteMany(final ClientSession clientSession, final Bson filter) {
        return deleteMany(clientSession, filter, new DeleteOptions());
    }

    @Override
    public Publisher<DeleteResult> deleteMany(final ClientSession clientSession, final Bson filter, final DeleteOptions options) {
        return publisherHelper.deleteMany(notNull("clientSession", clientSession), filter, options);
    }

    @Override
    public Publisher<UpdateResult> replaceOne(final Bson filter, final T replacement) {
        return replaceOne(filter, replacement, new ReplaceOptions());
    }

    @Override
    public Publisher<UpdateResult> replaceOne(final Bson filter, final T replacement, final ReplaceOptions options) {
        return publisherHelper.replaceOne(null, filter, replacement, options);
    }

    @Override
    public Publisher<UpdateResult> replaceOne(final ClientSession clientSession, final Bson filter, final T replacement) {
        return replaceOne(clientSession, filter, replacement, new ReplaceOptions());
    }

    @Override
    public Publisher<UpdateResult> replaceOne(final ClientSession clientSession, final Bson filter, final T replacement,
                                              final ReplaceOptions options) {
        return publisherHelper.replaceOne(notNull("clientSession", clientSession), filter, replacement, options);
    }

    @Override
    public Publisher<UpdateResult> updateOne(final Bson filter, final Bson update) {
        return updateOne(filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateOne(final Bson filter, final Bson update, final UpdateOptions options) {
        return publisherHelper.updateOne(null, filter, update, options);
    }

    @Override
    public Publisher<UpdateResult> updateOne(final ClientSession clientSession, final Bson filter, final Bson update) {
        return updateOne(clientSession, filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateOne(final ClientSession clientSession, final Bson filter, final Bson update,
                                             final UpdateOptions options) {
        return publisherHelper.updateOne(notNull("clientSession", clientSession), filter, update, options);
    }

    @Override
    public Publisher<UpdateResult> updateOne(final Bson filter, final List<? extends Bson> update) {
        return updateOne(filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateOne(final Bson filter, final List<? extends Bson> update, final UpdateOptions options) {
        return publisherHelper.updateOne(null, filter, update, options);
    }

    @Override
    public Publisher<UpdateResult> updateOne(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update) {
        return updateOne(clientSession, filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateOne(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                                             final UpdateOptions options) {
        return publisherHelper.updateOne(notNull("clientSession", clientSession), filter, update, options);
    }

    @Override
    public Publisher<UpdateResult> updateMany(final Bson filter, final Bson update) {
        return updateMany(filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateMany(final Bson filter, final Bson update, final UpdateOptions options) {
        return publisherHelper.updateMany(null, filter, update, options);
    }

    @Override
    public Publisher<UpdateResult> updateMany(final ClientSession clientSession, final Bson filter, final Bson update) {
        return updateMany(clientSession, filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateMany(final ClientSession clientSession, final Bson filter, final Bson update,
                                              final UpdateOptions options) {
        return publisherHelper.updateMany(notNull("clientSession", clientSession), filter, update, options);
    }

    @Override
    public Publisher<UpdateResult> updateMany(final Bson filter, final List<? extends Bson> update) {
        return updateMany(filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateMany(final Bson filter, final List<? extends Bson> update, final UpdateOptions options) {
        return publisherHelper.updateMany(null, filter, update, options);
    }

    @Override
    public Publisher<UpdateResult> updateMany(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update) {
        return updateMany(clientSession, filter, update, new UpdateOptions());
    }

    @Override
    public Publisher<UpdateResult> updateMany(final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                                              final UpdateOptions options) {
        return publisherHelper.updateMany(notNull("clientSession", clientSession), filter, update, options);
    }

    @Override
    public Publisher<T> findOneAndDelete(final Bson filter) {
        return findOneAndDelete(filter, new FindOneAndDeleteOptions());
    }

    @Override
    public Publisher<T> findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options) {
        return publisherHelper.findOneAndDelete(null, filter, options);
    }

    @Override
    public Publisher<T> findOneAndDelete(final ClientSession clientSession, final Bson filter) {
        return findOneAndDelete(clientSession, filter, new FindOneAndDeleteOptions());
    }

    @Override
    public Publisher<T> findOneAndDelete(final ClientSession clientSession, final Bson filter,
                                         final FindOneAndDeleteOptions options) {
        return publisherHelper.findOneAndDelete(notNull("clientSession", clientSession), filter, options);
    }

    @Override
    public Publisher<T> findOneAndReplace(final Bson filter, final T replacement) {
        return findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions());
    }

    @Override
    public Publisher<T> findOneAndReplace(final Bson filter, final T replacement, final FindOneAndReplaceOptions options) {
        return publisherHelper.findOneAndReplace(null, filter, replacement, options);
    }

    @Override
    public Publisher<T> findOneAndReplace(final ClientSession clientSession, final Bson filter, final T replacement) {
        return findOneAndReplace(clientSession, filter, replacement, new FindOneAndReplaceOptions());
    }

    @Override
    public Publisher<T> findOneAndReplace(final ClientSession clientSession, final Bson filter, final T replacement,
                                          final FindOneAndReplaceOptions options) {
        return publisherHelper.findOneAndReplace(notNull("clientSession", clientSession), filter, replacement, options);
    }

    @Override
    public Publisher<T> findOneAndUpdate(final Bson filter, final Bson update) {
        return findOneAndUpdate(filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    public Publisher<T> findOneAndUpdate(final Bson filter, final Bson update, final FindOneAndUpdateOptions options) {
        return publisherHelper.findOneAndUpdate(null, filter, update, options);
    }

    @Override
    public Publisher<T> findOneAndUpdate(final ClientSession clientSession, final Bson filter, final Bson update) {
        return findOneAndUpdate(clientSession, filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    public Publisher<T> findOneAndUpdate(final ClientSession clientSession, final Bson filter, final Bson update,
                                         final FindOneAndUpdateOptions options) {
        return publisherHelper.findOneAndUpdate(notNull("clientSession", clientSession), filter, update, options);
    }

    @Override
    public Publisher<T> findOneAndUpdate(final Bson filter, final List<? extends Bson> update) {
        return findOneAndUpdate(filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    public Publisher<T> findOneAndUpdate(final Bson filter, final List<? extends Bson> update,
                                         final FindOneAndUpdateOptions options) {
        return publisherHelper.findOneAndUpdate(null, filter, update, options);
    }

    @Override
    public Publisher<T> findOneAndUpdate(final ClientSession clientSession, final Bson filter,
                                         final List<? extends Bson> update) {
        return findOneAndUpdate(clientSession, filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    public Publisher<T> findOneAndUpdate(final ClientSession clientSession, final Bson filter,
                                         final List<? extends Bson> update, final FindOneAndUpdateOptions options) {
        return publisherHelper.findOneAndUpdate(notNull("clientSession", clientSession), filter, update, options);
    }

    @Override
    public Publisher<Void> drop() {
        return publisherHelper.dropCollection(null);
    }

    @Override
    public Publisher<Void> drop(final ClientSession clientSession) {
        return publisherHelper.dropCollection(notNull("clientSession", clientSession));
    }

    @Override
    public Publisher<String> createIndex(final Bson key) {
        return createIndex(key, new IndexOptions());
    }

    @Override
    public Publisher<String> createIndex(final Bson key, final IndexOptions options) {
        return publisherHelper.createIndex(null, key, options);
    }

    @Override
    public Publisher<String> createIndex(final ClientSession clientSession, final Bson key) {
        return createIndex(clientSession, key, new IndexOptions());
    }

    @Override
    public Publisher<String> createIndex(final ClientSession clientSession, final Bson key, final IndexOptions options) {
        return publisherHelper.createIndex(notNull("clientSession", clientSession), key, options);
    }

    @Override
    public Publisher<String> createIndexes(final List<IndexModel> indexes) {
        return createIndexes(indexes, new CreateIndexOptions());
    }

    @Override
    public Publisher<String> createIndexes(final List<IndexModel> indexes, final CreateIndexOptions options) {
        return publisherHelper.createIndexes(null, indexes, options);
    }

    @Override
    public Publisher<String> createIndexes(final ClientSession clientSession, final List<IndexModel> indexes) {
        return createIndexes(clientSession, indexes, new CreateIndexOptions());
    }

    @Override
    public Publisher<String> createIndexes(final ClientSession clientSession, final List<IndexModel> indexes,
            final CreateIndexOptions options) {
        return publisherHelper.createIndexes(notNull("clientSession", clientSession), indexes, options);
    }

    @Override
    public ListIndexesPublisher<Document> listIndexes() {
        return listIndexes(Document.class);
    }

    @Override
    public <TResult> ListIndexesPublisher<TResult> listIndexes(final Class<TResult> clazz) {
        return new ListIndexesPublisherImpl<>(null, getNamespace(), clazz, getCodecRegistry(), getReadPreference(), getExecutor(),
                                              getRetryReads());
    }

    @Override
    public ListIndexesPublisher<Document> listIndexes(final ClientSession clientSession) {
        return listIndexes(clientSession, Document.class);
    }

    @Override
    public <TResult> ListIndexesPublisher<TResult> listIndexes(final ClientSession clientSession, final Class<TResult> clazz) {
        return new ListIndexesPublisherImpl<>(notNull("clientSession", clientSession), namespace, clazz, codecRegistry,
                                              readPreference, executor, retryReads);
    }

    @Override
    public Publisher<Void> dropIndex(final String indexName) {
        return dropIndex(indexName, new DropIndexOptions());
    }

    @Override
    public Publisher<Void> dropIndex(final Bson keys) {
        return dropIndex(keys, new DropIndexOptions());
    }

    @Override
    public Publisher<Void> dropIndex(final String indexName, final DropIndexOptions dropIndexOptions) {
        return publisherHelper.dropIndex(null, indexName, dropIndexOptions);
    }

    @Override
    public Publisher<Void> dropIndex(final Bson keys, final DropIndexOptions dropIndexOptions) {
        return publisherHelper.dropIndex(null, keys, dropIndexOptions);
    }

    @Override
    public Publisher<Void> dropIndex(final ClientSession clientSession, final String indexName) {
        return dropIndex(clientSession, indexName, new DropIndexOptions());
    }

    @Override
    public Publisher<Void> dropIndex(final ClientSession clientSession, final Bson keys) {
        return dropIndex(clientSession, keys, new DropIndexOptions());
    }

    @Override
    public Publisher<Void> dropIndex(final ClientSession clientSession, final String indexName,
                                     final DropIndexOptions options) {
        return publisherHelper.dropIndex(notNull("clientSession", clientSession), indexName, options);
    }

    @Override
    public Publisher<Void> dropIndex(final ClientSession clientSession, final Bson keys, final DropIndexOptions options) {
        return publisherHelper.dropIndex(notNull("clientSession", clientSession), keys, options);
    }

    @Override
    public Publisher<Void> dropIndexes() {
        return dropIndexes(new DropIndexOptions());
    }

    @Override
    public Publisher<Void> dropIndexes(final DropIndexOptions options) {
        return publisherHelper.dropIndexes(null, options);
    }

    @Override
    public Publisher<Void> dropIndexes(final ClientSession clientSession) {
        return dropIndexes(clientSession, new DropIndexOptions());
    }

    @Override
    public Publisher<Void> dropIndexes(final ClientSession clientSession, final DropIndexOptions options) {
        return publisherHelper.dropIndexes(notNull("clientSession", clientSession), options);
    }

    @Override
    public Publisher<Void> renameCollection(final MongoNamespace newCollectionNamespace) {
        return renameCollection(newCollectionNamespace, new RenameCollectionOptions());
    }

    @Override
    public Publisher<Void> renameCollection(final MongoNamespace newCollectionNamespace, final RenameCollectionOptions options) {
        return publisherHelper.renameCollection(null, newCollectionNamespace, options);
    }

    @Override
    public Publisher<Void> renameCollection(final ClientSession clientSession, final MongoNamespace newCollectionNamespace) {
        return renameCollection(clientSession, newCollectionNamespace, new RenameCollectionOptions());
    }

    @Override
    public Publisher<Void> renameCollection(final ClientSession clientSession, final MongoNamespace newCollectionNamespace,
                                            final RenameCollectionOptions options) {
        return publisherHelper.renameCollection(notNull("clientSession", clientSession), newCollectionNamespace, options);
    }



}
