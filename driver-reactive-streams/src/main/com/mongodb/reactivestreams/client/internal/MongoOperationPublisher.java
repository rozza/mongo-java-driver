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

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.WriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptionDefaults;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.TimeoutMode;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.ClientSideOperationTimeouts;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.internal.operation.AsyncWriteOperation;
import com.mongodb.internal.operation.CommandReadOperation;
import com.mongodb.internal.operation.CreateCollectionOperation;
import com.mongodb.internal.operation.CreateViewOperation;
import com.mongodb.internal.operation.DropDatabaseOperation;
import com.mongodb.internal.operation.IndexHelper;
import com.mongodb.internal.operation.Operations;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.bson.internal.CodecRegistryHelper.createRegistry;

@SuppressWarnings("deprecation")
public final class MongoOperationPublisher<T> {

    private final Operations<T> operations;
    private final UuidRepresentation uuidRepresentation;
    @Nullable
    private final Long timeoutMS;
    private final OperationExecutor executor;

    MongoOperationPublisher(
            final Class<T> documentClass, final CodecRegistry codecRegistry, final ReadPreference readPreference,
            final ReadConcern readConcern, final WriteConcern writeConcern, final boolean retryWrites, final boolean retryReads,
            final UuidRepresentation uuidRepresentation, @Nullable final Long timeoutMS, final OperationExecutor executor) {
        this(new MongoNamespace("_ignored", "_ignored"), documentClass,
             codecRegistry, readPreference, readConcern, writeConcern, retryWrites, retryReads,
             uuidRepresentation, timeoutMS, executor);
    }

    MongoOperationPublisher(
            final MongoNamespace namespace, final Class<T> documentClass, final CodecRegistry codecRegistry,
            final ReadPreference readPreference, final ReadConcern readConcern, final WriteConcern writeConcern,
            final boolean retryWrites, final boolean retryReads, final UuidRepresentation uuidRepresentation,
            @Nullable final Long timeoutMS, final OperationExecutor executor) {
        this.operations = new Operations<>(namespace, notNull("documentClass", documentClass),
                                           notNull("readPreference", readPreference), notNull("codecRegistry", codecRegistry),
                                           notNull("readConcern", readConcern), notNull("writeConcern", writeConcern),
                                           retryWrites, retryReads);
        this.uuidRepresentation = notNull("uuidRepresentation", uuidRepresentation);
        this.timeoutMS = timeoutMS;
        this.executor = notNull("executor", executor);
    }

    MongoNamespace getNamespace() {
        return operations.getNamespace();
    }

    ReadPreference getReadPreference() {
        return operations.getReadPreference();
    }

    CodecRegistry getCodecRegistry() {
        return operations.getCodecRegistry();
    }

    ReadConcern getReadConcern() {
        return operations.getReadConcern();
    }

    WriteConcern getWriteConcern() {
        return operations.getWriteConcern();
    }

    public boolean getRetryWrites() {
        return operations.isRetryWrites();
    }

    public boolean getRetryReads() {
        return operations.isRetryReads();
    }

    @Nullable
    public Long getTimeout(final TimeUnit timeUnit) {
        if (timeoutMS != null) {
            return notNull("timeUnit", timeUnit).convert(timeoutMS, MILLISECONDS);
        }
        return null;
    }

    Class<T> getDocumentClass() {
        return operations.getDocumentClass();
    }

    public Operations<T> getOperations() {
        return operations;
    }

    MongoOperationPublisher<T> withDatabase(final String name) {
        return withDatabaseAndDocumentClass(name, getDocumentClass());
    }

    <D> MongoOperationPublisher<D> withDatabaseAndDocumentClass(final String name, final Class<D> documentClass) {
        return withNamespaceAndDocumentClass(new MongoNamespace(notNull("name", name), "ignored"),
                                             notNull("documentClass", documentClass));
    }

    MongoOperationPublisher<T> withNamespace(final MongoNamespace namespace) {
        return withNamespaceAndDocumentClass(namespace, getDocumentClass());
    }

    <D> MongoOperationPublisher<D> withDocumentClass(final Class<D> documentClass) {
        return withNamespaceAndDocumentClass(getNamespace(), documentClass);
    }

    @SuppressWarnings("unchecked")
    <D> MongoOperationPublisher<D> withNamespaceAndDocumentClass(final MongoNamespace namespace, final Class<D> documentClass) {
        if (getNamespace().equals(namespace) && getDocumentClass().equals(documentClass)) {
            return (MongoOperationPublisher<D>) this;
        }
        return new MongoOperationPublisher<>(notNull("namespace", namespace), notNull("documentClass", documentClass),
                                             getCodecRegistry(), getReadPreference(), getReadConcern(), getWriteConcern(),
                                             getRetryWrites(), getRetryReads(), uuidRepresentation, timeoutMS, executor);
    }

    MongoOperationPublisher<T> withCodecRegistry(final CodecRegistry codecRegistry) {
        return new MongoOperationPublisher<>(getNamespace(), getDocumentClass(),
                                             createRegistry(notNull("codecRegistry", codecRegistry), uuidRepresentation),
                                             getReadPreference(), getReadConcern(), getWriteConcern(), getRetryWrites(), getRetryReads(),
                                             uuidRepresentation, timeoutMS, executor);
    }

    MongoOperationPublisher<T> withReadPreference(final ReadPreference readPreference) {
        if (getReadPreference().equals(readPreference)) {
            return this;
        }
        return new MongoOperationPublisher<>(getNamespace(), getDocumentClass(), getCodecRegistry(),
                                             notNull("readPreference", readPreference),
                                             getReadConcern(), getWriteConcern(), getRetryWrites(), getRetryReads(),
                                             uuidRepresentation, timeoutMS, executor);
    }

    MongoOperationPublisher<T> withWriteConcern(final WriteConcern writeConcern) {
        if (getWriteConcern().equals(writeConcern)) {
            return this;
        }
        return new MongoOperationPublisher<>(getNamespace(), getDocumentClass(), getCodecRegistry(), getReadPreference(), getReadConcern(),
                                             notNull("writeConcern", writeConcern),
                                             getRetryWrites(), getRetryReads(), uuidRepresentation, timeoutMS, executor);
    }

    MongoOperationPublisher<T> withReadConcern(final ReadConcern readConcern) {
        if (getReadConcern().equals(readConcern)) {
            return this;
        }
        return new MongoOperationPublisher<>(getNamespace(), getDocumentClass(),
                                             getCodecRegistry(), getReadPreference(), notNull("readConcern", readConcern),
                                             getWriteConcern(), getRetryWrites(), getRetryReads(), uuidRepresentation, timeoutMS, executor);
    }

    MongoOperationPublisher<T> withTimeout(final long timeout, final TimeUnit timeUnit) {
        isTrueArgument("timeout >= 0", timeout >= 0);
        notNull("timeUnit", timeUnit);
        long timeoutMS = timeUnit.convert(timeout, TimeUnit.MILLISECONDS);
        return new MongoOperationPublisher<>(getNamespace(), getDocumentClass(),
                getCodecRegistry(), getReadPreference(), getReadConcern(),
                getWriteConcern(), getRetryWrites(), getRetryReads(), uuidRepresentation, timeoutMS, executor);
    }

    Publisher<Void> dropDatabase(@Nullable final ClientSession clientSession) {
        return createWriteOperationMono(() -> new DropDatabaseOperation(getClientSideOperationTimeout(),
                        getNamespace().getDatabaseName(), getWriteConcern()), clientSession);
    }

    Publisher<Void> createCollection(@Nullable final ClientSession clientSession, final MongoNamespace namespace,
                                     final CreateCollectionOptions options) {
        notNull("namespace", namespace);
        notNull("options", options);
        return createWriteOperationMono(() -> {
            CreateCollectionOperation operation =
                    new CreateCollectionOperation(getClientSideOperationTimeout(),
                            namespace.getDatabaseName(), namespace.getCollectionName(), getWriteConcern())
                            .capped(options.isCapped())
                            .sizeInBytes(options.getSizeInBytes())
                            .maxDocuments(options.getMaxDocuments())
                            .storageEngineOptions(toBsonDocument(options.getStorageEngineOptions()))
                            .collation(options.getCollation());

            IndexOptionDefaults indexOptionDefaults = options.getIndexOptionDefaults();
            Bson storageEngine = indexOptionDefaults.getStorageEngine();
            if (storageEngine != null) {
                operation.indexOptionDefaults(new BsonDocument("storageEngine", toBsonDocument(storageEngine)));
            }
            ValidationOptions validationOptions = options.getValidationOptions();
            Bson validator = validationOptions.getValidator();
            if (validator != null) {
                operation.validator(toBsonDocument(validator));
            }
            if (validationOptions.getValidationLevel() != null) {
                operation.validationLevel(validationOptions.getValidationLevel());
            }
            if (validationOptions.getValidationAction() != null) {
                operation.validationAction(validationOptions.getValidationAction());
            }
            return operation;
        }, clientSession);
    }

    Publisher<Void> createView(@Nullable final ClientSession clientSession, final String viewName, final String viewOn,
                               final List<? extends Bson> pipeline, final CreateViewOptions options) {
        notNull("viewName", viewName);
        notNull("viewOn", viewOn);
        notNull("pipeline", pipeline);
        notNull("options", options);
        List<BsonDocument> bsonDocumentPipeline = createBsonDocumentList(pipeline);
        return createWriteOperationMono(
                () -> new CreateViewOperation(getClientSideOperationTimeout(),
                        getNamespace().getDatabaseName(), viewName, viewOn, bsonDocumentPipeline, getWriteConcern())
                        .collation(options.getCollation()),
                clientSession);
    }

    public <R> Publisher<R> runCommand(@Nullable final ClientSession clientSession, final Bson command,
                                       final ReadPreference readPreference, final Class<R> clazz) {
        if (clientSession != null && clientSession.hasActiveTransaction() && !readPreference.equals(ReadPreference.primary())) {
            return Mono.error(new MongoClientException("Read preference in a transaction must be primary"));
        }
        notNull("command", command);
        notNull("readPreference", readPreference);
        notNull("clazz", clazz);
        return createReadOperationMono(
                () -> new CommandReadOperation<>(getClientSideOperationTimeout(), getNamespace().getDatabaseName(),
                        toBsonDocument(command), getCodecRegistry().get(clazz)),
                clientSession, readPreference);
    }


    Publisher<Long> estimatedDocumentCount(final EstimatedDocumentCountOptions options) {
        notNull("options", options);
        return createReadOperationMono(() -> operations.estimatedDocumentCount(
                getClientSideOperationTimeout(options.getMaxTime(MILLISECONDS))), null);
    }

    Publisher<Long> countDocuments(@Nullable final ClientSession clientSession, final Bson filter, final CountOptions options) {
        notNull("filter", filter);
        notNull("options", options);
        return createReadOperationMono(() -> operations.countDocuments(
                getClientSideOperationTimeout(options.getMaxTime(MILLISECONDS)), filter, options), clientSession);
    }

    Publisher<BulkWriteResult> bulkWrite(@Nullable final ClientSession clientSession,
                                         final List<? extends WriteModel<? extends T>> requests, final BulkWriteOptions options) {
        notNull("requests", requests);
        notNull("options", options);
        return createWriteOperationMono(() -> operations.bulkWrite(getClientSideOperationTimeout(), requests, options), clientSession);
    }

    Publisher<InsertOneResult> insertOne(@Nullable final ClientSession clientSession, final T document, final InsertOneOptions options) {
        notNull("document", document);
        notNull("options", options);
        return createSingleWriteRequestMono(() -> operations.insertOne(getClientSideOperationTimeout(), document, options),
                clientSession, WriteRequest.Type.INSERT)
                .map(INSERT_ONE_RESULT_MAPPER);
    }

    Publisher<InsertManyResult> insertMany(@Nullable final ClientSession clientSession, final List<? extends T> documents,
                                           final InsertManyOptions options) {
        notNull("documents", documents);
        notNull("options", options);
        return createWriteOperationMono(() -> operations.insertMany(getClientSideOperationTimeout(), documents, options), clientSession)
                .map(INSERT_MANY_RESULT_MAPPER);
    }

    Publisher<DeleteResult> deleteOne(@Nullable final ClientSession clientSession, final Bson filter, final DeleteOptions options) {
        notNull("filter", filter);
        notNull("options", options);
        return createSingleWriteRequestMono(() -> operations.deleteOne(getClientSideOperationTimeout(), filter, options),
                clientSession, WriteRequest.Type.DELETE)
                .map(DELETE_RESULT_MAPPER);
    }

    Publisher<DeleteResult> deleteMany(@Nullable final ClientSession clientSession, final Bson filter, final DeleteOptions options) {
        notNull("filter", filter);
        notNull("options", options);
        return createSingleWriteRequestMono(() -> operations.deleteMany(getClientSideOperationTimeout(), filter, options),
                clientSession, WriteRequest.Type.DELETE)
                .map(DELETE_RESULT_MAPPER);
    }

    Publisher<UpdateResult> replaceOne(@Nullable final ClientSession clientSession, final Bson filter, final T replacement,
                                       final ReplaceOptions options) {
        notNull("filter", filter);
        notNull("replacement", replacement);
        notNull("options", options);
        return createSingleWriteRequestMono(() -> operations.replaceOne(getClientSideOperationTimeout(), filter, replacement, options),
                clientSession, WriteRequest.Type.REPLACE)
                .map(UPDATE_RESULT_MAPPER);
    }

    Publisher<UpdateResult> updateOne(@Nullable final ClientSession clientSession, final Bson filter, final Bson update,
                                      final UpdateOptions options) {
        notNull("filter", filter);
        notNull("update", update);
        notNull("options", options);
        return createSingleWriteRequestMono(() -> operations.updateOne(getClientSideOperationTimeout(), filter, update, options),
                clientSession, WriteRequest.Type.UPDATE)
                .map(UPDATE_RESULT_MAPPER);
    }

    Publisher<UpdateResult> updateOne(@Nullable final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                                      final UpdateOptions options) {
        notNull("filter", filter);
        notNull("update", update);
        notNull("options", options);
        return createSingleWriteRequestMono(() -> operations.updateOne(getClientSideOperationTimeout(), filter, update, options),
                clientSession, WriteRequest.Type.UPDATE)
                .map(UPDATE_RESULT_MAPPER);
    }

    Publisher<UpdateResult> updateMany(@Nullable final ClientSession clientSession, final Bson filter, final Bson update,
                                       final UpdateOptions options) {
        notNull("filter", filter);
        notNull("update", update);
        notNull("options", options);
        return createSingleWriteRequestMono(() -> operations.updateMany(getClientSideOperationTimeout(), filter, update, options),
                clientSession, WriteRequest.Type.UPDATE)
                .map(UPDATE_RESULT_MAPPER);
    }

    Publisher<UpdateResult> updateMany(@Nullable final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                                       final UpdateOptions options) {
        notNull("filter", filter);
        notNull("update", update);
        notNull("options", options);
        return createSingleWriteRequestMono(() -> operations.updateMany(getClientSideOperationTimeout(), filter, update, options),
                clientSession, WriteRequest.Type.UPDATE)
                .map(UPDATE_RESULT_MAPPER);
    }

    Publisher<T> findOneAndDelete(@Nullable final ClientSession clientSession, final Bson filter, final FindOneAndDeleteOptions options) {
        notNull("filter", filter);
        notNull("options", options);
        return createWriteOperationMono(() ->
                operations.findOneAndDelete(getClientSideOperationTimeout(options.getMaxTime(MILLISECONDS)), filter, options),
                clientSession);
    }

    Publisher<T> findOneAndReplace(@Nullable final ClientSession clientSession, final Bson filter, final T replacement,
                                   final FindOneAndReplaceOptions options) {
        notNull("filter", filter);
        notNull("replacement", replacement);
        notNull("options", options);
        return createWriteOperationMono(() -> operations.findOneAndReplace(getClientSideOperationTimeout(options.getMaxTime(MILLISECONDS)),
                filter, replacement, options), clientSession);
    }

    Publisher<T> findOneAndUpdate(@Nullable final ClientSession clientSession, final Bson filter, final Bson update,
                                  final FindOneAndUpdateOptions options) {
        notNull("filter", filter);
        notNull("update", update);
        notNull("options", options);
        return createWriteOperationMono(() -> operations.findOneAndUpdate(getClientSideOperationTimeout(options.getMaxTime(MILLISECONDS)),
                filter, update, options), clientSession);
    }

    Publisher<T> findOneAndUpdate(@Nullable final ClientSession clientSession, final Bson filter, final List<? extends Bson> update,
                                  final FindOneAndUpdateOptions options) {
        notNull("filter", filter);
        notNull("update", update);
        notNull("options", options);
        return createWriteOperationMono(() -> operations.findOneAndUpdate(getClientSideOperationTimeout(options.getMaxTime(MILLISECONDS)),
                filter, update, options), clientSession);
    }


    Publisher<Void> dropCollection(@Nullable final ClientSession clientSession) {
        return createWriteOperationMono(() -> operations.dropCollection(getClientSideOperationTimeout()), clientSession);
    }

    Publisher<String> createIndex(@Nullable final ClientSession clientSession, final Bson key, final IndexOptions options) {
        return createIndexes(clientSession, singletonList(new IndexModel(notNull("key", key), options)), new CreateIndexOptions());
    }

    Publisher<String> createIndexes(@Nullable final ClientSession clientSession, final List<IndexModel> indexes,
                                    final CreateIndexOptions options) {
        notNull("indexes", indexes);
        notNull("options", options);
        return createWriteOperationMono(() -> operations.createIndexes(getClientSideOperationTimeout(options.getMaxTime(MILLISECONDS)),
                indexes, options), clientSession)
                .thenMany(Flux.fromIterable(IndexHelper.getIndexNames(indexes, getCodecRegistry())));
    }

    Publisher<Void> dropIndex(@Nullable final ClientSession clientSession, final String indexName, final DropIndexOptions options) {
        notNull("indexName", indexName);
        notNull("options", options);
        return createWriteOperationMono(() -> operations.dropIndex(getClientSideOperationTimeout(options.getMaxTime(MILLISECONDS)),
                indexName), clientSession);
    }

    Publisher<Void> dropIndex(@Nullable final ClientSession clientSession, final Bson keys, final DropIndexOptions options) {
        notNull("keys", keys);
        notNull("options", options);
        return createWriteOperationMono(() -> operations.dropIndex(getClientSideOperationTimeout(options.getMaxTime(MILLISECONDS)), keys),
                clientSession);
    }

    Publisher<Void> dropIndexes(@Nullable final ClientSession clientSession, final DropIndexOptions options) {
        return dropIndex(clientSession, "*", options);
    }

    Publisher<Void> renameCollection(@Nullable final ClientSession clientSession, final MongoNamespace newCollectionNamespace,
                                     final RenameCollectionOptions options) {
        notNull("newCollectionNamespace", newCollectionNamespace);
        notNull("options", options);
        return createWriteOperationMono(() -> operations.renameCollection(getClientSideOperationTimeout(), newCollectionNamespace, options),
                clientSession);
    }

    <R> Mono<R> createReadOperationMono(final Supplier<AsyncReadOperation<R>> operation, @Nullable final ClientSession clientSession) {
        return createReadOperationMono(operation, clientSession, getReadPreference());
    }

    <R> Mono<R> createReadOperationMono(final Supplier<AsyncReadOperation<R>> operation, @Nullable final ClientSession clientSession,
                                        final ReadPreference readPreference) {
        return executor.execute(operation, readPreference, getReadConcern(), clientSession);
    }

    <R> Mono<R> createWriteOperationMono(final Supplier<AsyncWriteOperation<R>> operation, @Nullable final ClientSession clientSession) {
        return executor.execute(operation, getReadConcern(), clientSession);
    }

    private Mono<BulkWriteResult> createSingleWriteRequestMono(final Supplier<AsyncWriteOperation<BulkWriteResult>> operation,
                                                               @Nullable final ClientSession clientSession,
                                                               final WriteRequest.Type type) {
        return createWriteOperationMono(operation, clientSession)
                .onErrorMap(MongoBulkWriteException.class, e -> {
                    MongoException exception;
                    WriteConcernError writeConcernError = e.getWriteConcernError();
                    if (e.getWriteErrors().isEmpty() && writeConcernError != null) {
                        WriteConcernResult writeConcernResult;
                        if (type == WriteRequest.Type.INSERT) {
                            writeConcernResult = WriteConcernResult.acknowledged(e.getWriteResult().getInsertedCount(), false, null);
                        } else if (type == WriteRequest.Type.DELETE) {
                            writeConcernResult = WriteConcernResult.acknowledged(e.getWriteResult().getDeletedCount(), false, null);
                        } else {
                            writeConcernResult = WriteConcernResult
                                    .acknowledged(e.getWriteResult().getMatchedCount() + e.getWriteResult().getUpserts().size(),
                                                  e.getWriteResult().getMatchedCount() > 0,
                                                  e.getWriteResult().getUpserts().isEmpty()
                                                          ? null : e.getWriteResult().getUpserts().get(0).getId());
                        }
                        exception = new MongoWriteConcernException(writeConcernError, writeConcernResult, e.getServerAddress());
                    } else if (!e.getWriteErrors().isEmpty()) {
                        exception = new MongoWriteException(new WriteError(e.getWriteErrors().get(0)), e.getServerAddress());
                    } else {
                        exception = new MongoWriteException(new WriteError(-1, "Unknown write error", new BsonDocument()),
                                                            e.getServerAddress());
                    }

                    for (final String errorLabel : e.getErrorLabels()) {
                        exception.addLabel(errorLabel);
                    }
                    return exception;
                });
    }

    private static final Function<BulkWriteResult, InsertOneResult> INSERT_ONE_RESULT_MAPPER = result -> {
        if (result.wasAcknowledged()) {
            BsonValue insertedId = result.getInserts().isEmpty() ? null : result.getInserts().get(0).getId();
            return InsertOneResult.acknowledged(insertedId);
        } else {
            return InsertOneResult.unacknowledged();
        }
    };
    private static final Function<BulkWriteResult, InsertManyResult> INSERT_MANY_RESULT_MAPPER = result -> {
        if (result.wasAcknowledged()) {
            return InsertManyResult.acknowledged(result.getInserts().stream()
                                                         .collect(HashMap::new, (m, v) -> m.put(v.getIndex(), v.getId()), HashMap::putAll));
        } else {
            return InsertManyResult.unacknowledged();
        }
    };
    private static final Function<BulkWriteResult, DeleteResult> DELETE_RESULT_MAPPER = result -> {
        if (result.wasAcknowledged()) {
            return DeleteResult.acknowledged(result.getDeletedCount());
        } else {
            return DeleteResult.unacknowledged();
        }
    };
    private static final Function<BulkWriteResult, UpdateResult> UPDATE_RESULT_MAPPER = result -> {
        if (result.wasAcknowledged()) {
            BsonValue upsertedId = result.getUpserts().isEmpty() ? null : result.getUpserts().get(0).getId();
            return UpdateResult.acknowledged(result.getMatchedCount(), (long) result.getModifiedCount(), upsertedId);
        } else {
            return UpdateResult.unacknowledged();
        }
    };

    private List<BsonDocument> createBsonDocumentList(final List<? extends Bson> pipeline) {
        if (pipeline.contains(null)) {
            throw new IllegalArgumentException("pipeline can not contain a null value");
        }
        return pipeline.stream().map(this::toBsonDocument).collect(toList());
    }

    public ClientSideOperationTimeout getClientSideOperationTimeout() {
        return ClientSideOperationTimeouts.create(timeoutMS);
    }

    public ClientSideOperationTimeout getClientSideOperationTimeout(final long maxTimeMS) {
        return ClientSideOperationTimeouts.create(timeoutMS, maxTimeMS);
    }

    public ClientSideOperationTimeout getClientSideOperationTimeout(final TimeoutMode timeoutMode, final long maxTimeMS) {
        return ClientSideOperationTimeouts.create(timeoutMS, timeoutMode, maxTimeMS);
    }

    public ClientSideOperationTimeout getClientSideOperationTimeout(final TimeoutMode timeoutMode, final long maxTimeMS,
                                                                    final long maxAwaitTimeMS) {
        return ClientSideOperationTimeouts.create(timeoutMS, timeoutMode, maxTimeMS, maxAwaitTimeMS);
    }

    public static <T> SingleResultCallback<T> sinkToCallback(final MonoSink<T> sink) {
        return (result, t) -> {
            if (t != null) {
                sink.error(t);
            } else if (result == null) {
                sink.success();
            } else {
                sink.success(result);
            }
        };
    }

    @Nullable
    private BsonDocument toBsonDocument(@Nullable final Bson document) {
        return document == null ? null : document.toBsonDocument(BsonDocument.class, getCodecRegistry());
    }
}



