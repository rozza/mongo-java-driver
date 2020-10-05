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
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.client.model.IndexOptionDefaults;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.internal.async.client.AsyncClientSession;
import com.mongodb.internal.async.client.OperationExecutor;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.internal.operation.AsyncWriteOperation;
import com.mongodb.internal.operation.CommandReadOperation;
import com.mongodb.internal.operation.CreateCollectionOperation;
import com.mongodb.internal.operation.CreateViewOperation;
import com.mongodb.internal.operation.DropDatabaseOperation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.DistinctPublisher;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.ListCollectionsPublisher;
import com.mongodb.reactivestreams.client.ListDatabasesPublisher;
import com.mongodb.reactivestreams.client.ListIndexesPublisher;
import com.mongodb.reactivestreams.client.MapReducePublisher;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.stream.Collectors.toList;

public class PublisherCreator {

    public static <D, T> DistinctPublisher<T> createDistinctPublisher(@Nullable final ClientSession clientSession,
                                                                      final MongoNamespace namespace,
                                                                      final Class<D> documentClass,
                                                                      final Class<T> resultClass,
                                                                      final CodecRegistry codecRegistry,
                                                                      final ReadPreference readPreference,
                                                                      final ReadConcern readConcern,
                                                                      final OperationExecutor executor,
                                                                      final String fieldName,
                                                                      final Bson filter,
                                                                      final boolean retryReads) {
        return new DistinctPublisherImpl<>(clientSession, namespace, documentClass, resultClass, codecRegistry, readPreference, readConcern,
                executor, fieldName, filter, retryReads);
    }


    public static <T> ListDatabasesPublisher<T> createListDatabasesPublisher(@Nullable final ClientSession clientSession,
                                                                             final Class<T> resultClass,
                                                                             final CodecRegistry codecRegistry,
                                                                             final ReadPreference readPreference,
                                                                             final OperationExecutor executor,
                                                                             final boolean retryReads) {
        return new ListDatabasesPublisherImpl<>(clientSession, resultClass, codecRegistry, readPreference, executor, retryReads);
    }

    public static <T> ListCollectionsPublisher<T> createListCollectionsPublisher(@Nullable final ClientSession clientSession,
                                                                                 final String databaseName,
                                                                                 final Class<T> resultClass,
                                                                                 final CodecRegistry codecRegistry,
                                                                                 final ReadPreference readPreference,
                                                                                 final OperationExecutor executor,
                                                                                 final boolean retryReads,
                                                                                 final boolean collectionNameOnly) {
        return new ListCollectionsPublisherImpl<>(clientSession, databaseName, resultClass, codecRegistry, readPreference, executor, retryReads, collectionNameOnly);
    }

    public static <T> ListIndexesPublisher<T> createListIndexesPublisher(@Nullable final ClientSession clientSession,
                                                                         final MongoNamespace namespace,
                                                                         final Class<T> resultClass,
                                                                         final CodecRegistry codecRegistry,
                                                                         final ReadPreference readPreference,
                                                                         final OperationExecutor executor,
                                                                         final boolean retryReads) {
        return new ListIndexesPublisherImpl<>(clientSession, namespace, resultClass, codecRegistry, readPreference, executor, retryReads);
    }

    public static <D, T> FindPublisher<T> createFindPublisher(@Nullable final ClientSession clientSession,
                                                              final MongoNamespace namespace,
                                                              final Class<D> documentClass,
                                                              final Class<T> resultClass,
                                                              final CodecRegistry codecRegistry,
                                                              final ReadPreference readPreference,
                                                              final ReadConcern readConcern,
                                                              final OperationExecutor executor,
                                                              final Bson filter,
                                                              final boolean retryReads) {
        return new FindPublisherImpl<>(clientSession, namespace, documentClass, resultClass, codecRegistry, readPreference, readConcern,
                executor, filter, retryReads);
    }

    public static <D, T> AggregatePublisher<T> createAggregatePublisher(@Nullable final ClientSession clientSession,
                                                                        final String databaseName,
                                                                        final Class<D> documentClass,
                                                                        final Class<T> resultClass, final CodecRegistry codecRegistry,
                                                                        final ReadPreference readPreference,
                                                                        final ReadConcern readConcern,
                                                                        final WriteConcern writeConcern,
                                                                        final OperationExecutor executor,
                                                                        final List<? extends Bson> pipeline,
                                                                        final AggregationLevel aggregationLevel,
                                                                        final boolean retryReads) {
        return createAggregatePublisher(clientSession, new MongoNamespace(databaseName, "ignored"), documentClass, resultClass,
                codecRegistry, readPreference, readConcern, writeConcern, executor, pipeline, aggregationLevel, retryReads);
    }

    public static <D, T> AggregatePublisher<T> createAggregatePublisher(@Nullable final ClientSession clientSession,
                                                                        final MongoNamespace namespace,
                                                                        final Class<D> documentClass,
                                                                        final Class<T> resultClass,
                                                                        final CodecRegistry codecRegistry,
                                                                        final ReadPreference readPreference,
                                                                        final ReadConcern readConcern,
                                                                        final WriteConcern writeConcern,
                                                                        final OperationExecutor executor,
                                                                        final List<? extends Bson> pipeline,
                                                                        final AggregationLevel aggregationLevel,
                                                                        final boolean retryReads) {
        return new AggregatePublisherImpl<>(clientSession, namespace, documentClass, resultClass, codecRegistry, readPreference,
                readConcern, writeConcern, executor, pipeline, aggregationLevel, retryReads);
    }

    public static <T> ChangeStreamPublisher<T> createChangeStreamPublisher(@Nullable final ClientSession clientSession,
                                                                           final String databaseName,
                                                                           final Class<T> resultClass,
                                                                           final CodecRegistry codecRegistry,
                                                                           final ReadPreference readPreference,
                                                                           final ReadConcern readConcern,
                                                                           final OperationExecutor executor,
                                                                           final List<? extends Bson> pipeline,
                                                                           final ChangeStreamLevel changeStreamLevel,
                                                                           final boolean retryReads) {
        return createChangeStreamPublisher(clientSession, new MongoNamespace(databaseName, "ignored"), resultClass,
                codecRegistry, readPreference, readConcern, executor, pipeline, changeStreamLevel, retryReads);
    }

    public static <T> ChangeStreamPublisher<T> createChangeStreamPublisher(@Nullable final ClientSession clientSession,
                                                                           final MongoNamespace namespace,
                                                                           final Class<T> resultClass,
                                                                           final CodecRegistry codecRegistry,
                                                                           final ReadPreference readPreference,
                                                                           final ReadConcern readConcern,
                                                                           final OperationExecutor executor,
                                                                           final List<? extends Bson> pipeline,
                                                                           final ChangeStreamLevel changeStreamLevel,
                                                                           final boolean retryReads) {
        return new ChangeStreamPublisherImpl<>(clientSession, namespace, resultClass, codecRegistry, readPreference,
                readConcern, executor, pipeline, changeStreamLevel, retryReads);
    }

    public static <D, T> MapReducePublisher<T> createMapReducePublisher(@Nullable final ClientSession clientSession,
                                                                        final MongoNamespace namespace,
                                                                        final Class<D> documentClass,
                                                                        final Class<T> resultClass,
                                                                        final CodecRegistry codecRegistry,
                                                                        final ReadPreference readPreference,
                                                                        final ReadConcern readConcern,
                                                                        final WriteConcern writeConcern,
                                                                        final OperationExecutor executor,
                                                                        final String mapFunction,
                                                                        final String reduceFunction) {
        return new MapReducePublisherImpl<>(clientSession, namespace, documentClass, resultClass, codecRegistry, readPreference,
                readConcern, writeConcern, executor, mapFunction, reduceFunction);
    }

    public static Mono<Void> createCreateCollectionPublisher(@Nullable final ClientSession clientSession,
                                                                  final MongoNamespace namespace,
                                                                  final CodecRegistry codecRegistry,
                                                                  final ReadConcern readConcern,
                                                                  final WriteConcern writeConcern,
                                                                  final OperationExecutor executor,
                                                                  final CreateCollectionOptions options) {
        return createWriteOperationPublisher(() -> {
            CreateCollectionOperation operation = new CreateCollectionOperation(namespace.getDatabaseName(), namespace.getCollectionName(), writeConcern)
                    .capped(options.isCapped())
                    .sizeInBytes(options.getSizeInBytes())
                    .maxDocuments(options.getMaxDocuments())
                    .storageEngineOptions(toBsonDocument(options.getStorageEngineOptions(), codecRegistry))
                    .collation(options.getCollation());

            IndexOptionDefaults indexOptionDefaults = options.getIndexOptionDefaults();
            Bson storageEngine = indexOptionDefaults.getStorageEngine();
            if (storageEngine != null) {
                operation.indexOptionDefaults(new BsonDocument("storageEngine", toBsonDocument(storageEngine, codecRegistry)));
            }
            ValidationOptions validationOptions = options.getValidationOptions();
            Bson validator = validationOptions.getValidator();
            if (validator != null) {
                operation.validator(toBsonDocument(validator, codecRegistry));
            }
            if (validationOptions.getValidationLevel() != null) {
                operation.validationLevel(validationOptions.getValidationLevel());
            }
            if (validationOptions.getValidationAction() != null) {
                operation.validationAction(validationOptions.getValidationAction());
            }
            return operation;
        }, clientSession, readConcern, executor);
    }

    public static Mono<Void> createViewPublisher(@Nullable final ClientSession clientSession,
                                                                  final MongoNamespace namespace,
                                                                  final CodecRegistry codecRegistry,
                                                                  final ReadConcern readConcern,
                                                                  final WriteConcern writeConcern,
                                                                  final OperationExecutor executor,
                                                                  final String viewName,
                                                                final List<? extends Bson> pipeline,
                                                            final CreateViewOptions createViewOptions) {
        List<BsonDocument> bsonDocumentPipeline = createBsonDocumentList(pipeline, codecRegistry);
        return createWriteOperationPublisher(
                () -> new CreateViewOperation(namespace.getDatabaseName(), viewName, namespace.getCollectionName(),
                        bsonDocumentPipeline, writeConcern).collation(createViewOptions.getCollation()),
                clientSession, readConcern, executor);
    }
    
    public static Mono<Void> createDropDatabasePublisher(@Nullable final ClientSession clientSession,
                                                              final String databaseName,
                                                              final ReadConcern readConcern,
                                                              final WriteConcern writeConcern,
                                                              final OperationExecutor executor) {
        return createWriteOperationPublisher(() -> new DropDatabaseOperation(databaseName, writeConcern), clientSession, readConcern, executor);
    }

    public static <T> Mono<T> createRunCommandPublisher(@Nullable final ClientSession clientSession,
                                                         final String databaseName,
                                                         final Class<T> resultClass,
                                                         final CodecRegistry codecRegistry,
                                                         final ReadPreference readPreference,
                                                         final ReadConcern readConcern,
                                                         final OperationExecutor executor,
                                                         final Bson command) {
        if (clientSession != null && clientSession.hasActiveTransaction() && !readPreference.equals(ReadPreference.primary())) {
            throw new MongoClientException("Read preference in a transaction must be primary");
        }
        return createReadOperationPublisher(
                () -> new CommandReadOperation<>(databaseName, toBsonDocument(command, codecRegistry), codecRegistry.get(resultClass)),
                clientSession,
                readPreference, readConcern, executor);
    }

    public static Mono<BulkWriteResult> createSingleWriteRequestPublisher(
            final Supplier<AsyncWriteOperation<BulkWriteResult>> operation,
            @Nullable final ClientSession clientSession,
            final ReadConcern readConcern,
            final OperationExecutor executor,
            final WriteRequest.Type type) {
        return createWriteOperationPublisher(operation, clientSession, readConcern, executor)
                .onErrorMap(MongoBulkWriteException.class, e -> {
                    if (e.getWriteErrors().isEmpty()) {
                        return e;
                    }
                    MongoException exception;
                    if (e.getWriteConcernError() != null) {
                        WriteConcernResult writeConcernResult;
                        if (type == WriteRequest.Type.INSERT) {
                            writeConcernResult = WriteConcernResult.acknowledged(e.getWriteResult().getInsertedCount(), false, null);
                        } else if (type == WriteRequest.Type.DELETE) {
                            writeConcernResult = WriteConcernResult.acknowledged(e.getWriteResult().getDeletedCount(), false, null);
                        } else {
                            writeConcernResult = WriteConcernResult.acknowledged(e.getWriteResult().getMatchedCount() + e.getWriteResult().getUpserts().size(),
                                    e.getWriteResult().getMatchedCount() > 0,
                                    e.getWriteResult().getUpserts().isEmpty()
                                            ? null : e.getWriteResult().getUpserts().get(0).getId());
                        }
                        exception = new MongoWriteConcernException(e.getWriteConcernError(), writeConcernResult, e.getServerAddress());
                    } else {
                        exception = new MongoWriteException(new WriteError(e.getWriteErrors().get(0)), e.getServerAddress());
                    }
                    for (final String errorLabel : e.getErrorLabels()) {
                        exception.addLabel(errorLabel);
                    }
                    return exception;
                });
    }



    public static <T> Mono<T> createReadOperationPublisher(
            final Supplier<AsyncReadOperation<T>> operation,
            @Nullable final ClientSession clientSession,
            final ReadPreference readPreference,
            final ReadConcern readConcern,
            final OperationExecutor executor) {
        return Mono.create(sink -> {
            try {
                executor.execute(operation.get(), readPreference, readConcern, getSession(clientSession), (result, t) -> {
                    if (t != null) {
                        sink.error(t);
                    } else if (result == null) {
                        sink.success();
                    } else {
                        sink.success(result);
                    }
                });
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    public static <T> Mono<T> createWriteOperationPublisher(
            final Supplier<AsyncWriteOperation<T>> operation,
            @Nullable final ClientSession clientSession,
            final ReadConcern readConcern,
            final OperationExecutor executor) {
        return Mono.create(sink -> {
            try {
                executor.execute(operation.get(), readConcern, getSession(clientSession), (result, t) -> {
                    if (t != null) {
                        sink.error(t);
                    } else if (result == null) {
                        sink.success();
                    } else {
                        sink.success(result);
                    }
                });
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    @Nullable
    private static AsyncClientSession getSession(@Nullable final ClientSession clientSession) {
        return clientSession != null ? getSession(clientSession) : null;
    }


    private static List<BsonDocument> createBsonDocumentList(final List<? extends Bson> pipeline, final CodecRegistry codecRegistry) {
        if (notNull("pipeline", pipeline).contains(null)) {
            throw new IllegalArgumentException("pipeline can not contain a null value");
        }
        return pipeline.stream().map((Function<Bson, BsonDocument>) bson -> toBsonDocument(bson, codecRegistry)).collect(toList());
    }

    @Nullable
    private static BsonDocument toBsonDocument(@Nullable final Bson document, final CodecRegistry codecRegistry) {
        return document == null ? null : document.toBsonDocument(BsonDocument.class, codecRegistry);
    }

    private PublisherCreator() {
    }
}
