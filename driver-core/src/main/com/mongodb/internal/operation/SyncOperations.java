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

package com.mongodb.internal.operation;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropCollectionOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.SearchIndexModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.FindOptions;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.codecs.Decoder;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class SyncOperations<TDocument> {
    private final Operations<TDocument> operations;

    public SyncOperations(final Class<TDocument> documentClass, final ReadPreference readPreference,
                          final CodecRegistry codecRegistry, final boolean retryReads, final TimeoutSettings timeoutSettings) {
        this(null, documentClass, readPreference, codecRegistry, ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED, true, retryReads, timeoutSettings);
    }

    public SyncOperations(final MongoNamespace namespace, final Class<TDocument> documentClass, final ReadPreference readPreference,
                          final CodecRegistry codecRegistry, final boolean retryReads, final TimeoutSettings timeoutSettings) {
        this(namespace, documentClass, readPreference, codecRegistry, ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED, true, retryReads, timeoutSettings);
    }

    public SyncOperations(@Nullable final MongoNamespace namespace, final Class<TDocument> documentClass, final ReadPreference readPreference,
                          final CodecRegistry codecRegistry, final ReadConcern readConcern, final WriteConcern writeConcern,
                          final boolean retryWrites, final boolean retryReads, final TimeoutSettings timeoutSettings) {
        this.operations = new Operations<>(namespace, documentClass, readPreference, codecRegistry, readConcern, writeConcern,
                retryWrites, retryReads, timeoutSettings);
    }

    public ReadOperation<Long> countDocuments(final Bson filter, final CountOptions options) {
        return operations.countDocuments(filter, options);
    }

    public ReadOperation<Long> estimatedDocumentCount(final EstimatedDocumentCountOptions options) {
        return operations.estimatedDocumentCount(options);
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> findFirst(final Bson filter, final Class<TResult> resultClass,
                                                                   final FindOptions options) {
        return operations.findFirst(filter, resultClass, options);
    }

    public <TResult> ExplainableReadOperation<BatchCursor<TResult>> find(final Bson filter, final Class<TResult> resultClass,
                                                              final FindOptions options) {
        return operations.find(filter, resultClass, options);
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> find(final MongoNamespace findNamespace, final Bson filter,
                                                              final Class<TResult> resultClass, final FindOptions options) {
        return operations.find(findNamespace, filter, resultClass, options);
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> distinct(final String fieldName, final Bson filter,
                                                                  final Class<TResult> resultClass, final long maxTimeMS,
                                                                  final Collation collation, final BsonValue comment) {
        return operations.distinct(fieldName, filter, resultClass, maxTimeMS, collation, comment);
    }

    public <TResult> ExplainableReadOperation<BatchCursor<TResult>> aggregate(final List<? extends Bson> pipeline,
            final Class<TResult> resultClass, final long maxTimeMS, final long maxAwaitTimeMS, @Nullable final Integer batchSize,
            final Collation collation, final Bson hint, final String hintString, final BsonValue comment, final Bson variables,
            final Boolean allowDiskUse, final AggregationLevel aggregationLevel) {
        return operations.aggregate(pipeline, resultClass, maxTimeMS, maxAwaitTimeMS, batchSize, collation, hint, hintString, comment,
                variables, allowDiskUse, aggregationLevel);
    }

    public AggregateToCollectionOperation aggregateToCollection(final List<? extends Bson> pipeline, final long maxTimeMS,
            final Boolean allowDiskUse, final Boolean bypassDocumentValidation,
            final Collation collation, @Nullable final Bson hint, @Nullable final String hintString, final BsonValue comment,
            final Bson variables, final AggregationLevel aggregationLevel) {
        return operations.aggregateToCollection(pipeline, maxTimeMS, allowDiskUse, bypassDocumentValidation, collation, hint, hintString,
                comment, variables, aggregationLevel);
    }

    @SuppressWarnings("deprecation")
    public WriteOperation<MapReduceStatistics> mapReduceToCollection(final String databaseName, final String collectionName,
                                                                     final String mapFunction, final String reduceFunction,
                                                                     final String finalizeFunction, final Bson filter, final int limit,
                                                                     final long maxTimeMS, final boolean jsMode, final Bson scope,
                                                                     final Bson sort, final boolean verbose,
                                                                     final com.mongodb.client.model.MapReduceAction action,
                                                                     final boolean nonAtomic, final boolean sharded,
                                                                     final Boolean bypassDocumentValidation, final Collation collation) {
        return operations.mapReduceToCollection(databaseName, collectionName, mapFunction, reduceFunction, finalizeFunction, filter, limit,
                maxTimeMS, jsMode, scope, sort, verbose, action, nonAtomic, sharded, bypassDocumentValidation, collation);
    }

    public <TResult> ReadOperation<MapReduceBatchCursor<TResult>> mapReduce(final String mapFunction, final String reduceFunction,
                                                                            final String finalizeFunction, final Class<TResult> resultClass,
                                                                            final Bson filter, final int limit,
                                                                            final long maxTimeMS, final boolean jsMode, final Bson scope,
                                                                            final Bson sort, final boolean verbose,
                                                                            final Collation collation) {
        return operations.mapReduce(mapFunction, reduceFunction, finalizeFunction, resultClass, filter, limit, maxTimeMS, jsMode, scope,
                sort, verbose, collation);
    }

    public WriteOperation<TDocument> findOneAndDelete(final Bson filter, final FindOneAndDeleteOptions options) {
        return operations.findOneAndDelete(filter, options);
    }

    public WriteOperation<TDocument> findOneAndReplace(final Bson filter, final TDocument replacement,
                                                       final FindOneAndReplaceOptions options) {
        return operations.findOneAndReplace(filter, replacement, options);
    }

    public WriteOperation<TDocument> findOneAndUpdate(final Bson filter, final Bson update, final FindOneAndUpdateOptions options) {
        return operations.findOneAndUpdate(filter, update, options);
    }

    public WriteOperation<TDocument> findOneAndUpdate(final Bson filter, final List<? extends Bson> update,
                                                      final FindOneAndUpdateOptions options) {
        return operations.findOneAndUpdate(filter, update, options);
    }

    public WriteOperation<BulkWriteResult> insertOne(final TDocument document, final InsertOneOptions options) {
        return operations.insertOne(document, options);
    }


    public WriteOperation<BulkWriteResult> replaceOne(final Bson filter, final TDocument replacement, final ReplaceOptions options) {
        return operations.replaceOne(filter, replacement, options);
    }

    public WriteOperation<BulkWriteResult> deleteOne(final Bson filter, final DeleteOptions options) {
        return operations.deleteOne(filter, options);
    }

    public WriteOperation<BulkWriteResult> deleteMany(final Bson filter, final DeleteOptions options) {
        return operations.deleteMany(filter, options);
    }

    public WriteOperation<BulkWriteResult> updateOne(final Bson filter, final Bson update, final UpdateOptions updateOptions) {
        return operations.updateOne(filter, update, updateOptions);
    }

    public WriteOperation<BulkWriteResult> updateOne(final Bson filter, final List<? extends Bson> update,
                                                     final UpdateOptions updateOptions) {
        return operations.updateOne(filter, update, updateOptions);
    }

    public WriteOperation<BulkWriteResult> updateMany(final Bson filter, final Bson update, final UpdateOptions updateOptions) {
        return operations.updateMany(filter, update, updateOptions);
    }

    public WriteOperation<BulkWriteResult> updateMany(final Bson filter, final List<? extends Bson> update,
                                                      final UpdateOptions updateOptions) {
        return operations.updateMany(filter, update, updateOptions);
    }

    public WriteOperation<BulkWriteResult> insertMany(final List<? extends TDocument> documents,
                                                      final InsertManyOptions options) {
        return operations.insertMany(documents, options);
    }

    public WriteOperation<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends TDocument>> requests,
                                                     final BulkWriteOptions options) {
        return operations.bulkWrite(requests, options);
    }

    public <TResult> ReadOperation<TResult> commandRead(final Bson command, final Class<TResult> resultClass) {
        return operations.commandRead(command, resultClass);
    }

    public WriteOperation<Void> dropDatabase() {
        return operations.dropDatabase();
    }

    public WriteOperation<Void> createCollection(final String collectionName, final CreateCollectionOptions createCollectionOptions,
            @Nullable final AutoEncryptionSettings autoEncryptionSettings) {
        return operations.createCollection(collectionName, createCollectionOptions, autoEncryptionSettings);
    }

    public WriteOperation<Void> dropCollection(final DropCollectionOptions dropCollectionOptions,
            @Nullable final AutoEncryptionSettings autoEncryptionSettings) {
        return operations.dropCollection(dropCollectionOptions, autoEncryptionSettings);
    }

    public WriteOperation<Void> renameCollection(final MongoNamespace newCollectionNamespace, final RenameCollectionOptions options) {
        return operations.renameCollection(newCollectionNamespace, options);
    }

    public WriteOperation<Void> createView(final String viewName, final String viewOn, final List<? extends Bson> pipeline,
            final CreateViewOptions createViewOptions) {
        return operations.createView(viewName, viewOn, pipeline, createViewOptions);
    }

    public WriteOperation<Void> createIndexes(final List<IndexModel> indexes, final CreateIndexOptions options) {
        return operations.createIndexes(indexes, options);
    }

    public WriteOperation<Void> createSearchIndexes(final List<SearchIndexModel> indexes) {
        return operations.createSearchIndexes(indexes);
    }

    public WriteOperation<Void> updateSearchIndex(final String indexName, final Bson definition) {
        return operations.updateSearchIndex(indexName, definition);
    }

    public WriteOperation<Void> dropSearchIndex(final String indexName) {
        return operations.dropSearchIndex(indexName);
    }


    public <TResult> ExplainableReadOperation<BatchCursor<TResult>> listSearchIndexes(final Class<TResult> resultClass,
                                                                           final long maxTimeMS,
                                                                           @Nullable final String indexName,
                                                                           @Nullable final Integer batchSize,
                                                                           @Nullable final Collation collation,
                                                                           @Nullable final BsonValue comment,
                                                                           @Nullable final Boolean allowDiskUse) {
        return operations.listSearchIndexes(resultClass, maxTimeMS, indexName, batchSize, collation,
               comment, allowDiskUse);
    }

    public WriteOperation<Void> dropIndex(final String indexName, final DropIndexOptions options) {
        return operations.dropIndex(indexName, options);
    }

    public WriteOperation<Void> dropIndex(final Bson keys, final DropIndexOptions options) {
        return operations.dropIndex(keys, options);
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> listCollections(final String databaseName, final Class<TResult> resultClass,
                                                                         final Bson filter, final boolean collectionNamesOnly,
                                                                         @Nullable final Integer batchSize, final long maxTimeMS,
                                                                         final BsonValue comment) {
        return operations.listCollections(databaseName, resultClass, filter, collectionNamesOnly, batchSize, maxTimeMS, comment);
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> listDatabases(final Class<TResult> resultClass, final Bson filter,
                                                                       final Boolean nameOnly, final long maxTimeMS,
                                                                       final Boolean authorizedDatabases, final BsonValue comment) {
        return operations.listDatabases(resultClass, filter, nameOnly, maxTimeMS, authorizedDatabases, comment);
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> listIndexes(final Class<TResult> resultClass, @Nullable final Integer batchSize,
                                                                     final long maxTimeMS, final BsonValue comment) {
        return operations.listIndexes(resultClass, batchSize, maxTimeMS, comment);
    }

    public <TResult> ReadOperation<BatchCursor<TResult>> changeStream(final FullDocument fullDocument,
            final FullDocumentBeforeChange fullDocumentBeforeChange, final List<? extends Bson> pipeline, final Decoder<TResult> decoder,
            final ChangeStreamLevel changeStreamLevel, @Nullable final Integer batchSize, final Collation collation,
            final BsonValue comment, final long maxAwaitTimeMS, final BsonDocument resumeToken, final BsonTimestamp startAtOperationTime,
            final BsonDocument startAfter, final boolean showExpandedEvents) {
        return operations.changeStream(fullDocument, fullDocumentBeforeChange, pipeline, decoder, changeStreamLevel, batchSize,
                collation, comment, maxAwaitTimeMS, resumeToken, startAtOperationTime, startAfter, showExpandedEvents);
    }
}
