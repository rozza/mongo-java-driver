/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.operation;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.bulk.WriteRequest;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.BulkWriteBatchCombiner;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.SessionContext;
import com.mongodb.internal.connection.IndexMap;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonArray;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBinaryWriterSettings;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNumber;
import org.bson.BsonValue;
import org.bson.BsonWriterSettings;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.BsonOutput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.bulk.WriteRequest.Type.DELETE;
import static com.mongodb.bulk.WriteRequest.Type.INSERT;
import static com.mongodb.bulk.WriteRequest.Type.REPLACE;
import static com.mongodb.bulk.WriteRequest.Type.UPDATE;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.OperationHelper.CallableWithConnection;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.serverIsAtLeastVersionThreeDotSix;
import static com.mongodb.operation.OperationHelper.validateWriteRequests;
import static com.mongodb.operation.OperationHelper.withConnection;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * An operation to execute a series of write operations in bulk.
 *
 * @since 3.0
 */
public class MixedBulkWriteOperation implements AsyncWriteOperation<BulkWriteResult>, WriteOperation<BulkWriteResult> {
    private static final CodecRegistry REGISTRY = fromProviders(new BsonValueCodecProvider());
    private static final int HEADROOM = 16 * 1024;

    private final MongoNamespace namespace;
    private final List<? extends WriteRequest> writeRequests;
    private final boolean ordered;
    private final WriteConcern writeConcern;
    private Boolean bypassDocumentValidation;

    /**
     * Construct a new instance.
     *
     * @param namespace     the database and collection namespace for the operation.
     * @param writeRequests the list of writeRequests to execute.
     * @param ordered       whether the writeRequests must be executed in order.
     * @param writeConcern  the write concern for the operation.
     */
    public MixedBulkWriteOperation(final MongoNamespace namespace, final List<? extends WriteRequest> writeRequests,
                                   final boolean ordered, final WriteConcern writeConcern) {
        this.ordered = ordered;
        this.namespace = notNull("namespace", namespace);
        this.writeRequests = notNull("writes", writeRequests);
        this.writeConcern = notNull("writeConcern", writeConcern);
        isTrueArgument("writes is not an empty list", !writeRequests.isEmpty());
    }

    /**
     * Gets the namespace of the collection to write to.
     *
     * @return the namespace
     */
    public MongoNamespace getNamespace() {
        return namespace;
    }

    /**
     * Gets the write concern to apply
     *
     * @return the write concern
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Gets whether the writes are ordered.  If true, no more writes will be executed after the first failure.
     *
     * @return whether the writes are ordered
     */
    public boolean isOrdered() {
        return ordered;
    }

    /**
     * Gets the list of write requests to execute.
     *
     * @return the list of write requests
     */
    public List<? extends WriteRequest> getWriteRequests() {
        return writeRequests;
    }

    /**
     * Gets the the bypass document level validation flag
     *
     * @return the bypass document level validation flag
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    /**
     * Sets the bypass document level validation flag.
     *
     * @param bypassDocumentValidation If true, allows the write to opt-out of document level validation.
     * @return this
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public MixedBulkWriteOperation bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    /**
     * Executes a bulk write operation.
     *
     * @param binding the WriteBinding        for the operation
     * @return the bulk write result.
     * @throws MongoBulkWriteException if a failure to complete the bulk write is detected based on the server response
     */
    @Override
    public BulkWriteResult execute(final WriteBinding binding) {
        return withConnection(binding, new CallableWithConnection<BulkWriteResult>() {
            @Override
            public BulkWriteResult call(final Connection connection) {
                validateWriteRequests(connection, bypassDocumentValidation, writeRequests, writeConcern);
                if (getWriteConcern().isAcknowledged() || serverIsAtLeastVersionThreeDotSix(connection.getDescription())) {
                    return executeBatches(connection, binding.getSessionContext(), getWriteRequestsWithIndices());
                } else {
                    return executeLegacyBatches(connection);
                }
            }
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<BulkWriteResult> callback) {
        withConnection(binding, new OperationHelper.AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                final SingleResultCallback<BulkWriteResult> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    validateWriteRequests(connection, bypassDocumentValidation, getWriteRequests(), writeConcern,
                            new OperationHelper.AsyncCallableWithConnection() {
                                @Override
                                public void call(final AsyncConnection connection, final Throwable t1) {
                                    if (t1 != null) {
                                        releasingCallback(errHandlingCallback, connection).onResult(null, t1);
                                    } else {
                                        SingleResultCallback<BulkWriteResult> wrappedCallback =
                                                releasingCallback(errHandlingCallback, connection);
                                        if (writeConcern.isAcknowledged()
                                                || serverIsAtLeastVersionThreeDotSix(connection.getDescription())) {
                                            executeBatchesAsync(connection,
                                                    binding.getSessionContext(),
                                                    new BulkWriteBatchCombiner(connection.getDescription().getServerAddress(), isOrdered(),
                                                            getWriteConcern()),
                                                    new BatchMetadata(getWriteRequestsWithIndices(), 0), wrappedCallback);
                                        } else {
                                            executeLegacyBatchesAsync(connection,  getWriteRequests(), 1, wrappedCallback);
                                        }
                                    }
                                }
                            });
                }
            }
        });
    }

    private BulkWriteResult executeBatches(final Connection connection, final SessionContext sessionContext,
                                           final List<WriteRequestWithIndex> writeRequestsWithIndices) {
        BulkWriteBatchCombiner bulkWriteBatchCombiner = new BulkWriteBatchCombiner(connection.getDescription().getServerAddress(),
                ordered, writeConcern);

        BatchMetadata batchMetadata = new BatchMetadata(writeRequestsWithIndices, 0);
        do {
            RawBsonDocument command = createCommand(connection.getDescription(), batchMetadata);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Sending batch %d", batchMetadata.batchNumber));
            }

            BsonDocument result = connection.command(getNamespace().getDatabaseName(), command, primary(),
                    new NoOpFieldNameValidator(), getCodec(command), sessionContext);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Received response for batch %d", batchMetadata.batchNumber));
            }

            processCommandResult(connection.getDescription().getServerAddress(), bulkWriteBatchCombiner, batchMetadata, result);
            batchMetadata = batchMetadata.getNextBatchMetadata();
        } while (!batchMetadata.isEmpty() && !bulkWriteBatchCombiner.shouldStopSendingMoreBatches());

        return bulkWriteBatchCombiner.getResult();
    }

    List<WriteRequestWithIndex> getWriteRequestsWithIndices() {
        ArrayList<WriteRequestWithIndex> writeRequestsWithIndex = new ArrayList<WriteRequestWithIndex>();
        for (int i = 0; i < writeRequests.size(); i++) {
            writeRequestsWithIndex.add(new WriteRequestWithIndex(writeRequests.get(i), i));
        }
        return writeRequestsWithIndex;
    }

    static class WriteRequestWithIndex {
        private final int index;
        private final WriteRequest writeRequest;

        WriteRequestWithIndex(final WriteRequest writeRequest, final int index) {
            this.writeRequest = writeRequest;
            this.index = index;
        }

        WriteRequest.Type getType() {
            return writeRequest.getType();
        }

        @Override
        public String toString() {
            return "WriteRequestWithIndex{"
                    + "index=" + index
                    + ", writeRequestType=" + writeRequest.getType()
                    + "}";
        }
    }

    class BatchMetadata {
        private final IndexMap indexMap = IndexMap.create();
        private final List<WriteRequestWithIndex> writeRequestsWithIndices;
        private final int batchNumber;
        private List<WriteRequestWithIndex> unprocessed = emptyList();

        BatchMetadata(final List<WriteRequestWithIndex> writeRequestsWithIndices, final int batchNumber) {
            this.writeRequestsWithIndices = writeRequestsWithIndices;
            this.batchNumber = batchNumber;
        }

        WriteRequest.Type getBatchType() {
            return writeRequestsWithIndices.get(0).writeRequest.getType();
        }

        void setUnprocessed(final List<WriteRequestWithIndex> unprocessed) {
            this.unprocessed = unprocessed;
        }

        BatchMetadata getNextBatchMetadata() {
            return new BatchMetadata(unprocessed, batchNumber + 1);
        }

        void addIndex(final int batchIndex, final int writeRequestIndex) {
            indexMap.add(batchIndex, writeRequestIndex);
        }

        int getSize() {
            return writeRequests.size() - unprocessed.size();
        }

        boolean isEmpty() {
            return writeRequestsWithIndices.isEmpty();
        }
    }

    private BulkWriteResult executeLegacyBatches(final Connection connection) {
        int batchNum = 0;
        for (WriteRequest writeRequest : getWriteRequests()) {
            batchNum++;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Asynchronously sending batch %d", batchNum));
            }
            if (writeRequest.getType() == INSERT) {
                connection.insert(getNamespace(), isOrdered(), getWriteConcern(), singletonList((InsertRequest) writeRequest));
            } else if (writeRequest.getType() == UPDATE || writeRequest.getType() == REPLACE) {
                connection.update(getNamespace(), isOrdered(), getWriteConcern(), singletonList((UpdateRequest) writeRequest));
            } else if (writeRequest.getType() == DELETE) {
                connection.delete(getNamespace(), isOrdered(), getWriteConcern(), singletonList((DeleteRequest) writeRequest));
            } else {
                throw getUnsupportedType(writeRequest.getType());
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Received response for batch %d", batchNum));
            }
        }
        return BulkWriteResult.unacknowledged();
    }

    private void executeBatchesAsync(final AsyncConnection connection, final SessionContext sessionContext,
                                     final BulkWriteBatchCombiner bulkWriteBatchCombiner,
                                     final BatchMetadata batchMetadata,
                                     final SingleResultCallback<BulkWriteResult> callback) {
        try {

            if (!batchMetadata.isEmpty() && !bulkWriteBatchCombiner.shouldStopSendingMoreBatches()) {
                RawBsonDocument command = createCommand(connection.getDescription(), batchMetadata);

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(format("Asynchronously sending batch %d", batchMetadata.batchNumber));
                }
                connection.commandAsync(getNamespace().getDatabaseName(), command, primary(),
                        new NoOpFieldNameValidator(), getCodec(command), sessionContext, new SingleResultCallback<BsonDocument>() {
                            @Override
                            public void onResult(final BsonDocument result, final Throwable t) {
                                if (t != null) {
                                    callback.onResult(null, t);
                                } else {
                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug(format("Asynchronously received response for batch %d", batchMetadata.batchNumber));
                                    }

                                    processCommandResult(connection.getDescription().getServerAddress(), bulkWriteBatchCombiner,
                                            batchMetadata, result);

                                    executeBatchesAsync(connection, sessionContext, bulkWriteBatchCombiner, batchMetadata
                                            .getNextBatchMetadata(), callback);
                                }
                            }
                        });
            } else {
                if (bulkWriteBatchCombiner.hasErrors()) {
                    callback.onResult(null, bulkWriteBatchCombiner.getError());
                } else {
                    callback.onResult(bulkWriteBatchCombiner.getResult(), null);
                }
            }
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    private void executeLegacyBatchesAsync(final AsyncConnection connection, final List<? extends WriteRequest> writeRequests,
                                           final int batchNum, final SingleResultCallback<BulkWriteResult> callback) {
        try {
            if (!writeRequests.isEmpty()) {
                WriteRequest writeRequest = writeRequests.get(0);
                final List<? extends WriteRequest> remaining = writeRequests.subList(1, writeRequests.size());

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(format("Asynchronously sending batch %d", batchNum));
                }

                SingleResultCallback<WriteConcernResult> writeCallback = new SingleResultCallback<WriteConcernResult>() {
                    @Override
                    public void onResult(final WriteConcernResult result, final Throwable t) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(format("Received response for batch %d", batchNum));
                        }
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            executeLegacyBatchesAsync(connection, remaining, batchNum + 1, callback);
                        }
                    }
                };

                if (writeRequest.getType() == INSERT) {
                    connection.insertAsync(getNamespace(), isOrdered(), getWriteConcern(), singletonList((InsertRequest) writeRequest),
                            writeCallback);
                } else if (writeRequest.getType() == UPDATE || writeRequest.getType() == REPLACE) {
                    connection.updateAsync(getNamespace(), isOrdered(), getWriteConcern(), singletonList((UpdateRequest) writeRequest),
                            writeCallback);
                } else if (writeRequest.getType() == DELETE) {
                    connection.deleteAsync(getNamespace(), isOrdered(), getWriteConcern(), singletonList((DeleteRequest) writeRequest),
                            writeCallback);
                } else {
                    throw new UnsupportedOperationException(format("Unsupported write of type %s", writeRequest.getType()));
                }
            } else {
                callback.onResult(BulkWriteResult.unacknowledged(), null);
            }
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    // Command generation
    private RawBsonDocument createCommand(final ConnectionDescription description, final BatchMetadata batchMetaData) {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();


        BulkWriteFieldValidator fieldValidator = new BulkWriteFieldValidator(batchMetaData.getBatchType());
        BsonBinaryWriter writer = new BsonBinaryWriter(new BsonWriterSettings(),
                new BsonBinaryWriterSettings(description.getMaxDocumentSize() + HEADROOM), bsonOutput,
                fieldValidator);

        writeBatch(description, bsonOutput, writer, batchMetaData, fieldValidator);

        return new RawBsonDocument(bsonOutput.getInternalBuffer());
    }

    private void writeBatch(final ConnectionDescription description, final BsonOutput bsonOutput, final BsonBinaryWriter writer,
                            final BatchMetadata batchMetadata, final BulkWriteFieldValidator fieldValidator) {
        writer.writeStartDocument();

        writer.writeString(getCommandName(batchMetadata.getBatchType()), getNamespace().getCollectionName());
        writer.writeBoolean("ordered", isOrdered());
        if (!getWriteConcern().isServerDefault()) {
            writer.writeName("writeConcern");
            BsonDocument document = getWriteConcern().asDocument();
            getCodec(document).encode(writer, document, EncoderContext.builder().build());
        }
        if (getBypassDocumentValidation() != null) {
            writer.writeBoolean("bypassDocumentValidation", getBypassDocumentValidation());
        }

        List<WriteRequestWithIndex> unprocessedWrites = Collections.emptyList();
        List<WriteRequestWithIndex> batchItems = new ArrayList<WriteRequestWithIndex>();

        writer.writeStartArray(getArrayName(batchMetadata.getBatchType()));
        List<WriteRequestWithIndex> writeRequestsWithIndices = batchMetadata.writeRequestsWithIndices;
        int lastWriteIndex = 0;
        for (int i = 0; i < writeRequestsWithIndices.size(); i++) {
            WriteRequestWithIndex writeRequestWithIndex = writeRequestsWithIndices.get(i);
            if (writeRequestWithIndex.getType() != batchMetadata.getBatchType()) {
                break;
            }
            fieldValidator.setWriteRequest(writeRequestWithIndex.writeRequest);

            writer.mark();

            if (batchMetadata.getBatchType() == INSERT) {
                writer.pushMaxDocumentSize(description.getMaxDocumentSize());
                BsonDocument document = ((InsertRequest) writeRequestWithIndex.writeRequest).getDocument();
                getCodec(document).encode(writer, document, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
                writer.popMaxDocumentSize();
            } else if (batchMetadata.getBatchType() == UPDATE || batchMetadata.getBatchType() == REPLACE) {
                UpdateRequest update = (UpdateRequest) writeRequestWithIndex.writeRequest;
                writer.writeStartDocument();
                writer.pushMaxDocumentSize(description.getMaxDocumentSize());

                writer.writeName("q");
                getCodec(update.getFilter()).encode(writer, update.getFilter(), EncoderContext.builder().build());
                writer.writeName("u");

                int bufferPosition = bsonOutput.getPosition();
                getCodec(update.getUpdate()).encode(writer, update.getUpdate(), EncoderContext.builder().build());
                if (update.getType() == WriteRequest.Type.UPDATE && bsonOutput.getPosition() == bufferPosition + 8) {
                    throw new IllegalArgumentException("Invalid BSON document for an update");
                }

                if (update.isMulti()) {
                    writer.writeBoolean("multi", update.isMulti());
                }
                if (update.isUpsert()) {
                    writer.writeBoolean("upsert", update.isUpsert());
                }
                if (update.getCollation() != null) {
                    writer.writeName("collation");
                    BsonDocument collation = update.getCollation().asDocument();
                    getCodec(collation).encode(writer, collation, EncoderContext.builder().build());
                }
                writer.popMaxDocumentSize();
                writer.writeEndDocument();
            } else if (batchMetadata.getBatchType() == DELETE) {
                DeleteRequest deleteRequest = (DeleteRequest) writeRequestWithIndex.writeRequest;
                writer.writeStartDocument();
                writer.pushMaxDocumentSize(description.getMaxDocumentSize());
                writer.writeName("q");
                getCodec(deleteRequest.getFilter()).encode(writer, deleteRequest.getFilter(), EncoderContext.builder().build());
                writer.writeInt32("limit", deleteRequest.isMulti() ? 0 : 1);
                if (deleteRequest.getCollation() != null) {
                    writer.writeName("collation");
                    BsonDocument collation = deleteRequest.getCollation().asDocument();
                    getCodec(collation).encode(writer, collation, EncoderContext.builder().build());
                }
                writer.popMaxDocumentSize();
                writer.writeEndDocument();
            } else {
                throw new UnsupportedOperationException(format("Unsupported write of type %s", batchMetadata.getBatchType()));
            }

            if (exceedsLimits(bsonOutput.getPosition(), i + 1, description.getMaxDocumentSize(), description.getMaxBatchCount())) {
                writer.reset();
                break;
            }
            lastWriteIndex = i;
            batchItems.add(writeRequestWithIndex);
            batchMetadata.addIndex(batchItems.size(), writeRequestWithIndex.index);
        }
        writer.writeEndArray();
        writer.writeEndDocument();

        if (ordered) {
            unprocessedWrites = writeRequestsWithIndices.subList(lastWriteIndex + 1, writeRequestsWithIndices.size());
        } else {
            unprocessedWrites = new ArrayList<WriteRequestWithIndex>(writeRequestsWithIndices);
            unprocessedWrites.removeAll(batchItems);
        }
        batchMetadata.setUnprocessed(unprocessedWrites);
    }

    private String getCommandName(final WriteRequest.Type batchType) {
        if (batchType == INSERT) {
            return "insert";
        } else if (batchType == UPDATE || batchType == REPLACE) {
            return "update";
        } else if (batchType == DELETE) {
            return "delete";
        } else {
            throw getUnsupportedType(batchType);
        }
    }

    private String getArrayName(final WriteRequest.Type batchType) {
        if (batchType == INSERT) {
            return "documents";
        } else if (batchType == UPDATE || batchType == REPLACE) {
            return "updates";
        } else if (batchType == DELETE) {
            return "deletes";
        } else {
            throw getUnsupportedType(batchType);
        }
    }

    @SuppressWarnings("unchecked")
    private Codec<BsonDocument> getCodec(final BsonDocument document) {
        return (Codec<BsonDocument>) REGISTRY.get(document.getClass());
    }

    private UnsupportedOperationException getUnsupportedType(final WriteRequest.Type batchType) {
        return new UnsupportedOperationException(format("Unsupported write of type %s", batchType));
    }

    // make a special exception for a command with only a single item added to it.  It's allowed to exceed maximum document size so that
    // it's possible to, say, send a replacement document that is itself 16MB, which would push the size of the containing command
    // document to be greater than the maximum document size.
    private boolean exceedsLimits(final int batchLength, final int batchItemCount, final int maxDocumentSize, final int maxBatchCount) {
        return (batchLength > maxDocumentSize && batchItemCount > 1) || (batchItemCount > maxBatchCount);
    }

    // Response processing
    private void processCommandResult(final ServerAddress serverAddress, final BulkWriteBatchCombiner bulkWriteBatchCombiner,
                                      final BatchMetadata batchMetadata, final BsonDocument result) {
        if (getWriteConcern().isAcknowledged()) {
            if (hasError(result)) {
                MongoBulkWriteException bulkWriteException = getBulkWriteException(batchMetadata.getBatchType(), result, serverAddress);
                bulkWriteBatchCombiner.addErrorResult(bulkWriteException, batchMetadata.indexMap);
            } else {
                bulkWriteBatchCombiner.addResult(getBulkWriteResult(batchMetadata.getBatchType(), result), batchMetadata.indexMap);
            }
        }
    }

    private boolean hasError(final BsonDocument result) {
        return result.get("writeErrors") != null || result.get("writeConcernError") != null;
    }

    private MongoBulkWriteException getBulkWriteException(final WriteRequest.Type type, final BsonDocument result,
                                                          final ServerAddress serverAddress) {
        if (!hasError(result)) {
            throw new MongoInternalException("This method should not have been called");
        }
        return new MongoBulkWriteException(getBulkWriteResult(type, result), getWriteErrors(result),
                getWriteConcernError(result), serverAddress);
    }

    @SuppressWarnings("unchecked")
    private List<BulkWriteError> getWriteErrors(final BsonDocument result) {
        List<BulkWriteError> writeErrors = new ArrayList<BulkWriteError>();
        BsonArray writeErrorsDocuments = (BsonArray) result.get("writeErrors");
        if (writeErrorsDocuments != null) {
            for (BsonValue cur : writeErrorsDocuments) {
                BsonDocument curDocument = (BsonDocument) cur;
                writeErrors.add(new BulkWriteError(curDocument.getNumber("code").intValue(),
                        curDocument.getString("errmsg").getValue(),
                        curDocument.getDocument("errInfo", new BsonDocument()),
                        curDocument.getNumber("index").intValue()));
            }
        }
        return writeErrors;
    }

    private WriteConcernError getWriteConcernError(final BsonDocument result) {
        BsonDocument writeConcernErrorDocument = (BsonDocument) result.get("writeConcernError");
        if (writeConcernErrorDocument == null) {
            return null;
        } else {
            return new WriteConcernError(writeConcernErrorDocument.getNumber("code").intValue(),
                    writeConcernErrorDocument.getString("errmsg").getValue(),
                    writeConcernErrorDocument.getDocument("errInfo", new BsonDocument()));
        }
    }

    private BulkWriteResult getBulkWriteResult(final WriteRequest.Type type, final BsonDocument result) {
        int count = getCount(result);
        List<BulkWriteUpsert> upsertedItems = getUpsertedItems(result);
        return BulkWriteResult.acknowledged(type, count - upsertedItems.size(), getModifiedCount(type, result), upsertedItems);
    }

    private int getCount(final BsonDocument result) {
        return result.getNumber("n").intValue();
    }

    @SuppressWarnings("unchecked")
    private List<BulkWriteUpsert> getUpsertedItems(final BsonDocument result) {
        BsonValue upsertedValue = result.get("upserted");
        if (upsertedValue == null) {
            return Collections.emptyList();
        } else {
            List<BulkWriteUpsert> bulkWriteUpsertList = new ArrayList<BulkWriteUpsert>();
            for (BsonValue upsertedItem : (BsonArray) upsertedValue) {
                BsonDocument upsertedItemDocument = (BsonDocument) upsertedItem;
                bulkWriteUpsertList.add(new BulkWriteUpsert(upsertedItemDocument.getNumber("index").intValue(),
                        upsertedItemDocument.get("_id")));
            }
            return bulkWriteUpsertList;
        }
    }

    private Integer getModifiedCount(final WriteRequest.Type type, final BsonDocument result) {
        BsonNumber modifiedCount = result.getNumber("nModified", (type == UPDATE || type == REPLACE) ? null : new BsonInt32(0));
        return modifiedCount == null ? null : modifiedCount.intValue();
    }

}
