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

package org.mongodb.protocol;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.codecs.BsonDocumentCodec;
import org.mongodb.CommandResult;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.WriteResult;
import org.mongodb.connection.ByteBufferOutputBuffer;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.diagnostics.logging.Logger;
import org.mongodb.operation.QueryFlag;
import org.mongodb.operation.SingleResultFuture;
import org.mongodb.protocol.message.CommandMessage;
import org.mongodb.protocol.message.MessageSettings;
import org.mongodb.protocol.message.ReplyMessage;
import org.mongodb.protocol.message.RequestMessage;

import java.util.EnumSet;

import static java.lang.String.format;
import static org.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;
import static org.mongodb.protocol.ProtocolHelper.encodeMessageToBuffer;
import static org.mongodb.protocol.ProtocolHelper.getMessageSettings;

public abstract class WriteProtocol implements Protocol<WriteResult> {

    private final MongoNamespace namespace;
    private final boolean ordered;
    private final WriteConcern writeConcern;

    public WriteProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern) {
        this.namespace = namespace;
        this.ordered = ordered;
        this.writeConcern = writeConcern;
    }

    public WriteResult execute(final Connection connection) {
        return receiveMessage(connection, sendMessage(connection));
    }

    public MongoFuture<WriteResult> executeAsync(final Connection connection) {
        SingleResultFuture<WriteResult> retVal = new SingleResultFuture<WriteResult>();

        ByteBufferOutputBuffer buffer = new ByteBufferOutputBuffer(connection);
        RequestMessage requestMessage = createRequestMessage(getMessageSettings(connection.getServerDescription()));
        RequestMessage nextMessage = encodeMessageToBuffer(requestMessage, buffer);
        if (writeConcern.isAcknowledged()) {
            CommandMessage getLastErrorMessage = new CommandMessage(new MongoNamespace(getNamespace().getDatabaseName(),
                                                                                       COMMAND_COLLECTION_NAME).getFullName(),
                                                                    createGetLastErrorCommandDocument(),
                                                                    EnumSet.noneOf(QueryFlag.class),
                                                                    getMessageSettings(connection.getServerDescription())
            );
            encodeMessageToBuffer(getLastErrorMessage, buffer);
            connection.sendMessageAsync(buffer.getByteBuffers(), getLastErrorMessage.getId(),
                                        new SendMessageCallback<WriteResult>(connection, buffer, getLastErrorMessage.getId(), retVal,
                                                                             new WriteResultCallback(retVal, new BsonDocumentCodec(),
                                                                                                     getNamespace(), nextMessage,
                                                                                                     ordered, writeConcern,
                                                                                                     getLastErrorMessage.getId(),
                                                                                                     connection)));
        } else {
            connection.sendMessageAsync(buffer.getByteBuffers(), requestMessage.getId(),
                                        new UnacknowledgedWriteResultCallback(retVal, getNamespace(), nextMessage, ordered, buffer,
                                                                              connection));
        }
        return retVal;
    }


    private CommandMessage sendMessage(final Connection connection) {
        ByteBufferOutputBuffer buffer = new ByteBufferOutputBuffer(connection);
        try {
            RequestMessage lastMessage = createRequestMessage(getMessageSettings(connection.getServerDescription()));
            RequestMessage nextMessage = lastMessage.encode(buffer);
            int batchNum = 1;
            if (nextMessage != null) {
                getLogger().debug(format("Sending batch %d", batchNum));
            }

            while (nextMessage != null) {
                batchNum++;
                getLogger().debug(format("Sending batch %d", batchNum));
                lastMessage = nextMessage;
                nextMessage = nextMessage.encode(buffer);
            }
            CommandMessage getLastErrorMessage = null;
            if (writeConcern.isAcknowledged()) {
                getLastErrorMessage = new CommandMessage(new MongoNamespace(getNamespace().getDatabaseName(),
                                                                            COMMAND_COLLECTION_NAME).getFullName(),
                                                         createGetLastErrorCommandDocument(), EnumSet.noneOf(QueryFlag.class),
                                                         getMessageSettings(connection.getServerDescription())
                );
                getLastErrorMessage.encode(buffer);
                lastMessage = getLastErrorMessage;
            }
            connection.sendMessage(buffer.getByteBuffers(), lastMessage.getId());
            return getLastErrorMessage;
        } finally {
            buffer.close();
        }
    }

    private BsonDocument createGetLastErrorCommandDocument() {
        BsonDocument command = new BsonDocument("getlasterror", new BsonInt32(1));
        command.putAll(writeConcern.asDocument());
        return command;
    }

    private WriteResult receiveMessage(final Connection connection, final RequestMessage requestMessage) {
        if (requestMessage == null) {
            return new UnacknowledgedWriteResult();
        }
        ResponseBuffers responseBuffers = connection.receiveMessage(requestMessage.getId());
        try {
            ReplyMessage<BsonDocument> replyMessage = new ReplyMessage<BsonDocument>(responseBuffers, new BsonDocumentCodec(),
                                                                                     requestMessage.getId());
            return ProtocolHelper.getWriteResult(new CommandResult(connection.getServerAddress(), replyMessage.getDocuments().get(0),
                                                                   replyMessage.getElapsedNanoseconds()));
        } finally {
            responseBuffers.close();
        }
    }

    protected abstract RequestMessage createRequestMessage(final MessageSettings settings);

    protected MongoNamespace getNamespace() {
        return namespace;
    }

    protected boolean isOrdered() {
        return ordered;
    }

    protected WriteConcern getWriteConcern() {
        return writeConcern;
    }

    protected abstract Logger getLogger();
}
