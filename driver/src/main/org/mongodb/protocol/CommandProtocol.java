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
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;
import org.mongodb.CommandResult;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.connection.ByteBufferOutputBuffer;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerAddress;
import org.mongodb.diagnostics.Loggers;
import org.mongodb.diagnostics.logging.Logger;
import org.mongodb.operation.QueryFlag;
import org.mongodb.operation.SingleResultFuture;
import org.mongodb.operation.SingleResultFutureCallback;
import org.mongodb.protocol.message.CommandMessage;
import org.mongodb.protocol.message.ReplyMessage;

import java.util.EnumSet;

import static java.lang.String.format;
import static org.mongodb.protocol.ProtocolHelper.encodeMessageToBuffer;
import static org.mongodb.protocol.ProtocolHelper.getCommandFailureException;
import static org.mongodb.protocol.ProtocolHelper.getMessageSettings;

public class CommandProtocol implements Protocol<CommandResult> {

    public static final Logger LOGGER = Loggers.getLogger("protocol.command");

    private final MongoNamespace namespace;
    private final BsonDocument command;
    private final Decoder<BsonDocument> commandResultDecoder;
    private final EnumSet<QueryFlag> queryFlags;
    private final FieldNameValidator fieldNameValidator;

    public CommandProtocol(final String database, final BsonDocument command, final EnumSet<QueryFlag> queryFlags,
                           final FieldNameValidator fieldNameValidator, final Decoder<BsonDocument> commandResultDecoder) {
        this.queryFlags = queryFlags;
        this.fieldNameValidator = fieldNameValidator;
        this.namespace = new MongoNamespace(database, MongoNamespace.COMMAND_COLLECTION_NAME);
        this.command = command;
        this.commandResultDecoder = commandResultDecoder;
    }

    public CommandResult execute(final Connection connection) {
        LOGGER.debug(format("Sending command {%s : %s} to database %s on connection [%s] to server %s",
                            command.keySet().iterator().next(), command.values().iterator().next(),
                            namespace.getDatabaseName(), connection.getId(), connection.getServerAddress()));
        CommandResult commandResult = receiveMessage(connection, sendMessage(connection).getId());
        LOGGER.debug("Command execution completed with status " + commandResult.isOk());
        return commandResult;
    }

    private CommandMessage sendMessage(final Connection connection) {
        ByteBufferOutputBuffer buffer = new ByteBufferOutputBuffer(connection);
        try {
            CommandMessage message = new CommandMessage(namespace.getFullName(), command, queryFlags, fieldNameValidator,
                                                        getMessageSettings(connection.getServerDescription()));
            message.encode(buffer);
            connection.sendMessage(buffer.getByteBuffers(), message.getId());
            return message;
        } finally {
            buffer.close();
        }
    }

    private CommandResult receiveMessage(final Connection connection, final int messageId) {
        ResponseBuffers responseBuffers = connection.receiveMessage(messageId);
        try {
            ReplyMessage<BsonDocument> replyMessage = new ReplyMessage<BsonDocument>(responseBuffers, commandResultDecoder, messageId);
            return createCommandResult(replyMessage, connection.getServerAddress());
        } finally {
            responseBuffers.close();
        }
    }

    public MongoFuture<CommandResult> executeAsync(final Connection connection) {
        LOGGER.debug(format("Asynchronously sending command {%s : %s} to database %s on connection [%s] to server %s",
                            command.keySet().iterator().next(), command.values().iterator().next(),
                            namespace.getDatabaseName(), connection.getId(), connection.getServerAddress()));
        SingleResultFuture<CommandResult> retVal = new SingleResultFuture<CommandResult>();

        ByteBufferOutputBuffer buffer = new ByteBufferOutputBuffer(connection);
        CommandMessage message = new CommandMessage(namespace.getFullName(), command, queryFlags, fieldNameValidator,
                                                    getMessageSettings(connection.getServerDescription()));
        encodeMessageToBuffer(message, buffer);

        CommandResultCallback receiveCallback = new CommandResultCallback(new SingleResultFutureCallback<CommandResult>(retVal),
                                                                          commandResultDecoder,
                                                                          message.getId(),
                                                                          connection.getServerAddress());
        connection.sendMessageAsync(buffer.getByteBuffers(),
                                    message.getId(),
                                    new SendMessageCallback<CommandResult>(connection, buffer, message.getId(), retVal, receiveCallback));
        return retVal;
    }

    private CommandResult createCommandResult(final ReplyMessage<BsonDocument> replyMessage, final ServerAddress serverAddress) {
        CommandResult commandResult = new CommandResult(serverAddress,
                                                        replyMessage.getDocuments().get(0),
                                                        replyMessage.getElapsedNanoseconds());
        if (!commandResult.isOk()) {
            throw getCommandFailureException(commandResult);
        }

        return commandResult;
    }
}
