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

package com.mongodb.connection;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ProtocolHelper.getMessageSettings;
import static java.lang.String.format;

class SplittablePayloadCommandProtocol<T> implements CommandProtocol<T> {
    public static final Logger LOGGER = Loggers.getLogger("protocol.command");
    private final MongoNamespace namespace;
    private final BsonDocument command;
    private final SplittablePayload payload;
    private final ReadPreference readPreference;
    private final FieldNameValidator commandFieldNameValidator;
    private final FieldNameValidator payloadFieldNameValidator;
    private final Decoder<T> commandResultDecoder;
    private final boolean responseExpected;
    private SessionContext sessionContext;

    SplittablePayloadCommandProtocol(final String database, final BsonDocument command, final SplittablePayload payload,
                                     final ReadPreference readPreference, final FieldNameValidator commandFieldNameValidator,
                                     final FieldNameValidator payloadFieldNameValidator, final Decoder<T> commandResultDecoder,
                                     final boolean responseExpected) {
        notNull("database", database);
        this.namespace = new MongoNamespace(notNull("database", database), MongoNamespace.COMMAND_COLLECTION_NAME);
        this.command = notNull("command", command);
        this.payload = notNull("payload", payload);
        this.readPreference = notNull("readPreference", readPreference);
        this.commandFieldNameValidator = notNull("commandFieldNameValidator", commandFieldNameValidator);
        this.payloadFieldNameValidator = notNull("payloadFieldNameValidator", payloadFieldNameValidator);
        this.commandResultDecoder = notNull("commandResultDecoder", commandResultDecoder);
        this.responseExpected = responseExpected;
    }

    @Override
    public T execute(final InternalConnection connection) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Sending command {%s : %s} to database %s on connection [%s] to server %s",
                                getCommandName(), command.values().iterator().next(),
                                namespace.getDatabaseName(), connection.getDescription().getConnectionId(),
                                connection.getDescription().getServerAddress()));
        }
        SplittablePayloadCommandMessage commandMessage = getSplittablePayloadCommandMessage(connection);
        T retval = connection.sendAndReceive(commandMessage, commandResultDecoder, sessionContext);
        LOGGER.debug("Command execution completed");
        return retval;
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<T> callback) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Asynchronously sending command {%s : %s} to database %s on connection [%s] to server %s",
                                    getCommandName(), command.values().iterator().next(),
                                    namespace.getDatabaseName(), connection.getDescription().getConnectionId(),
                                    connection.getDescription().getServerAddress()));
            }

            SplittablePayloadCommandMessage commandMessage = getSplittablePayloadCommandMessage(connection);
            connection.sendAndReceiveAsync(commandMessage, commandResultDecoder, sessionContext, new SingleResultCallback<T>() {
                @Override
                public void onResult(final T result, final Throwable t) {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        callback.onResult(result, null);
                    }
                }
            });
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    @Override
    public SplittablePayloadCommandProtocol<T> sessionContext(final SessionContext sessionContext) {
        this.sessionContext = sessionContext;
        return this;
    }

    private SplittablePayloadCommandMessage getSplittablePayloadCommandMessage(final InternalConnection connection) {
        return new SplittablePayloadCommandMessage(namespace, command, payload, readPreference, commandFieldNameValidator,
                payloadFieldNameValidator, getMessageSettings(connection.getDescription()), responseExpected);
    }

    private String getCommandName() {
        return command.keySet().iterator().next();
    }
}
