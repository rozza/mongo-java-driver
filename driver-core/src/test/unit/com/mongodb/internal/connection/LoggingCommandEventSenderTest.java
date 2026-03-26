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

package com.mongodb.internal.connection;

import com.mongodb.LoggerSettings;
import com.mongodb.MongoInternalException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerId;
import com.mongodb.event.CommandListener;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.logging.StructuredLogger;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.HashSet;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.internal.operation.ServerVersionHelper.LATEST_WIRE_VERSION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LoggingCommandEventSenderTest {

    static Stream<Boolean> debugLoggingProvider() {
        return Stream.of(true, false);
    }

    @ParameterizedTest
    @MethodSource("debugLoggingProvider")
    void shouldSendEvents(boolean debugLoggingEnabled) {
        ConnectionDescription connectionDescription = new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress()));
        String database = "test";
        MessageSettings messageSettings = MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build();
        TestCommandListener commandListener = new TestCommandListener();
        BsonDocument commandDocument = new BsonDocument("ping", new BsonInt32(1));
        BsonDocument replyDocument = new BsonDocument("ok", new BsonInt32(1));
        MongoInternalException failureException = new MongoInternalException("failure!");
        CommandMessage message = new CommandMessage(database, commandDocument,
                NoOpFieldNameValidator.INSTANCE, ReadPreference.primary(), messageSettings, MULTIPLE, null);
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider());
        message.encode(bsonOutput, new OperationContext(IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                mock(TimeoutContext.class), null));
        Logger logger = mock(Logger.class);
        when(logger.isDebugEnabled()).thenReturn(debugLoggingEnabled);

        OperationContext operationContext = OPERATION_CONTEXT;
        LoggingCommandEventSender sender = new LoggingCommandEventSender(new HashSet<>(), new HashSet<>(),
                connectionDescription, commandListener, operationContext, message, message.getCommandDocument(bsonOutput),
                new StructuredLogger(logger), LoggerSettings.builder().build());

        sender.sendStartedEvent();
        sender.sendSucceededEventForOneWayCommand();
        sender.sendSucceededEvent(MessageHelper.buildSuccessfulReply(message.getId(), replyDocument.toJson()));
        sender.sendFailedEvent(failureException);

        // Verify 4 events were delivered: started, succeeded (one-way), succeeded, failed
        assertEquals(4, commandListener.getEvents().size());
    }

    @Test
    void shouldLogLargeCommandWithEllipses() {
        ServerId serverId = new ServerId(new ClusterId(), new ServerAddress());
        ConnectionDescription connectionDescription = new ConnectionDescription(serverId)
                .withConnectionId(new ConnectionId(serverId, 42, 1000L));
        String database = "test";
        MessageSettings messageSettings = MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build();
        BsonDocument commandDocument = new BsonDocument("fake", new BsonBinary(new byte[2048]));
        CommandMessage message = new CommandMessage(database, commandDocument, NoOpFieldNameValidator.INSTANCE,
                ReadPreference.primary(), messageSettings, SINGLE, null);
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider());
        message.encode(bsonOutput, new OperationContext(IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                mock(TimeoutContext.class), null));
        Logger logger = mock(Logger.class);
        when(logger.isDebugEnabled()).thenReturn(true);

        OperationContext operationContext = OPERATION_CONTEXT;
        LoggingCommandEventSender sender = new LoggingCommandEventSender(new HashSet<>(), new HashSet<>(),
                connectionDescription, null, operationContext, message, message.getCommandDocument(bsonOutput),
                new StructuredLogger(logger), LoggerSettings.builder().build());

        sender.sendStartedEvent();

        verify(logger).debug(argThat(arg -> arg instanceof String && ((String) arg).endsWith("...")));
    }

    @Test
    void shouldLogRedactedCommandWithEmptyBody() {
        ServerId serverId = new ServerId(new ClusterId(), new ServerAddress());
        ConnectionDescription connectionDescription = new ConnectionDescription(serverId)
                .withConnectionId(new ConnectionId(serverId, 42, 1000L));
        String database = "test";
        MessageSettings messageSettings = MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build();
        BsonDocument commandDocument = new BsonDocument("createUser", new BsonString("private"));
        CommandMessage message = new CommandMessage(database, commandDocument, NoOpFieldNameValidator.INSTANCE,
                ReadPreference.primary(), messageSettings, SINGLE, null);
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider());
        message.encode(bsonOutput, new OperationContext(IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                mock(TimeoutContext.class), null));
        Logger logger = mock(Logger.class);
        when(logger.isDebugEnabled()).thenReturn(true);

        OperationContext operationContext = OPERATION_CONTEXT;
        LoggingCommandEventSender sender = new LoggingCommandEventSender(
                Collections.singleton("createUser"), new HashSet<>(),
                connectionDescription, null, operationContext, message, message.getCommandDocument(bsonOutput),
                new StructuredLogger(logger), LoggerSettings.builder().build());

        sender.sendStartedEvent();

        verify(logger).debug(argThat(arg -> arg instanceof String && ((String) arg).contains("Command: {}")));
    }
}
