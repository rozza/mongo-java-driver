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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSocketClosedException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.ByteBufNIO;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.util.Collections;
import java.util.List;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.connection.ConnectionDescription.getDefaultMaxMessageSize;
import static com.mongodb.connection.ConnectionDescription.getDefaultMaxWriteBatchSize;
import static com.mongodb.connection.ServerDescription.getDefaultMaxDocumentSize;
import static com.mongodb.internal.operation.ServerVersionHelper.LATEST_WIRE_VERSION;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class InternalStreamConnectionTest {

    private static final ServerId SERVER_ID = new ServerId(new ClusterId(), new ServerAddress());

    private final String database = "admin";
    private final NoOpFieldNameValidator fieldNameValidator = NoOpFieldNameValidator.INSTANCE;
    private final StreamHelper helper = new StreamHelper();
    private final ServerAddress serverAddress = new ServerAddress();
    private final ConnectionId connectionId = new ConnectionId(SERVER_ID, 1L, 1L);
    private final TestCommandListener commandListener = new TestCommandListener();
    private final MessageSettings messageSettings = MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build();

    private final ConnectionDescription connectionDescription = new ConnectionDescription(connectionId, 3,
            ServerType.STANDALONE, getDefaultMaxWriteBatchSize(), getDefaultMaxDocumentSize(),
            getDefaultMaxMessageSize(), Collections.emptyList());
    private final ServerDescription serverDescription = ServerDescription.builder()
            .ok(true)
            .state(ServerConnectionState.CONNECTED)
            .type(ServerType.STANDALONE)
            .address(serverAddress)
            .build();
    private final InternalConnectionInitializationDescription internalConnectionInitializationDescription =
            new InternalConnectionInitializationDescription(connectionDescription, serverDescription);

    private Stream getStream() {
        Stream stream = mock(Stream.class);
        doAnswer(invocation -> {
            ((com.mongodb.connection.AsyncCompletionHandler<Void>) invocation.getArgument(1))
                    .completed(null);
            return null;
        }).when(stream).openAsync(any(), any());
        return stream;
    }

    private StreamFactory getStreamFactory(Stream stream) {
        StreamFactory streamFactory = mock(StreamFactory.class);
        when(streamFactory.create(any())).thenReturn(stream);
        return streamFactory;
    }

    private InternalConnectionInitializer getInitializer() {
        InternalConnectionInitializer initializer = mock(InternalConnectionInitializer.class);
        when(initializer.startHandshake(any(), any())).thenReturn(internalConnectionInitializationDescription);
        when(initializer.finishHandshake(any(), any(), any())).thenReturn(internalConnectionInitializationDescription);
        doAnswer(invocation -> {
            ((com.mongodb.internal.async.SingleResultCallback<InternalConnectionInitializationDescription>)
                    invocation.getArgument(2)).onResult(internalConnectionInitializationDescription, null);
            return null;
        }).when(initializer).startHandshakeAsync(any(), any(), any());
        doAnswer(invocation -> {
            ((com.mongodb.internal.async.SingleResultCallback<InternalConnectionInitializationDescription>)
                    invocation.getArgument(3)).onResult(internalConnectionInitializationDescription, null);
            return null;
        }).when(initializer).finishHandshakeAsync(any(), any(), any(), any());
        return initializer;
    }

    private InternalStreamConnection getConnection() {
        Stream stream = getStream();
        return new InternalStreamConnection(SINGLE, SERVER_ID, new TestConnectionGenerationSupplier(),
                getStreamFactory(stream), Collections.emptyList(), commandListener, getInitializer());
    }

    private InternalStreamConnection getOpenedConnection() {
        InternalStreamConnection connection = getConnection();
        connection.open(OPERATION_CONTEXT);
        return connection;
    }

    @Test
    void shouldChangeTheDescriptionWhenOpened() {
        InternalStreamConnection connection = getConnection();

        assertEquals(ServerType.UNKNOWN, connection.getDescription().getServerType());
        assertEquals(null, connection.getDescription().getConnectionId().getServerValue());
        assertEquals(ServerDescription.builder()
                        .address(serverAddress)
                        .type(ServerType.UNKNOWN)
                        .state(ServerConnectionState.CONNECTING)
                        .lastUpdateTimeNanos(connection.getInitialServerDescription().getLastUpdateTime(NANOSECONDS))
                        .build(),
                connection.getInitialServerDescription());

        connection.open(OPERATION_CONTEXT);

        assertTrue(connection.opened());
        assertEquals(ServerType.STANDALONE, connection.getDescription().getServerType());
        assertEquals(Long.valueOf(1), connection.getDescription().getConnectionId().getServerValue());
        assertEquals(connectionDescription, connection.getDescription());
        assertEquals(serverDescription, connection.getInitialServerDescription());
    }

    @Test
    void shouldChangeTheDescriptionWhenOpenedAsynchronously() {
        InternalStreamConnection connection = getConnection();

        assertEquals(ServerType.UNKNOWN, connection.getDescription().getServerType());
        assertEquals(null, connection.getDescription().getConnectionId().getServerValue());

        FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<>();
        connection.openAsync(OPERATION_CONTEXT, futureResultCallback);
        futureResultCallback.get();

        assertTrue(connection.opened());
        assertEquals(connectionDescription, connection.getDescription());
        assertEquals(serverDescription, connection.getInitialServerDescription());
    }

    @Test
    void shouldCloseTheStreamWhenInitializationThrowsAnException() {
        InternalConnectionInitializer failedInitializer = mock(InternalConnectionInitializer.class);
        when(failedInitializer.startHandshake(any(), any()))
                .thenThrow(new MongoInternalException("Something went wrong"));
        Stream stream = getStream();
        InternalStreamConnection connection = new InternalStreamConnection(SINGLE, SERVER_ID,
                new TestConnectionGenerationSupplier(), getStreamFactory(stream), Collections.emptyList(),
                null, failedInitializer);

        assertThrows(MongoInternalException.class, () -> connection.open(OPERATION_CONTEXT));
        assertTrue(connection.isClosed());
    }

    @Test
    void shouldCloseTheStreamWhenInitializationThrowsAnExceptionAsynchronously() {
        InternalConnectionInitializer failedInitializer = mock(InternalConnectionInitializer.class);
        doAnswer(invocation -> {
            ((com.mongodb.internal.async.SingleResultCallback<InternalConnectionInitializationDescription>)
                    invocation.getArgument(2)).onResult(null, new MongoInternalException("Something went wrong"));
            return null;
        }).when(failedInitializer).startHandshakeAsync(any(), any(), any());
        Stream stream = getStream();
        InternalStreamConnection connection = new InternalStreamConnection(SINGLE, SERVER_ID,
                new TestConnectionGenerationSupplier(), getStreamFactory(stream), Collections.emptyList(),
                null, failedInitializer);

        FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<>();
        connection.openAsync(OPERATION_CONTEXT, futureResultCallback);
        assertThrows(MongoInternalException.class, () -> futureResultCallback.get());
        assertTrue(connection.isClosed());
    }

    @Test
    void shouldCloseTheStreamWhenWritingAMessageThrowsAnException() throws IOException {
        Stream stream = getStream();
        StreamFactory streamFactory = getStreamFactory(stream);
        InternalConnectionInitializer initializer = getInitializer();
        doThrow(new IOException("Something went wrong")).when(stream).write(any(), any());

        InternalStreamConnection connection = new InternalStreamConnection(SINGLE, SERVER_ID,
                new TestConnectionGenerationSupplier(), streamFactory, Collections.emptyList(),
                commandListener, initializer);
        connection.open(OPERATION_CONTEXT);

        Object[] hello1 = StreamHelper.hello();
        @SuppressWarnings("unchecked")
        List<org.bson.ByteBuf> buffers1 = (List<org.bson.ByteBuf>) hello1[0];
        int messageId1 = (int) hello1[1];

        assertThrows(MongoSocketWriteException.class,
                () -> connection.sendMessage(buffers1, messageId1, OPERATION_CONTEXT));
        assertTrue(connection.isClosed());

        Object[] hello2 = StreamHelper.hello();
        @SuppressWarnings("unchecked")
        List<org.bson.ByteBuf> buffers2 = (List<org.bson.ByteBuf>) hello2[0];
        int messageId2 = (int) hello2[1];

        assertThrows(MongoSocketClosedException.class,
                () -> connection.sendMessage(buffers2, messageId2, OPERATION_CONTEXT));
    }

    @Test
    void shouldCloseTheStreamWhenReadingTheMessageHeaderThrowsAnException() throws IOException {
        Stream stream = getStream();
        StreamFactory streamFactory = getStreamFactory(stream);
        InternalConnectionInitializer initializer = getInitializer();
        when(stream.read(anyInt(), any())).thenThrow(new IOException("Something went wrong"));

        InternalStreamConnection connection = new InternalStreamConnection(SINGLE, SERVER_ID,
                new TestConnectionGenerationSupplier(), streamFactory, Collections.emptyList(),
                commandListener, initializer);
        connection.open(OPERATION_CONTEXT);

        Object[] hello1 = StreamHelper.hello();
        @SuppressWarnings("unchecked")
        List<org.bson.ByteBuf> buffers1 = (List<org.bson.ByteBuf>) hello1[0];
        int messageId1 = (int) hello1[1];
        Object[] hello2 = StreamHelper.hello();
        int messageId2 = (int) hello2[1];

        // sendMessage should succeed since stream.write is not throwing
        // receiveMessage should fail since stream.read throws
        assertThrows(MongoSocketReadException.class,
                () -> connection.receiveMessage(messageId1, OPERATION_CONTEXT));
        assertTrue(connection.isClosed());

        assertThrows(MongoSocketClosedException.class,
                () -> connection.receiveMessage(messageId2, OPERATION_CONTEXT));
    }

    @Test
    void shouldThrowMongoInterruptedExceptionWhenStreamWriteThrowsInterruptedIOException() throws IOException {
        Stream stream = getStream();
        StreamFactory streamFactory = getStreamFactory(stream);
        InternalConnectionInitializer initializer = getInitializer();
        doThrow(new InterruptedIOException()).when(stream).write(any(), any());

        InternalStreamConnection connection = new InternalStreamConnection(SINGLE, SERVER_ID,
                new TestConnectionGenerationSupplier(), streamFactory, Collections.emptyList(),
                commandListener, initializer);
        connection.open(OPERATION_CONTEXT);

        assertThrows(MongoInterruptedException.class, () ->
                connection.sendMessage(
                        Collections.singletonList(new ByteBufNIO(ByteBuffer.allocate(1))), 1, OPERATION_CONTEXT));
        assertTrue(connection.isClosed());
    }

    @Test
    void shouldThrowMongoInterruptedExceptionWhenStreamWriteThrowsClosedByInterruptException() throws IOException {
        Stream stream = getStream();
        StreamFactory streamFactory = getStreamFactory(stream);
        InternalConnectionInitializer initializer = getInitializer();
        doThrow(new ClosedByInterruptException()).when(stream).write(any(), any());

        InternalStreamConnection connection = new InternalStreamConnection(SINGLE, SERVER_ID,
                new TestConnectionGenerationSupplier(), streamFactory, Collections.emptyList(),
                commandListener, initializer);
        connection.open(OPERATION_CONTEXT);
        Thread.currentThread().interrupt();

        assertThrows(MongoInterruptedException.class, () ->
                connection.sendMessage(
                        Collections.singletonList(new ByteBufNIO(ByteBuffer.allocate(1))), 1, OPERATION_CONTEXT));
        Thread.interrupted(); // clear the interrupt flag
        assertTrue(connection.isClosed());
    }

    @Test
    void shouldNotCloseTheStreamOnACommandException() throws IOException {
        Stream stream = getStream();
        StreamFactory streamFactory = getStreamFactory(stream);
        InternalConnectionInitializer initializer = getInitializer();
        InternalStreamConnection connection = new InternalStreamConnection(SINGLE, SERVER_ID,
                new TestConnectionGenerationSupplier(), streamFactory, Collections.emptyList(),
                commandListener, initializer);
        connection.open(OPERATION_CONTEXT);

        BsonDocument pingCommandDocument = new BsonDocument("ping", new BsonInt32(1));
        CommandMessage commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator,
                primary(), messageSettings, MULTIPLE, null);
        String response = "{ok : 0, errmsg : \"failed\"}";
        when(stream.getBuffer(1024)).thenReturn(new ByteBufNIO(ByteBuffer.wrap(new byte[1024])));
        when(stream.read(anyInt(), any())).thenReturn(StreamHelper.reply(response));
        when(stream.read(eq(16), any())).thenReturn(StreamHelper.messageHeader(commandMessage.getId(), response));

        assertThrows(MongoCommandException.class, () ->
                connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), OPERATION_CONTEXT));
        assertFalse(connection.isClosed());
    }

    @Test
    void shouldSendEventsForSuccessfulCommand() throws IOException {
        Stream stream = getStream();
        StreamFactory streamFactory = getStreamFactory(stream);
        InternalConnectionInitializer initializer = getInitializer();
        InternalStreamConnection connection = new InternalStreamConnection(SINGLE, SERVER_ID,
                new TestConnectionGenerationSupplier(), streamFactory, Collections.emptyList(),
                commandListener, initializer);
        connection.open(OPERATION_CONTEXT);

        BsonDocument pingCommandDocument = new BsonDocument("ping", new BsonInt32(1));
        CommandMessage commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator,
                primary(), messageSettings, MULTIPLE, null);
        when(stream.getBuffer(1024)).thenReturn(new ByteBufNIO(ByteBuffer.wrap(new byte[1024])));
        when(stream.read(eq(16), any())).thenReturn(StreamHelper.defaultMessageHeader(commandMessage.getId()));
        when(stream.read(eq(90), any())).thenReturn(StreamHelper.defaultReply());

        connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), OPERATION_CONTEXT);

        assertEquals(2, commandListener.getEvents().size());
    }

    @Test
    void shouldSendEventsForCommandFailureWithExceptionWritingMessage() throws IOException {
        Stream stream = getStream();
        StreamFactory streamFactory = getStreamFactory(stream);
        InternalConnectionInitializer initializer = getInitializer();
        InternalStreamConnection connection = new InternalStreamConnection(SINGLE, SERVER_ID,
                new TestConnectionGenerationSupplier(), streamFactory, Collections.emptyList(),
                commandListener, initializer);
        connection.open(OPERATION_CONTEXT);

        BsonDocument pingCommandDocument = new BsonDocument("ping", new BsonInt32(1));
        CommandMessage commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator,
                primary(), messageSettings, MULTIPLE, null);
        when(stream.getBuffer(1024)).thenReturn(new ByteBufNIO(ByteBuffer.wrap(new byte[1024])));
        doThrow(new MongoSocketWriteException("Failed to write", serverAddress, new IOException()))
                .when(stream).write(any(), any());

        assertThrows(MongoSocketWriteException.class, () ->
                connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), OPERATION_CONTEXT));
        assertEquals(2, commandListener.getEvents().size());
    }

    @Test
    void shouldSendEventsForCommandFailureWithExceptionReadingHeader() throws IOException {
        Stream stream = getStream();
        StreamFactory streamFactory = getStreamFactory(stream);
        InternalConnectionInitializer initializer = getInitializer();
        InternalStreamConnection connection = new InternalStreamConnection(SINGLE, SERVER_ID,
                new TestConnectionGenerationSupplier(), streamFactory, Collections.emptyList(),
                commandListener, initializer);
        connection.open(OPERATION_CONTEXT);

        BsonDocument pingCommandDocument = new BsonDocument("ping", new BsonInt32(1));
        CommandMessage commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator,
                primary(), messageSettings, MULTIPLE, null);
        when(stream.getBuffer(1024)).thenReturn(new ByteBufNIO(ByteBuffer.wrap(new byte[1024])));
        when(stream.read(eq(16), any())).thenThrow(new MongoSocketReadException("Failed to read", serverAddress));

        assertThrows(MongoSocketReadException.class, () ->
                connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), OPERATION_CONTEXT));
        assertEquals(2, commandListener.getEvents().size());
    }

    @Test
    void shouldSendEventsForCommandFailureWithExceptionFromFailedCommand() throws IOException {
        Stream stream = getStream();
        StreamFactory streamFactory = getStreamFactory(stream);
        InternalConnectionInitializer initializer = getInitializer();
        InternalStreamConnection connection = new InternalStreamConnection(SINGLE, SERVER_ID,
                new TestConnectionGenerationSupplier(), streamFactory, Collections.emptyList(),
                commandListener, initializer);
        connection.open(OPERATION_CONTEXT);

        BsonDocument pingCommandDocument = new BsonDocument("ping", new BsonInt32(1));
        CommandMessage commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator,
                primary(), messageSettings, MULTIPLE, null);
        String response = "{ok : 0, errmsg : \"failed\"}";
        when(stream.getBuffer(1024)).thenReturn(new ByteBufNIO(ByteBuffer.wrap(new byte[1024])));
        when(stream.read(anyInt(), any())).thenReturn(StreamHelper.reply(response));
        when(stream.read(eq(16), any())).thenReturn(StreamHelper.messageHeader(commandMessage.getId(), response));

        assertThrows(MongoCommandException.class, () ->
                connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), OPERATION_CONTEXT));
        assertEquals(2, commandListener.getEvents().size());
    }
}
