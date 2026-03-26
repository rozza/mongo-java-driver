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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.WriteRequestWithIndex;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonMaximumSizeExceededException;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.ByteBufNIO;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.mongodb.internal.connection.SplittablePayload.Type.INSERT;
import static com.mongodb.internal.operation.ServerVersionHelper.LATEST_WIRE_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * New tests must be added to {@link CommandMessageTest}.
 */
class CommandMessageUnitTest {

    private final MongoNamespace namespace = new MongoNamespace("db.test");
    private final BsonDocument command = new BsonDocument("find", new BsonString(namespace.getCollectionName()));
    private final NoOpFieldNameValidator fieldNameValidator = NoOpFieldNameValidator.INSTANCE;

    static Stream<Object[]> getCommandDocumentProvider() {
        BsonDocument insertCommand = new BsonDocument("insert", new BsonString("coll"));
        List<WriteRequestWithIndex> writeRequests = new ArrayList<>();
        List<BsonDocument> docs = Arrays.asList(
                new BsonDocument("_id", new BsonInt32(1)),
                new BsonDocument("_id", new BsonInt32(2)));
        for (int i = 0; i < docs.size(); i++) {
            writeRequests.add(new WriteRequestWithIndex(new InsertRequest(docs.get(i)), i));
        }

        return Stream.of(
                new Object[]{
                        LATEST_WIRE_VERSION,
                        insertCommand.clone(),
                        new SplittablePayload(INSERT, writeRequests, true, NoOpFieldNameValidator.INSTANCE)
                },
                new Object[]{
                        LATEST_WIRE_VERSION,
                        new BsonDocument("insert", new BsonString("coll")).append("documents",
                                new BsonArray(Arrays.asList(
                                        new BsonDocument("_id", new BsonInt32(1)),
                                        new BsonDocument("_id", new BsonInt32(2))))),
                        null
                }
        );
    }

    @ParameterizedTest
    @MethodSource("getCommandDocumentProvider")
    void shouldGetCommandDocument(int maxWireVersion, BsonDocument originalCommandDocument,
                                  SplittablePayload payload) {
        CommandMessage message = new CommandMessage(namespace.getDatabaseName(), originalCommandDocument,
                fieldNameValidator, ReadPreference.primary(),
                MessageSettings.builder().maxWireVersion(maxWireVersion).build(), true,
                payload == null ? MessageSequences.EmptyMessageSequences.INSTANCE : payload,
                ClusterConnectionMode.MULTIPLE, null);
        ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider());
        message.encode(output, new OperationContext(IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                mock(TimeoutContext.class), null));

        BsonDocument commandDocument = message.getCommandDocument(output);

        BsonDocument expectedCommandDocument = new BsonDocument("insert", new BsonString("coll")).append("documents",
                new BsonArray(Arrays.asList(
                        new BsonDocument("_id", new BsonInt32(1)),
                        new BsonDocument("_id", new BsonInt32(2)))));
        expectedCommandDocument.append("$db", new BsonString(namespace.getDatabaseName()));
        assertEquals(expectedCommandDocument, commandDocument);
    }

    @Test
    void shouldRespectTheMaxMessageSize() {
        int maxMessageSize = 1024;
        MessageSettings messageSettings = MessageSettings.builder()
                .maxMessageSize(maxMessageSize).maxWireVersion(LATEST_WIRE_VERSION).build();
        BsonDocument insertCommand = new BsonDocument("insert", new BsonString(namespace.getCollectionName()));
        List<WriteRequestWithIndex> writeRequests = new ArrayList<>();
        List<BsonDocument> docs = Arrays.asList(
                new BsonDocument("_id", new BsonInt32(1)).append("a", new BsonBinary(new byte[913])),
                new BsonDocument("_id", new BsonInt32(2)).append("b", new BsonBinary(new byte[441])),
                new BsonDocument("_id", new BsonInt32(3)).append("c", new BsonBinary(new byte[450])),
                new BsonDocument("_id", new BsonInt32(4)).append("b", new BsonBinary(new byte[441])),
                new BsonDocument("_id", new BsonInt32(5)).append("c", new BsonBinary(new byte[451])));
        for (int i = 0; i < docs.size(); i++) {
            writeRequests.add(new WriteRequestWithIndex(new InsertRequest(docs.get(i)), i));
        }
        SplittablePayload payload = new SplittablePayload(INSERT, writeRequests, true, fieldNameValidator);
        SessionContext sessionContext = mock(SessionContext.class);
        when(sessionContext.getReadConcern()).thenReturn(ReadConcern.DEFAULT);
        ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider());

        // First split
        CommandMessage message = new CommandMessage(namespace.getDatabaseName(), insertCommand, fieldNameValidator,
                ReadPreference.primary(), messageSettings, false, payload, ClusterConnectionMode.MULTIPLE, null);
        message.encode(output, new OperationContext(IgnorableRequestContext.INSTANCE, sessionContext,
                mock(TimeoutContext.class), null));
        ByteBufNIO byteBuf = new ByteBufNIO(ByteBuffer.wrap(output.toByteArray()));
        MessageHeader messageHeader = new MessageHeader(byteBuf, maxMessageSize);
        assertEquals(OpCode.OP_MSG.getValue(), messageHeader.getOpCode());
        assertEquals(1005, messageHeader.getMessageLength());
        assertEquals(1, payload.getPosition());
        assertTrue(payload.hasAnotherSplit());

        // Second split
        payload = payload.getNextSplit();
        message = new CommandMessage(namespace.getDatabaseName(), insertCommand, fieldNameValidator,
                ReadPreference.primary(), messageSettings, false, payload, ClusterConnectionMode.MULTIPLE, null);
        output.truncateToPosition(0);
        message.encode(output, new OperationContext(IgnorableRequestContext.INSTANCE, sessionContext,
                mock(TimeoutContext.class), null));
        byteBuf = new ByteBufNIO(ByteBuffer.wrap(output.toByteArray()));
        messageHeader = new MessageHeader(byteBuf, maxMessageSize);
        assertEquals(1005, messageHeader.getMessageLength());
        assertEquals(2, payload.getPosition());
        assertTrue(payload.hasAnotherSplit());

        // Third split (contains remaining 2 docs due to reduced overhead)
        payload = payload.getNextSplit();
        message = new CommandMessage(namespace.getDatabaseName(), insertCommand, fieldNameValidator,
                ReadPreference.primary(), messageSettings, false, payload, ClusterConnectionMode.MULTIPLE, null);
        output.truncateToPosition(0);
        message.encode(output, new OperationContext(IgnorableRequestContext.INSTANCE, sessionContext,
                mock(TimeoutContext.class), null));
        byteBuf = new ByteBufNIO(ByteBuffer.wrap(output.toByteArray()));
        messageHeader = new MessageHeader(byteBuf, maxMessageSize);
        assertEquals(1006, messageHeader.getMessageLength());
        assertEquals(1 << 1, byteBuf.getInt());
        assertEquals(2, payload.getPosition());
        assertFalse(payload.hasAnotherSplit());

        output.close();
    }

    @Test
    void shouldRespectTheMaxBatchCount() {
        MessageSettings messageSettings = MessageSettings.builder().maxBatchCount(2)
                .maxWireVersion(LATEST_WIRE_VERSION).build();
        List<WriteRequestWithIndex> writeRequests = new ArrayList<>();
        List<BsonDocument> docs = Arrays.asList(
                new BsonDocument("a", new BsonBinary(new byte[900])),
                new BsonDocument("b", new BsonBinary(new byte[450])),
                new BsonDocument("c", new BsonBinary(new byte[450])));
        for (int i = 0; i < docs.size(); i++) {
            writeRequests.add(new WriteRequestWithIndex(new InsertRequest(docs.get(i)), i));
        }
        SplittablePayload payload = new SplittablePayload(INSERT, writeRequests, true, fieldNameValidator);
        SessionContext sessionContext = mock(SessionContext.class);
        when(sessionContext.getReadConcern()).thenReturn(ReadConcern.DEFAULT);
        ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider());

        CommandMessage message = new CommandMessage(namespace.getDatabaseName(), command, fieldNameValidator,
                ReadPreference.primary(), messageSettings, false, payload, ClusterConnectionMode.MULTIPLE, null);
        message.encode(output, new OperationContext(IgnorableRequestContext.INSTANCE, sessionContext,
                mock(TimeoutContext.class), null));
        ByteBufNIO byteBuf = new ByteBufNIO(ByteBuffer.wrap(output.toByteArray()));
        MessageHeader messageHeader = new MessageHeader(byteBuf, 2048);
        assertEquals(OpCode.OP_MSG.getValue(), messageHeader.getOpCode());
        assertEquals(1478, messageHeader.getMessageLength());
        assertEquals(0, byteBuf.getInt());
        assertEquals(2, payload.getPosition());
        assertTrue(payload.hasAnotherSplit());

        payload = payload.getNextSplit();
        message = new CommandMessage(namespace.getDatabaseName(), command, fieldNameValidator,
                ReadPreference.primary(), messageSettings, false, payload, ClusterConnectionMode.MULTIPLE, null);
        output.truncateToPosition(0);
        message.encode(output, new OperationContext(IgnorableRequestContext.INSTANCE, sessionContext,
                mock(TimeoutContext.class), null));
        byteBuf = new ByteBufNIO(ByteBuffer.wrap(output.toByteArray()));
        messageHeader = new MessageHeader(byteBuf, 1024);
        assertEquals(OpCode.OP_MSG.getValue(), messageHeader.getOpCode());
        assertEquals(1 << 1, byteBuf.getInt());
        assertEquals(1, payload.getPosition());
        assertFalse(payload.hasAnotherSplit());

        output.close();
    }

    @Test
    void shouldThrowIfPayloadDocumentBiggerThanMaxDocumentSize() {
        MessageSettings messageSettings = MessageSettings.builder().maxDocumentSize(900)
                .maxWireVersion(LATEST_WIRE_VERSION).build();
        List<WriteRequestWithIndex> writeRequests = new ArrayList<>();
        writeRequests.add(new WriteRequestWithIndex(
                new InsertRequest(new BsonDocument("a", new BsonBinary(new byte[900]))), 0));
        SplittablePayload payload = new SplittablePayload(INSERT, writeRequests, true, fieldNameValidator);
        CommandMessage message = new CommandMessage(namespace.getDatabaseName(), command, fieldNameValidator,
                ReadPreference.primary(), messageSettings, false, payload, ClusterConnectionMode.MULTIPLE, null);
        ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider());
        SessionContext sessionContext = mock(SessionContext.class);
        when(sessionContext.getReadConcern()).thenReturn(ReadConcern.DEFAULT);

        assertThrows(BsonMaximumSizeExceededException.class, () ->
                message.encode(output, new OperationContext(IgnorableRequestContext.INSTANCE, sessionContext,
                        mock(TimeoutContext.class), null)));

        output.close();
    }
}
