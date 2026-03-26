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

package com.mongodb.client.internal;

import com.mongodb.ClusterFixture;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerId;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.WriteRequestWithIndex;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.MessageSequences;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.SplittablePayload;
import com.mongodb.internal.time.Timeout;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.mongodb.connection.ServerType.STANDALONE;
import static com.mongodb.internal.connection.SplittablePayload.Type.INSERT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CryptConnectionTest {

    @Test
    @DisplayName("should encrypt and decrypt a command")
    void shouldEncryptAndDecryptACommand() {
        Connection wrappedConnection = mock(Connection.class);
        Crypt crypt = mock(Crypt.class);
        CryptConnection cryptConnection = new CryptConnection(wrappedConnection, crypt);
        DocumentCodec codec = new DocumentCodec();
        TimeoutContext timeoutContext = mock(TimeoutContext.class);
        OperationContext operationContext = ClusterFixture.OPERATION_CONTEXT.withTimeoutContext(timeoutContext);
        Timeout operationTimeout = mock(Timeout.class);
        when(timeoutContext.getTimeout()).thenReturn(operationTimeout);

        RawBsonDocument encryptedCommand = toRaw(new BsonDocument("find", new BsonString("test"))
                .append("ssid", new BsonBinary((byte) 6, new byte[10])));

        RawBsonDocument encryptedResponse = toRaw(new BsonDocument("ok", new BsonInt32(1))
                .append("cursor",
                        new BsonDocument("firstBatch",
                                new BsonArray(Arrays.asList(new BsonDocument("_id", new BsonInt32(1))
                                        .append("ssid", new BsonBinary((byte) 6, new byte[10])))))));

        RawBsonDocument decryptedResponse = toRaw(new BsonDocument("ok", new BsonInt32(1))
                .append("cursor", new BsonDocument("firstBatch",
                        new BsonArray(Arrays.asList(new BsonDocument("_id", new BsonInt32(1))
                                .append("ssid", new BsonString("555-55-5555")))))));

        when(wrappedConnection.getDescription()).thenReturn(
                new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())), 8, STANDALONE,
                        1000, 1024 * 16_000, 1024 * 48_000, Collections.emptyList()));

        when(crypt.encrypt("db", toRaw(new BsonDocument("find", new BsonString("test"))
                .append("filter", new BsonDocument("ssid", new BsonString("555-55-5555")))), operationTimeout))
                .thenReturn(encryptedCommand);

        when(wrappedConnection.command(eq("db"), eq(encryptedCommand), any(NoOpFieldNameValidator.class),
                eq(ReadPreference.primary()), any(RawBsonDocumentCodec.class), eq(operationContext),
                eq(true), eq(MessageSequences.EmptyMessageSequences.INSTANCE)))
                .thenReturn(encryptedResponse);

        when(crypt.decrypt(encryptedResponse, operationTimeout)).thenReturn(decryptedResponse);

        Document response = cryptConnection.command("db",
                new BsonDocumentWrapper<>(new Document("find", "test")
                        .append("filter", new Document("ssid", "555-55-5555")), codec),
                NoOpFieldNameValidator.INSTANCE, ReadPreference.primary(), codec, operationContext);

        assertEquals(rawToDocument(decryptedResponse), response);
    }

    @Test
    @DisplayName("should split at 2 MiB")
    void shouldSplitAt2MiB() {
        Connection wrappedConnection = mock(Connection.class);
        Crypt crypt = mock(Crypt.class);
        CryptConnection cryptConnection = new CryptConnection(wrappedConnection, crypt);
        DocumentCodec codec = new DocumentCodec();
        byte[] bytes = new byte[2097152 - 85];
        List<WriteRequestWithIndex> requests = new ArrayList<>();
        requests.add(new WriteRequestWithIndex(new InsertRequest(
                new BsonDocumentWrapper<>(new Document("_id", 1).append("ssid", "555-55-5555").append("b", bytes), codec)), 0));
        requests.add(new WriteRequestWithIndex(new InsertRequest(
                new BsonDocumentWrapper<>(new Document("_id", 2).append("ssid", "666-66-6666").append("b", bytes), codec)), 1));
        SplittablePayload payload = new SplittablePayload(INSERT, requests, true, NoOpFieldNameValidator.INSTANCE);

        RawBsonDocument encryptedCommand = toRaw(new BsonDocument("insert", new BsonString("test")).append("documents", new BsonArray(
                Arrays.asList(
                        new BsonDocument("_id", new BsonInt32(1))
                                .append("ssid", new BsonBinary((byte) 6, new byte[10]))
                                .append("b", new BsonBinary(bytes))))));

        RawBsonDocument encryptedResponse = toRaw(new BsonDocument("ok", new BsonInt32(1)));
        RawBsonDocument decryptedResponse = encryptedResponse;
        TimeoutContext timeoutContext = mock(TimeoutContext.class);
        OperationContext operationContext = ClusterFixture.OPERATION_CONTEXT.withTimeoutContext(timeoutContext);
        Timeout operationTimeout = mock(Timeout.class);
        when(timeoutContext.getTimeout()).thenReturn(operationTimeout);

        when(wrappedConnection.getDescription()).thenReturn(
                new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())), 8, STANDALONE,
                        1000, 1024 * 16_000, 1024 * 48_000, Collections.emptyList()));

        when(crypt.encrypt(eq("db"),
                eq(toRaw(new BsonDocument("insert", new BsonString("test")).append("documents",
                        new BsonArray(Arrays.asList(
                                new BsonDocument("_id", new BsonInt32(1))
                                        .append("ssid", new BsonString("555-55-5555"))
                                        .append("b", new BsonBinary(bytes))))))), eq(operationTimeout)))
                .thenReturn(encryptedCommand);

        when(wrappedConnection.command(eq("db"), eq(encryptedCommand), any(NoOpFieldNameValidator.class),
                eq(ReadPreference.primary()), any(RawBsonDocumentCodec.class), eq(operationContext),
                eq(true), eq(MessageSequences.EmptyMessageSequences.INSTANCE)))
                .thenReturn(encryptedResponse);

        when(crypt.decrypt(encryptedResponse, operationTimeout)).thenReturn(decryptedResponse);

        BsonDocument response = cryptConnection.command("db",
                new BsonDocumentWrapper<>(new Document("insert", "test"), codec),
                NoOpFieldNameValidator.INSTANCE, ReadPreference.primary(), new BsonDocumentCodec(), operationContext, true, payload);

        assertEquals(rawToBsonDocument(decryptedResponse), response);
        assertEquals(1, payload.getPosition());
    }

    @Test
    @DisplayName("should split at maxBatchCount")
    void shouldSplitAtMaxBatchCount() {
        Connection wrappedConnection = mock(Connection.class);
        Crypt crypt = mock(Crypt.class);
        CryptConnection cryptConnection = new CryptConnection(wrappedConnection, crypt);
        DocumentCodec codec = new DocumentCodec();
        int maxBatchCount = 2;
        List<WriteRequestWithIndex> requests = new ArrayList<>();
        requests.add(new WriteRequestWithIndex(new InsertRequest(
                new BsonDocumentWrapper<>(new Document("_id", 1), codec)), 0));
        requests.add(new WriteRequestWithIndex(new InsertRequest(
                new BsonDocumentWrapper<>(new Document("_id", 2), codec)), 1));
        requests.add(new WriteRequestWithIndex(new InsertRequest(
                new BsonDocumentWrapper<>(new Document("_id", 3), codec)), 2));
        SplittablePayload payload = new SplittablePayload(INSERT, requests, true, NoOpFieldNameValidator.INSTANCE);

        RawBsonDocument encryptedCommand = toRaw(new BsonDocument("insert", new BsonString("test")).append("documents", new BsonArray(
                Arrays.asList(
                        new BsonDocument("_id", new BsonInt32(1)),
                        new BsonDocument("_id", new BsonInt32(2))))));

        RawBsonDocument encryptedResponse = toRaw(new BsonDocument("ok", new BsonInt32(1)));
        RawBsonDocument decryptedResponse = encryptedResponse;
        TimeoutContext timeoutContext = mock(TimeoutContext.class);
        OperationContext operationContext = ClusterFixture.OPERATION_CONTEXT.withTimeoutContext(timeoutContext);
        Timeout operationTimeout = mock(Timeout.class);
        when(timeoutContext.getTimeout()).thenReturn(operationTimeout);

        when(wrappedConnection.getDescription()).thenReturn(
                new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())), 8, STANDALONE,
                        maxBatchCount, 1024 * 16_000, 1024 * 48_000, Collections.emptyList()));

        when(crypt.encrypt(eq("db"),
                eq(toRaw(new BsonDocument("insert", new BsonString("test")).append("documents",
                        new BsonArray(Arrays.asList(
                                new BsonDocument("_id", new BsonInt32(1)),
                                new BsonDocument("_id", new BsonInt32(2))))))), eq(operationTimeout)))
                .thenReturn(encryptedCommand);

        when(wrappedConnection.command(eq("db"), eq(encryptedCommand), any(NoOpFieldNameValidator.class),
                eq(ReadPreference.primary()), any(RawBsonDocumentCodec.class), eq(operationContext),
                eq(true), eq(MessageSequences.EmptyMessageSequences.INSTANCE)))
                .thenReturn(encryptedResponse);

        when(crypt.decrypt(encryptedResponse, operationTimeout)).thenReturn(decryptedResponse);

        BsonDocument response = cryptConnection.command("db",
                new BsonDocumentWrapper<>(new Document("insert", "test"), codec),
                NoOpFieldNameValidator.INSTANCE, ReadPreference.primary(), new BsonDocumentCodec(), operationContext, true, payload);

        assertEquals(rawToBsonDocument(decryptedResponse), response);
        assertEquals(2, payload.getPosition());
    }

    private static RawBsonDocument toRaw(final BsonDocument document) {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(buffer);
        new BsonDocumentCodec().encode(writer, document, EncoderContext.builder().build());
        return new RawBsonDocument(buffer.getInternalBuffer(), 0, buffer.getSize());
    }

    private static Document rawToDocument(final RawBsonDocument document) {
        return new DocumentCodec().decode(new BsonBinaryReader(document.getByteBuffer().asNIO()), DecoderContext.builder().build());
    }

    private static BsonDocument rawToBsonDocument(final RawBsonDocument document) {
        return new BsonDocumentCodec().decode(new BsonBinaryReader(document.getByteBuffer().asNIO()), DecoderContext.builder().build());
    }
}
