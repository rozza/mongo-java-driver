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

import com.mongodb.ClusterFixture;
import com.mongodb.ReadPreference;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.io.BasicOutputBuffer;
import org.bson.json.JsonReader;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.internal.connection.MessageHelper.LEGACY_HELLO;

class StreamHelper {

    private static int nextMessageId = 900000;
    private static final String DEFAULT_JSON_RESPONSE =
            "{connectionId: 1,   n: 0,   syncMillis: 0,   writtenTo:  null,  err: null,   ok: 1 }";

    private static ByteBuf defaultHeader(int messageId) {
        return header(messageId, DEFAULT_JSON_RESPONSE);
    }

    static ByteBuf defaultMessageHeader(int messageId) {
        return messageHeader(messageId, DEFAULT_JSON_RESPONSE);
    }

    static ByteBuf defaultReply() {
        ByteBuf header = replyHeader();
        ByteBuf body = defaultBody();
        ByteBuffer reply = ByteBuffer.allocate(header.remaining() + body.remaining());
        append(reply, header);
        append(reply, body);
        reply.flip();
        return new ByteBufNIO(reply);
    }

    private static void append(ByteBuffer to, ByteBuf from) {
        byte[] bytes = new byte[from.remaining()];
        from.get(bytes);
        to.put(bytes);
    }

    private static ByteBuf defaultReplyHeader() {
        return replyHeader();
    }

    static ByteBuf messageHeader(int messageId, String json) {
        ByteBuffer headerByteBuffer = ByteBuffer.allocate(16);
        headerByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        headerByteBuffer.putInt(36 + body(json).remaining()); // messageLength
        headerByteBuffer.putInt(4);                     // requestId
        headerByteBuffer.putInt(messageId);             // responseTo
        headerByteBuffer.putInt(1);                     // opCode
        headerByteBuffer.flip();
        return new ByteBufNIO(headerByteBuffer);
    }

    private static ByteBuf replyHeader() {
        ByteBuffer headerByteBuffer = ByteBuffer.allocate(20);
        headerByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        headerByteBuffer.putInt(0);   // responseFlags
        headerByteBuffer.putLong(0);  // cursorId
        headerByteBuffer.putInt(0);   // starting from
        headerByteBuffer.putInt(1);   // number returned
        headerByteBuffer.flip();
        return new ByteBufNIO(headerByteBuffer);
    }

    private static ByteBuf header(int messageId, String json) {
        ByteBuffer headerByteBuffer = ByteBuffer.allocate(36);
        headerByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        headerByteBuffer.putInt(36 + body(json).remaining()); // messageLength
        headerByteBuffer.putInt(4);                     // requestId
        headerByteBuffer.putInt(messageId);             // responseTo
        headerByteBuffer.putInt(1);                     // opCode
        headerByteBuffer.putInt(0);                     // responseFlags
        headerByteBuffer.putLong(0);                    // cursorId
        headerByteBuffer.putInt(0);                     // starting from
        headerByteBuffer.putInt(1);                     // number returned
        headerByteBuffer.flip();
        return new ByteBufNIO(headerByteBuffer);
    }

    static ByteBuf headerWithMessageSizeGreaterThanMax(int messageId, int maxMessageSize) {
        ByteBuffer headerByteBuffer = ByteBuffer.allocate(36);
        headerByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        headerByteBuffer.putInt(maxMessageSize + 1);  // messageLength
        headerByteBuffer.putInt(4);             // requestId
        headerByteBuffer.putInt(messageId);     // responseTo
        headerByteBuffer.putInt(1);             // opCode
        headerByteBuffer.putInt(0);             // responseFlags
        headerByteBuffer.putLong(0);            // cursorId
        headerByteBuffer.putInt(0);             // starting from
        headerByteBuffer.putInt(1);             // number returned
        headerByteBuffer.flip();
        return new ByteBufNIO(headerByteBuffer);
    }

    static ByteBuf headerWithMessageSizeGreaterThanMax(int messageId) {
        return headerWithMessageSizeGreaterThanMax(messageId, 36);
    }

    static ByteBuf defaultBody() {
        return body(DEFAULT_JSON_RESPONSE);
    }

    static ByteBuf reply(String json) {
        ByteBuf replyHeader = defaultReplyHeader();
        BsonReader reader = new JsonReader(json);
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonWriter writer = new BsonBinaryWriter(outputBuffer);
        writer.pipe(reader);

        ByteBuffer buffer = ByteBuffer.allocate(replyHeader.remaining() + outputBuffer.getSize());
        append(buffer, replyHeader);
        buffer.put(outputBuffer.toByteArray());
        buffer.flip();
        return new ByteBufNIO(buffer);
    }

    private static ByteBuf body(String json) {
        BsonReader reader = new JsonReader(json);
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonWriter writer = new BsonBinaryWriter(outputBuffer);
        writer.pipe(reader);
        ByteBuffer buf = ByteBuffer.allocate(outputBuffer.getSize());
        buf.put(outputBuffer.toByteArray());
        buf.flip();
        return new ByteBufNIO(buf);
    }

    static List<ByteBuf> generateHeaders(List<Integer> messageIds) {
        List<ByteBuf> headers = new ArrayList<>();
        for (int id : messageIds) {
            headers.add(defaultHeader(id));
        }
        return headers;
    }

    /**
     * Returns [List of ByteBuf buffers, int messageId].
     */
    static Object[] hello() {
        CommandMessage command = new CommandMessage("admin",
                new BsonDocument(LEGACY_HELLO, new BsonInt32(1)), NoOpFieldNameValidator.INSTANCE,
                ReadPreference.primary(), MessageSettings.builder().build(), SINGLE, null);
        ByteBufferBsonOutput outputBuffer = new ByteBufferBsonOutput(new SimpleBufferProvider());
        try {
            command.encode(outputBuffer, new OperationContext(
                    IgnorableRequestContext.INSTANCE,
                    NoOpSessionContext.INSTANCE,
                    new TimeoutContext(ClusterFixture.TIMEOUT_SETTINGS), null));
            nextMessageId++;
            return new Object[]{outputBuffer.getByteBuffers(), nextMessageId};
        } finally {
            outputBuffer.close();
        }
    }

    /**
     * Returns [List of ByteBuf buffers, int messageId, FutureResultCallback sendCallback, FutureResultCallback receiveCallback].
     */
    static Object[] helloAsync() {
        Object[] helloResult = hello();
        return new Object[]{
                helloResult[0],
                helloResult[1],
                new FutureResultCallback<Void>(),
                new FutureResultCallback<ResponseBuffers>()
        };
    }
}
