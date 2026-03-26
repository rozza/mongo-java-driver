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

package org.bson.codecs;

import org.bson.BsonBinaryReader;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonElement;
import org.bson.ByteBufNIO;
import org.bson.RawBsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RawBsonDocumentCodecTest {

    private final RawBsonDocumentCodec codec = new RawBsonDocumentCodec();
    private final byte[] documentBytes = new byte[]{15, 0, 0, 0, 8, 98, 49, 0, 1, 8, 98, 50, 0, 0, 0};

    @Test
    void shouldGetEncoderClass() {
        assertEquals(RawBsonDocument.class, codec.getEncoderClass());
    }

    @Test
    void shouldEncode() {
        BsonDocument document = new BsonDocument();
        BsonDocumentWriter writer = new BsonDocumentWriter(document);

        codec.encode(writer, new RawBsonDocument(documentBytes), EncoderContext.builder().build());

        assertEquals(new BsonDocument(Arrays.asList(
                new BsonElement("b1", BsonBoolean.TRUE),
                new BsonElement("b2", BsonBoolean.FALSE))), document);
    }

    @Test
    void shouldDecode() {
        BsonBinaryReader reader = new BsonBinaryReader(
                new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(documentBytes))));

        RawBsonDocument buffer = codec.decode(reader, DecoderContext.builder().build());
        byte[] bytes = new byte[buffer.getByteBuffer().remaining()];
        buffer.getByteBuffer().get(bytes);

        assertArrayEquals(documentBytes, bytes);
    }
}
