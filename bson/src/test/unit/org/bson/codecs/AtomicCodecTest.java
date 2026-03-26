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

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AtomicCodecTest {

    @Test
    void shouldEncodeAndDecodeAtomicBoolean() {
        AtomicBooleanCodec codec = new AtomicBooleanCodec();
        AtomicBoolean atomicBoolean = new AtomicBoolean(true);
        BsonDocument document = new BsonDocument();

        BsonDocumentWriter writer = new BsonDocumentWriter(document);
        writer.writeStartDocument();
        writer.writeName("b");
        codec.encode(writer, atomicBoolean, EncoderContext.builder().build());
        writer.writeEndDocument();

        assertEquals(new BsonDocument("b", BsonBoolean.TRUE), document);

        BsonDocumentReader reader = new BsonDocumentReader(document);
        reader.readStartDocument();
        reader.readName("b");
        AtomicBoolean value = codec.decode(reader, DecoderContext.builder().build());

        assertEquals(atomicBoolean.get(), value.get());
    }

    @Test
    void shouldEncodeAndDecodeAtomicInteger() {
        AtomicIntegerCodec codec = new AtomicIntegerCodec();
        AtomicInteger atomicInteger = new AtomicInteger(1);
        BsonDocument document = new BsonDocument();

        BsonDocumentWriter writer = new BsonDocumentWriter(document);
        writer.writeStartDocument();
        writer.writeName("i");
        codec.encode(writer, atomicInteger, EncoderContext.builder().build());
        writer.writeEndDocument();

        assertEquals(new BsonDocument("i", new BsonInt32(1)), document);

        BsonDocumentReader reader = new BsonDocumentReader(document);
        reader.readStartDocument();
        reader.readName("i");
        AtomicInteger value = codec.decode(reader, DecoderContext.builder().build());

        assertEquals(atomicInteger.get(), value.get());
    }

    @Test
    void shouldEncodeAndDecodeAtomicLong() {
        AtomicLongCodec codec = new AtomicLongCodec();
        AtomicLong atomicLong = new AtomicLong(1L);
        BsonDocument document = new BsonDocument();

        BsonDocumentWriter writer = new BsonDocumentWriter(document);
        writer.writeStartDocument();
        writer.writeName("l");
        codec.encode(writer, atomicLong, EncoderContext.builder().build());
        writer.writeEndDocument();

        assertEquals(new BsonDocument("l", new BsonInt64(1L)), document);

        BsonDocumentReader reader = new BsonDocumentReader(document);
        reader.readStartDocument();
        reader.readName("l");
        AtomicLong value = codec.decode(reader, DecoderContext.builder().build());

        assertEquals(atomicLong.get(), value.get());
    }
}
