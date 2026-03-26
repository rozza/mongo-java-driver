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

package org.bson;

import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;
import org.bson.json.JsonMode;
import org.bson.json.JsonReader;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LimitedLookaheadMarkTest {

    private static void writeTestDocument(final BsonWriter writer) {
        writer.writeStartDocument();
        writer.writeInt64("int64", 52L);
        writer.writeStartArray("array");
        writer.writeInt32(1);
        writer.writeInt64(2L);
        writer.writeStartArray();
        writer.writeInt32(3);
        writer.writeInt32(4);
        writer.writeEndArray();
        writer.writeStartDocument();
        writer.writeInt32("a", 5);
        writer.writeEndDocument();
        writer.writeNull();
        writer.writeEndArray();
        writer.writeStartDocument("document");
        writer.writeInt32("a", 6);
        writer.writeEndDocument();
        writer.writeEndDocument();
    }

    private static BsonReader createReader(final BsonWriter writer, final boolean useAlternateReader) {
        if (writer instanceof BsonDocumentWriter) {
            return new BsonDocumentReader(((BsonDocumentWriter) writer).getDocument());
        } else if (writer instanceof BsonBinaryWriter) {
            BasicOutputBuffer buffer = (BasicOutputBuffer) ((BsonBinaryWriter) writer).getBsonOutput();
            return new BsonBinaryReader(new ByteBufferBsonInput(buffer.getByteBuffers().get(0)));
        } else if (writer instanceof JsonWriter) {
            String json = ((JsonWriter) writer).getWriter().toString();
            if (useAlternateReader) {
                return new JsonReader(new InputStreamReader(new ByteArrayInputStream(json.getBytes())));
            } else {
                return new JsonReader(json);
            }
        }
        throw new IllegalArgumentException("Unsupported writer type");
    }

    @ParameterizedTest
    @MethodSource("writerArgs")
    @DisplayName("Lookahead should work at various states with Mark")
    void lookaheadShouldWorkAtVariousStatesWithMark(final BsonWriter writer, final boolean useAlternateReader) {
        writeTestDocument(writer);
        BsonReader reader = createReader(writer, useAlternateReader);

        reader.readStartDocument();
        // mark beginning of document * 1
        BsonReaderMark mark = reader.getMark();

        assertEquals("int64", reader.readName());
        assertEquals(52L, reader.readInt64());
        reader.readStartArray();

        // reset to beginning of document * 2
        mark.reset();
        // mark beginning of document * 2
        mark = reader.getMark();

        assertEquals("int64", reader.readName());
        assertEquals(52L, reader.readInt64());

        // make sure it's possible to reset to a mark after getting a new mark
        reader.getMark();
        // reset to beginning of document * 3
        mark.reset();
        // mark beginning of document * 3
        mark = reader.getMark();

        assertEquals("int64", reader.readName());
        assertEquals(52L, reader.readInt64());
        assertEquals("array", reader.readName());
        reader.readStartArray();
        assertEquals(1, reader.readInt32());
        assertEquals(2, reader.readInt64());
        reader.readStartArray();
        assertEquals(3, reader.readInt32());
        assertEquals(4, reader.readInt32());
        reader.readEndArray();
        reader.readStartDocument();
        assertEquals("a", reader.readName());
        assertEquals(5, reader.readInt32());
        reader.readEndDocument();
        reader.readNull();
        reader.readEndArray();
        assertEquals("document", reader.readName());
        reader.readStartDocument();
        assertEquals("a", reader.readName());
        assertEquals(6, reader.readInt32());
        reader.readEndDocument();
        reader.readEndDocument();

        // read entire document, reset to beginning
        mark.reset();

        assertEquals("int64", reader.readName());
        assertEquals(52L, reader.readInt64());
        assertEquals("array", reader.readName());

        // mark in outer-document * 1
        mark = reader.getMark();

        reader.readStartArray();
        assertEquals(1, reader.readInt32());
        assertEquals(2, reader.readInt64());
        reader.readStartArray();

        // reset in sub-document * 1
        mark.reset();
        // mark in outer-document * 2
        mark = reader.getMark();

        reader.readStartArray();
        assertEquals(1, reader.readInt32());
        assertEquals(2, reader.readInt64());
        reader.readStartArray();
        assertEquals(3, reader.readInt32());

        // reset in sub-document * 2
        mark.reset();

        reader.readStartArray();
        assertEquals(1, reader.readInt32());
        assertEquals(2, reader.readInt64());
        reader.readStartArray();
        assertEquals(3, reader.readInt32());
        assertEquals(4, reader.readInt32());

        // mark in sub-document * 1
        mark = reader.getMark();

        reader.readEndArray();
        reader.readStartDocument();
        assertEquals("a", reader.readName());
        assertEquals(5, reader.readInt32());
        reader.readEndDocument();
        reader.readNull();
        reader.readEndArray();

        // reset in outer-document * 1
        mark.reset();
        // mark in sub-document * 2
        mark = reader.getMark();

        reader.readEndArray();
        reader.readStartDocument();
        assertEquals("a", reader.readName());
        assertEquals(5, reader.readInt32());
        reader.readEndDocument();
        reader.readNull();
        reader.readEndArray();

        // reset in outer-document * 2
        mark.reset();

        reader.readEndArray();
        reader.readStartDocument();
        assertEquals("a", reader.readName());
        assertEquals(5, reader.readInt32());
        reader.readEndDocument();
        reader.readNull();
        reader.readEndArray();
        assertEquals("document", reader.readName());
        reader.readStartDocument();
        assertEquals("a", reader.readName());
        assertEquals(6, reader.readInt32());
        reader.readEndDocument();
        reader.readEndDocument();
    }

    private static Stream<Arguments> writerArgs() {
        return Stream.of(
                Arguments.of(new BsonDocumentWriter(new BsonDocument()), false),
                Arguments.of(new BsonBinaryWriter(new BasicOutputBuffer()), false),
                Arguments.of(new JsonWriter(new StringWriter(), JsonWriterSettings.builder().outputMode(JsonMode.STRICT).build()), false),
                Arguments.of(new JsonWriter(new StringWriter(), JsonWriterSettings.builder().outputMode(JsonMode.STRICT).build()), true)
        );
    }

    @ParameterizedTest
    @MethodSource("peekWriterArgs")
    @DisplayName("should peek binary subtype and size")
    void shouldPeekBinarySubtypeAndSize(final BsonWriter writer) {
        writer.writeStartDocument();
        writer.writeBinaryData("binary", new BsonBinary(BsonBinarySubType.UUID_LEGACY, new byte[16]));
        writer.writeInt64("int64", 52L);
        writer.writeEndDocument();

        BsonReader reader = createReader(writer, false);

        reader.readStartDocument();
        reader.readName();
        byte subType = reader.peekBinarySubType();
        int size = reader.peekBinarySize();
        BsonBinary binary = reader.readBinaryData();
        long longValue = reader.readInt64("int64");
        reader.readEndDocument();

        assertEquals(BsonBinarySubType.UUID_LEGACY.getValue(), subType);
        assertEquals(16, size);
        assertEquals(new BsonBinary(BsonBinarySubType.UUID_LEGACY, new byte[16]), binary);
        assertEquals(52L, longValue);
    }

    private static Stream<Arguments> peekWriterArgs() {
        return Stream.of(
                Arguments.of(new BsonDocumentWriter(new BsonDocument())),
                Arguments.of(new BsonBinaryWriter(new BasicOutputBuffer())),
                Arguments.of(new JsonWriter(new StringWriter(), JsonWriterSettings.builder().outputMode(JsonMode.STRICT).build()))
        );
    }
}
