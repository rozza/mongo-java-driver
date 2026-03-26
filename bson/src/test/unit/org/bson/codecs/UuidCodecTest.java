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
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.ByteBufNIO;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UuidCodecTest {

    private BasicOutputBuffer outputBuffer;

    @BeforeEach
    void setUp() {
        outputBuffer = new BasicOutputBuffer();
    }

    @Test
    void shouldDefaultToUnspecifiedRepresentation() {
        assertEquals(UuidRepresentation.UNSPECIFIED, new UuidCodec().getUuidRepresentation());
    }

    static Stream<Arguments> decodeDifferentTypesOfUuid() {
        return Stream.of(
                Arguments.of(new UuidCodec(UuidRepresentation.JAVA_LEGACY),
                        new byte[]{0, 0, 0, 0,
                                5, 95, 105, 100, 0, 16, 0, 0, 0,
                                3,
                                1, 2, 3, 4, 5, 6, 7, 8,
                                9, 10, 11, 12, 13, 14, 15, 16}),
                Arguments.of(new UuidCodec(UuidRepresentation.STANDARD),
                        new byte[]{0, 0, 0, 0,
                                5, 95, 105, 100, 0, 16, 0, 0, 0,
                                4,
                                8, 7, 6, 5, 4, 3, 2, 1,
                                16, 15, 14, 13, 12, 11, 10, 9}),
                Arguments.of(new UuidCodec(UuidRepresentation.PYTHON_LEGACY),
                        new byte[]{0, 0, 0, 0,
                                5, 95, 105, 100, 0, 16, 0, 0, 0,
                                3,
                                8, 7, 6, 5, 4, 3, 2, 1,
                                16, 15, 14, 13, 12, 11, 10, 9}),
                Arguments.of(new UuidCodec(UuidRepresentation.C_SHARP_LEGACY),
                        new byte[]{0, 0, 0, 0,
                                5, 95, 105, 100, 0, 16, 0, 0, 0,
                                3,
                                5, 6, 7, 8, 3, 4, 1, 2,
                                16, 15, 14, 13, 12, 11, 10, 9})
        );
    }

    @ParameterizedTest
    @MethodSource("decodeDifferentTypesOfUuid")
    void shouldDecodeDifferentTypesOfUuid(UuidCodec codec, byte[] list) {
        ByteBufferBsonInput inputBuffer = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(list)));
        BsonBinaryReader bsonReader = new BsonBinaryReader(inputBuffer);
        UUID expectedUuid = UUID.fromString("08070605-0403-0201-100f-0e0d0c0b0a09");

        bsonReader.readStartDocument();
        bsonReader.readName();

        UUID actualUuid = codec.decode(bsonReader, DecoderContext.builder().build());

        assertEquals(expectedUuid, actualUuid);
        bsonReader.close();
    }

    static Stream<Arguments> encodeDifferentTypesOfUuid() {
        return Stream.of(
                Arguments.of((byte) 3,
                        new UuidCodec(UuidRepresentation.JAVA_LEGACY),
                        UUID.fromString("08070605-0403-0201-100f-0e0d0c0b0a09")),
                Arguments.of((byte) 4,
                        new UuidCodec(UuidRepresentation.STANDARD),
                        UUID.fromString("01020304-0506-0708-090a-0b0c0d0e0f10")),
                Arguments.of((byte) 3,
                        new UuidCodec(UuidRepresentation.PYTHON_LEGACY),
                        UUID.fromString("01020304-0506-0708-090a-0b0c0d0e0f10")),
                Arguments.of((byte) 3,
                        new UuidCodec(UuidRepresentation.C_SHARP_LEGACY),
                        UUID.fromString("04030201-0605-0807-090a-0b0c0d0e0f10"))
        );
    }

    @ParameterizedTest
    @MethodSource("encodeDifferentTypesOfUuid")
    void shouldEncodeDifferentTypesOfUuids(byte bsonSubType, UuidCodec codec, UUID uuid) {
        byte[] encodedDoc = new byte[]{0, 0, 0, 0,
                5, 95, 105, 100, 0, 16, 0, 0, 0,
                0,
                1, 2, 3, 4, 5, 6, 7, 8,
                9, 10, 11, 12, 13, 14, 15, 16};
        encodedDoc[13] = bsonSubType;

        BsonBinaryWriter bsonWriter = new BsonBinaryWriter(outputBuffer);
        bsonWriter.writeStartDocument();
        bsonWriter.writeName("_id");

        codec.encode(bsonWriter, uuid, EncoderContext.builder().build());

        assertArrayEquals(encodedDoc, outputBuffer.toByteArray());
        bsonWriter.close();
    }

    @Test
    void shouldThrowIfRepresentationIsUnspecified() {
        UuidCodec codec = new UuidCodec(UuidRepresentation.UNSPECIFIED);
        assertThrows(CodecConfigurationException.class, () ->
                codec.encode(new BsonDocumentWriter(new BsonDocument()), UUID.randomUUID(),
                        EncoderContext.builder().build()));
    }
}
