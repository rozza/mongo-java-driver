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

import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.UUID;
import java.util.stream.Stream;

import static org.bson.BasicBSONDecoder.getDefaultUuidRepresentation;
import static org.bson.BasicBSONDecoder.setDefaultUuidRepresentation;
import static org.bson.BsonBinarySubType.UUID_LEGACY;
import static org.bson.BsonBinarySubType.UUID_STANDARD;
import static org.bson.UuidRepresentation.JAVA_LEGACY;
import static org.bson.UuidRepresentation.STANDARD;
import static org.bson.internal.UuidHelper.encodeUuidToBinary;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BasicBSONDecoderTest {

    private final BasicBSONDecoder bsonDecoder = new BasicBSONDecoder();

    @Test
    @DisplayName("should decode from input stream")
    void shouldDecodeFromInputStream() throws java.io.IOException {
        byte[] bytes = {12, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0};
        InputStream is = new ByteArrayInputStream(bytes);

        BSONObject document = bsonDecoder.readObject(is);

        assertEquals(new BasicBSONObject("a", 1), document);
    }

    @ParameterizedTest
    @MethodSource("decodeTypeArgs")
    @DisplayName("should decode types")
    void shouldDecodeType(final BasicBSONObject expected, final byte[] bytes) {
        assertEquals(expected, bsonDecoder.readObject(bytes));
    }

    private static Stream<Arguments> decodeTypeArgs() {
        return Stream.of(
                Arguments.of(new BasicBSONObject("d1", -1.01), new byte[]{17, 0, 0, 0, 1, 100, 49, 0, 41, 92, -113, -62, -11, 40, -16, -65, 0}),
                Arguments.of(new BasicBSONObject("d2", (double) Float.MIN_VALUE), new byte[]{17, 0, 0, 0, 1, 100, 50, 0, 0, 0, 0, 0, 0, 0, -96, 54, 0}),
                Arguments.of(new BasicBSONObject("d3", Double.MAX_VALUE), new byte[]{17, 0, 0, 0, 1, 100, 51, 0, -1, -1, -1, -1, -1, -1, -17, 127, 0}),
                Arguments.of(new BasicBSONObject("d4", 0.0), new byte[]{17, 0, 0, 0, 1, 100, 52, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("s1", ""), new byte[]{14, 0, 0, 0, 2, 115, 49, 0, 1, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("s2", "danke"), new byte[]{19, 0, 0, 0, 2, 115, 50, 0, 6, 0, 0, 0, 100, 97, 110, 107, 101, 0, 0}),
                Arguments.of(new BasicBSONObject("s3", ",+\\\"<>;[]{}@#$%^&*()+_"), new byte[]{36, 0, 0, 0, 2, 115, 51, 0, 23, 0, 0, 0, 44, 43, 92, 34, 60, 62, 59, 91, 93, 123, 125, 64, 35, 36, 37, 94, 38, 42, 40, 41, 43, 95, 0, 0}),
                Arguments.of(new BasicBSONObject("s4", "a\u00e9\u3042\u0430\u0432\u0431\u0434"), new byte[]{28, 0, 0, 0, 2, 115, 52, 0, 15, 0, 0, 0, 97, -61, -87, -29, -127, -126, -48, -80, -48, -78, -48, -79, -48, -76, 0, 0}),
                Arguments.of(new BasicBSONObject("o", new BasicBSONObject()), new byte[]{13, 0, 0, 0, 3, 111, 0, 5, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("a1", new java.util.ArrayList<>()), new byte[]{14, 0, 0, 0, 4, 97, 49, 0, 5, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("b1", new Binary((byte) 0x01, new byte[]{115, 116, 11})), new byte[]{17, 0, 0, 0, 5, 98, 49, 0, 3, 0, 0, 0, 1, 115, 116, 11, 0}),
                Arguments.of(new BasicBSONObject("b2", new byte[]{102, 111, 111}), new byte[]{17, 0, 0, 0, 5, 98, 50, 0, 3, 0, 0, 0, 0, 102, 111, 111, 0}),
                Arguments.of(new BasicBSONObject("_id", new ObjectId("50d3332018c6a1d8d1662b61")), new byte[]{22, 0, 0, 0, 7, 95, 105, 100, 0, 80, -45, 51, 32, 24, -58, -95, -40, -47, 102, 43, 97, 0}),
                Arguments.of(new BasicBSONObject("b1", true), new byte[]{10, 0, 0, 0, 8, 98, 49, 0, 1, 0}),
                Arguments.of(new BasicBSONObject("b2", false), new byte[]{10, 0, 0, 0, 8, 98, 50, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("d", new java.util.Date(582163200)), new byte[]{16, 0, 0, 0, 9, 100, 0, 0, 27, -77, 34, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("n", null), new byte[]{8, 0, 0, 0, 10, 110, 0, 0}),
                Arguments.of(new BasicBSONObject("js1", new Code("var i = 0")), new byte[]{24, 0, 0, 0, 13, 106, 115, 49, 0, 10, 0, 0, 0, 118, 97, 114, 32, 105, 32, 61, 32, 48, 0, 0}),
                Arguments.of(new BasicBSONObject("s", "c"), new byte[]{14, 0, 0, 0, 14, 115, 0, 2, 0, 0, 0, 99, 0, 0}),
                Arguments.of(new BasicBSONObject("js2", new CodeWScope("i++", new BasicBSONObject("x", 1))), new byte[]{34, 0, 0, 0, 15, 106, 115, 50, 0, 24, 0, 0, 0, 4, 0, 0, 0, 105, 43, 43, 0, 12, 0, 0, 0, 16, 120, 0, 1, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("i1", -12), new byte[]{13, 0, 0, 0, 16, 105, 49, 0, -12, -1, -1, -1, 0}),
                Arguments.of(new BasicBSONObject("i2", Integer.MIN_VALUE), new byte[]{13, 0, 0, 0, 16, 105, 50, 0, 0, 0, 0, -128, 0}),
                Arguments.of(new BasicBSONObject("i3", 0), new byte[]{13, 0, 0, 0, 16, 105, 51, 0, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("t", new BSONTimestamp(123999401, 44332)), new byte[]{16, 0, 0, 0, 17, 116, 0, 44, -83, 0, 0, -87, 20, 100, 7, 0}),
                Arguments.of(new BasicBSONObject("i4", Long.MAX_VALUE), new byte[]{17, 0, 0, 0, 18, 105, 52, 0, -1, -1, -1, -1, -1, -1, -1, 127, 0}),
                Arguments.of(new BasicBSONObject("k1", new MinKey()), new byte[]{9, 0, 0, 0, -1, 107, 49, 0, 0}),
                Arguments.of(new BasicBSONObject("k2", new MaxKey()), new byte[]{9, 0, 0, 0, 127, 107, 50, 0, 0}),
                Arguments.of(new BasicBSONObject("f", Decimal128.parse("0E-6176")), new byte[]{24, 0, 0, 0, 19, 102, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("u", new UUID(1, 2)), new byte[]{29, 0, 0, 0, 5, 117, 0, 16, 0, 0, 0, 3, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0})
        );
    }

    @ParameterizedTest
    @MethodSource("decodeComplexArgs")
    @DisplayName("should decode complex structures")
    void shouldDecodeComplexStructures(final BasicBSONObject expected, final byte[] bytes) {
        assertEquals(expected, bsonDecoder.readObject(bytes));
    }

    private static Stream<Arguments> decodeComplexArgs() {
        return Stream.of(
                Arguments.of(
                        new BasicBSONObject("a", new BasicBSONObject("d1", new BasicBSONObject("b", true)).append("d2", new BasicBSONObject("b", false))),
                        new byte[]{39, 0, 0, 0, 3, 97, 0, 31, 0, 0, 0, 3, 100, 49, 0, 9, 0, 0, 0, 8, 98, 0, 1, 0, 3, 100, 50, 0, 9, 0, 0, 0, 8, 98, 0, 0, 0, 0, 0}),
                Arguments.of(
                        new BasicBSONObject("js", new CodeWScope("i++", new BasicBSONObject("njs", new CodeWScope("j++", new BasicBSONObject("j", 0))))),
                        new byte[]{55, 0, 0, 0, 15, 106, 115, 0, 46, 0, 0, 0, 4, 0, 0, 0, 105, 43, 43, 0, 34, 0, 0, 0, 15, 110, 106, 115, 0, 24, 0, 0, 0, 4, 0, 0, 0, 106, 43, 43, 0, 12, 0, 0, 0, 16, 106, 0, 0, 0, 0, 0, 0, 0, 0})
        );
    }

    @ParameterizedTest
    @MethodSource("invalidInputArgs")
    @DisplayName("should throw exception when input is invalid")
    void shouldThrowExceptionWhenInputIsInvalid(final byte[] bytes) {
        assertThrows(BsonSerializationException.class, () -> bsonDecoder.readObject(bytes));
    }

    private static Stream<Arguments> invalidInputArgs() {
        return Stream.of(
                Arguments.of((Object) new byte[]{13, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0}),
                Arguments.of((Object) new byte[]{12, 0, 0, 0, 17, 97, 0, 1, 0, 0, 0, 0}),
                Arguments.of((Object) new byte[]{12, 0, 2, 0, 16, 97, 0, 1, 0, 0, 0, 0}),
                Arguments.of((Object) new byte[]{5, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0}),
                Arguments.of((Object) new byte[]{5, 0, 0, 0, 16, 97, 45, 1, 0, 0, 0, 0})
        );
    }

    @Test
    @DisplayName("default value of defaultUuidRepresentation is JAVA_LEGACY")
    void defaultUuidRepresentationIsJavaLegacy() {
        assertEquals(JAVA_LEGACY, getDefaultUuidRepresentation());
    }

    @ParameterizedTest
    @MethodSource("uuidDecodingArgs")
    @DisplayName("should decode UUID according to default uuid representation")
    void shouldDecodeUuidAccordingToDefaultRepresentation(
            final UuidRepresentation encodedUuidRepresentation,
            final UuidRepresentation decodedUuidRepresentation,
            final Object expectedUuid) {
        UUID uuid = new UUID(1, 2);
        BasicOutputBuffer output = new BasicOutputBuffer();
        new BsonDocumentCodec().encode(new BsonBinaryWriter(output),
                new BsonDocument("u", new BsonBinary(uuid, encodedUuidRepresentation)), EncoderContext.builder().build());

        try {
            setDefaultUuidRepresentation(decodedUuidRepresentation);
            assertEquals(decodedUuidRepresentation, getDefaultUuidRepresentation());

            Object decodedUuid = bsonDecoder.readObject(output.getInternalBuffer()).get("u");
            assertEquals(expectedUuid, decodedUuid);
        } finally {
            setDefaultUuidRepresentation(JAVA_LEGACY);
        }
    }

    private static Stream<Arguments> uuidDecodingArgs() {
        return Stream.of(
                Arguments.of(JAVA_LEGACY, JAVA_LEGACY, new UUID(1, 2)),
                Arguments.of(JAVA_LEGACY, STANDARD, new Binary(UUID_LEGACY, encodeUuidToBinary(new UUID(1, 2), JAVA_LEGACY))),
                Arguments.of(STANDARD, JAVA_LEGACY, new Binary(UUID_STANDARD, encodeUuidToBinary(new UUID(1, 2), STANDARD))),
                Arguments.of(STANDARD, STANDARD, new UUID(1, 2))
        );
    }
}
