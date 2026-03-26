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
import org.bson.codecs.DecoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.BSONTimestamp;
import org.bson.types.BasicBSONList;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.bson.BasicBSONEncoder.getDefaultUuidRepresentation;
import static org.bson.BasicBSONEncoder.setDefaultUuidRepresentation;
import static org.bson.UuidRepresentation.JAVA_LEGACY;
import static org.bson.UuidRepresentation.STANDARD;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BasicBSONEncoderTest {

    private final BSONEncoder bsonEncoder = new BasicBSONEncoder();

    @ParameterizedTest
    @MethodSource("encodeTypeArgs")
    @DisplayName("should encode types")
    void shouldEncodeType(final BasicBSONObject document, final byte[] expectedBytes) {
        assertArrayEquals(expectedBytes, bsonEncoder.encode(document));
    }

    private static Stream<Arguments> encodeTypeArgs() {
        return Stream.of(
                Arguments.of(new BasicBSONObject("d", -1.01d), new byte[]{16, 0, 0, 0, 1, 100, 0, 41, 92, -113, -62, -11, 40, -16, -65, 0}),
                Arguments.of(new BasicBSONObject("d", (double) Float.MIN_VALUE), new byte[]{16, 0, 0, 0, 1, 100, 0, 0, 0, 0, 0, 0, 0, -96, 54, 0}),
                Arguments.of(new BasicBSONObject("d", Double.MAX_VALUE), new byte[]{16, 0, 0, 0, 1, 100, 0, -1, -1, -1, -1, -1, -1, -17, 127, 0}),
                Arguments.of(new BasicBSONObject("d", 0.0d), new byte[]{16, 0, 0, 0, 1, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("s", ""), new byte[]{13, 0, 0, 0, 2, 115, 0, 1, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("s", "danke"), new byte[]{18, 0, 0, 0, 2, 115, 0, 6, 0, 0, 0, 100, 97, 110, 107, 101, 0, 0}),
                Arguments.of(new BasicBSONObject("s", ",+\\\"<>;[]{}@#$%^&*()+_"), new byte[]{35, 0, 0, 0, 2, 115, 0, 23, 0, 0, 0, 44, 43, 92, 34, 60, 62, 59, 91, 93, 123, 125, 64, 35, 36, 37, 94, 38, 42, 40, 41, 43, 95, 0, 0}),
                Arguments.of(new BasicBSONObject("s", "a\u00e9\u3042\u0430\u0432\u0431\u0434"), new byte[]{27, 0, 0, 0, 2, 115, 0, 15, 0, 0, 0, 97, -61, -87, -29, -127, -126, -48, -80, -48, -78, -48, -79, -48, -76, 0, 0}),
                Arguments.of(new BasicBSONObject("o", new BasicBSONObject("a", 1)), new byte[]{20, 0, 0, 0, 3, 111, 0, 12, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("a", new ArrayList<>()), new byte[]{13, 0, 0, 0, 4, 97, 0, 5, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("a", new LinkedHashSet<>()), new byte[]{13, 0, 0, 0, 4, 97, 0, 5, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("a", (Iterable<?>) new ArrayList<>()), new byte[]{13, 0, 0, 0, 4, 97, 0, 5, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("a", new Object[0]), new byte[]{13, 0, 0, 0, 4, 97, 0, 5, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("a", new BasicBSONList()), new byte[]{13, 0, 0, 0, 4, 97, 0, 5, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("a", Collections.singletonList(new ArrayList<>())), new byte[]{21, 0, 0, 0, 4, 97, 0, 13, 0, 0, 0, 4, 48, 0, 5, 0, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("b", new Binary((byte) 0x01, new byte[]{115, 116, 11})), new byte[]{16, 0, 0, 0, 5, 98, 0, 3, 0, 0, 0, 1, 115, 116, 11, 0}),
                Arguments.of(new BasicBSONObject("b", new byte[]{102, 111, 111}), new byte[]{16, 0, 0, 0, 5, 98, 0, 3, 0, 0, 0, 0, 102, 111, 111, 0}),
                Arguments.of(new BasicBSONObject("_id", new ObjectId("50d3332018c6a1d8d1662b61")), new byte[]{22, 0, 0, 0, 7, 95, 105, 100, 0, 80, -45, 51, 32, 24, -58, -95, -40, -47, 102, 43, 97, 0}),
                Arguments.of(new BasicBSONObject("b", true), new byte[]{9, 0, 0, 0, 8, 98, 0, 1, 0}),
                Arguments.of(new BasicBSONObject("b", false), new byte[]{9, 0, 0, 0, 8, 98, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("d", new Date(582163200)), new byte[]{16, 0, 0, 0, 9, 100, 0, 0, 27, -77, 34, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("n", null), new byte[]{8, 0, 0, 0, 10, 110, 0, 0}),
                Arguments.of(new BasicBSONObject("r", Pattern.compile("[a]*", Pattern.CASE_INSENSITIVE)), new byte[]{15, 0, 0, 0, 11, 114, 0, 91, 97, 93, 42, 0, 105, 0, 0}),
                Arguments.of(new BasicBSONObject("js", new Code("var i = 0")), new byte[]{23, 0, 0, 0, 13, 106, 115, 0, 10, 0, 0, 0, 118, 97, 114, 32, 105, 32, 61, 32, 48, 0, 0}),
                Arguments.of(new BasicBSONObject("s", 'c'), new byte[]{14, 0, 0, 0, 2, 115, 0, 2, 0, 0, 0, 99, 0, 0}),
                Arguments.of(new BasicBSONObject("s", new Symbol("c")), new byte[]{14, 0, 0, 0, 14, 115, 0, 2, 0, 0, 0, 99, 0, 0}),
                Arguments.of(new BasicBSONObject("js", new CodeWScope("i++", new BasicBSONObject("x", 1))), new byte[]{33, 0, 0, 0, 15, 106, 115, 0, 24, 0, 0, 0, 4, 0, 0, 0, 105, 43, 43, 0, 12, 0, 0, 0, 16, 120, 0, 1, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("i", -12), new byte[]{12, 0, 0, 0, 16, 105, 0, -12, -1, -1, -1, 0}),
                Arguments.of(new BasicBSONObject("i", Integer.MIN_VALUE), new byte[]{12, 0, 0, 0, 16, 105, 0, 0, 0, 0, -128, 0}),
                Arguments.of(new BasicBSONObject("i", 0), new byte[]{12, 0, 0, 0, 16, 105, 0, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("t", new BSONTimestamp(123999401, 44332)), new byte[]{16, 0, 0, 0, 17, 116, 0, 44, -83, 0, 0, -87, 20, 100, 7, 0}),
                Arguments.of(new BasicBSONObject("i", Long.MAX_VALUE), new byte[]{16, 0, 0, 0, 18, 105, 0, -1, -1, -1, -1, -1, -1, -1, 127, 0}),
                Arguments.of(new BasicBSONObject("k", new MinKey()), new byte[]{8, 0, 0, 0, -1, 107, 0, 0}),
                Arguments.of(new BasicBSONObject("k", new MaxKey()), new byte[]{8, 0, 0, 0, 127, 107, 0, 0}),
                Arguments.of(new BasicBSONObject("f", Decimal128.parse("0E-6176")), new byte[]{24, 0, 0, 0, 19, 102, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("u", new UUID(1, 2)), new byte[]{29, 0, 0, 0, 5, 117, 0, 16, 0, 0, 0, 3, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0})
        );
    }

    @ParameterizedTest
    @MethodSource("encodeArrayArgs")
    @DisplayName("should encode array types")
    void shouldEncodeArrayTypes(final BasicBSONObject document, final byte[] expectedBytes) {
        assertArrayEquals(expectedBytes, bsonEncoder.encode(document));
    }

    private static Stream<Arguments> encodeArrayArgs() {
        return Stream.of(
                Arguments.of(new BasicBSONObject("a", new int[]{1, 2}), new byte[]{27, 0, 0, 0, 4, 97, 0, 19, 0, 0, 0, 16, 48, 0, 1, 0, 0, 0, 16, 49, 0, 2, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("a", new long[]{1, 2}), new byte[]{35, 0, 0, 0, 4, 97, 0, 27, 0, 0, 0, 18, 48, 0, 1, 0, 0, 0, 0, 0, 0, 0, 18, 49, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("a", new float[]{1, 2}), new byte[]{35, 0, 0, 0, 4, 97, 0, 27, 0, 0, 0, 1, 48, 0, 0, 0, 0, 0, 0, 0, -16, 63, 1, 49, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0}),
                Arguments.of(new BasicBSONObject("a", new short[]{1, 2}), new byte[]{27, 0, 0, 0, 4, 97, 0, 19, 0, 0, 0, 16, 48, 0, 1, 0, 0, 0, 16, 49, 0, 2, 0, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("a", new double[]{1, 2}), new byte[]{35, 0, 0, 0, 4, 97, 0, 27, 0, 0, 0, 1, 48, 0, 0, 0, 0, 0, 0, 0, -16, 63, 1, 49, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0}),
                Arguments.of(new BasicBSONObject("a", new boolean[]{true, false}), new byte[]{21, 0, 0, 0, 4, 97, 0, 13, 0, 0, 0, 8, 48, 0, 1, 8, 49, 0, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("a", new String[]{"x", "y"}), new byte[]{31, 0, 0, 0, 4, 97, 0, 23, 0, 0, 0, 2, 48, 0, 2, 0, 0, 0, 120, 0, 2, 49, 0, 2, 0, 0, 0, 121, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("a", new Object[]{1, "y"}), new byte[]{29, 0, 0, 0, 4, 97, 0, 21, 0, 0, 0, 16, 48, 0, 1, 0, 0, 0, 2, 49, 0, 2, 0, 0, 0, 121, 0, 0, 0}),
                Arguments.of(new BasicBSONObject("a", new ObjectId[]{new ObjectId("50d3332018c6a1d8d1662b61")}), new byte[]{28, 0, 0, 0, 4, 97, 0, 20, 0, 0, 0, 7, 48, 0, 80, -45, 51, 32, 24, -58, -95, -40, -47, 102, 43, 97, 0, 0})
        );
    }

    @ParameterizedTest
    @MethodSource("encodeComplexArgs")
    @DisplayName("should encode complex structures")
    void shouldEncodeComplexStructures(final BasicBSONObject document, final byte[] expectedBytes) {
        assertArrayEquals(expectedBytes, bsonEncoder.encode(document));
    }

    private static Stream<Arguments> encodeComplexArgs() {
        return Stream.of(
                Arguments.of(
                        new BasicBSONObject("a", new BasicBSONObject("d1", new BasicBSONObject("b", true)).append("d2", new BasicBSONObject("b", false))),
                        new byte[]{39, 0, 0, 0, 3, 97, 0, 31, 0, 0, 0, 3, 100, 49, 0, 9, 0, 0, 0, 8, 98, 0, 1, 0, 3, 100, 50, 0, 9, 0, 0, 0, 8, 98, 0, 0, 0, 0, 0}),
                Arguments.of(
                        new BasicBSONObject("js", new CodeWScope("i++", new BasicBSONObject("njs", new CodeWScope("j++", new BasicBSONObject("j", 0))))),
                        new byte[]{55, 0, 0, 0, 15, 106, 115, 0, 46, 0, 0, 0, 4, 0, 0, 0, 105, 43, 43, 0, 34, 0, 0, 0, 15, 110, 106, 115, 0, 24, 0, 0, 0, 4, 0, 0, 0, 106, 43, 43, 0, 12, 0, 0, 0, 16, 106, 0, 0, 0, 0, 0, 0, 0, 0})
        );
    }

    @Test
    @DisplayName("should throw IllegalArgumentException while encoding unknown class")
    void shouldThrowForUnknownClass() {
        Object instanceOfCustomClass = new Object() { };
        assertThrows(IllegalArgumentException.class, () -> bsonEncoder.encode(new BasicBSONObject("a", instanceOfCustomClass)));
    }

    @Test
    @DisplayName("should throw IllegalStateException on setting buffer while encoder in use")
    void shouldThrowOnSettingBufferWhileInUse() {
        bsonEncoder.set(new BasicOutputBuffer());
        bsonEncoder.putObject(new BasicBSONObject());

        assertThrows(IllegalStateException.class, () -> bsonEncoder.set(new BasicOutputBuffer()));
    }

    @ParameterizedTest
    @MethodSource("uuidEncodingArgs")
    @DisplayName("should encode UUID according to default uuid representation")
    void shouldEncodeUuidAccordingToDefaultRepresentation(final UuidRepresentation uuidRepresentation) {
        UuidRepresentation defaultUuidRepresentation = getDefaultUuidRepresentation();
        UUID uuid = new UUID(1, 2);
        BasicBSONObject document = new BasicBSONObject();
        document.append("u", uuid);

        try {
            assertEquals(JAVA_LEGACY, defaultUuidRepresentation);
            setDefaultUuidRepresentation(uuidRepresentation);
            byte[] bytes = bsonEncoder.encode(new BasicBSONObject(document));
            BsonDocument decodedDocument = new BsonDocumentCodec().decode(new BsonBinaryReader(ByteBuffer.wrap(bytes)),
                    DecoderContext.builder().build());

            assertEquals(uuid, decodedDocument.getBinary("u").asUuid(uuidRepresentation));
        } finally {
            setDefaultUuidRepresentation(defaultUuidRepresentation);
        }
    }

    private static Stream<Arguments> uuidEncodingArgs() {
        return Stream.of(
                Arguments.of(JAVA_LEGACY),
                Arguments.of(STANDARD)
        );
    }
}
