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

package org.bson.internal;

import org.bson.BSONException;
import org.bson.UuidRepresentation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UuidHelperTest {

    private static final UUID EXPECTED_UUID = UUID.fromString("08070605-0403-0201-100f-0e0d0c0b0a09");

    private static Stream<Arguments> encodeUuidArgs() {
        return Stream.of(
                Arguments.of(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, UuidRepresentation.JAVA_LEGACY),
                Arguments.of(new byte[]{8, 7, 6, 5, 4, 3, 2, 1, 16, 15, 14, 13, 12, 11, 10, 9}, UuidRepresentation.STANDARD),
                Arguments.of(new byte[]{8, 7, 6, 5, 4, 3, 2, 1, 16, 15, 14, 13, 12, 11, 10, 9}, UuidRepresentation.PYTHON_LEGACY),
                Arguments.of(new byte[]{5, 6, 7, 8, 3, 4, 1, 2, 16, 15, 14, 13, 12, 11, 10, 9}, UuidRepresentation.C_SHARP_LEGACY)
        );
    }

    @ParameterizedTest
    @MethodSource("encodeUuidArgs")
    void shouldEncodeDifferentTypesOfUuid(final byte[] expectedBytes, final UuidRepresentation uuidRepresentation) {
        assertArrayEquals(expectedBytes, UuidHelper.encodeUuidToBinary(EXPECTED_UUID, uuidRepresentation));
    }

    private static Stream<Arguments> decodeUuidArgs() {
        return Stream.of(
                Arguments.of(UUID.fromString("08070605-0403-0201-100f-0e0d0c0b0a09"), (byte) 3, UuidRepresentation.JAVA_LEGACY),
                Arguments.of(UUID.fromString("01020304-0506-0708-090a-0b0c0d0e0f10"), (byte) 3, UuidRepresentation.PYTHON_LEGACY),
                Arguments.of(UUID.fromString("04030201-0605-0807-090a-0b0c0d0e0f10"), (byte) 3, UuidRepresentation.C_SHARP_LEGACY),
                Arguments.of(UUID.fromString("01020304-0506-0708-090a-0b0c0d0e0f10"), (byte) 4, UuidRepresentation.STANDARD)
        );
    }

    @ParameterizedTest
    @MethodSource("decodeUuidArgs")
    void shouldDecodeDifferentTypesOfUuid(final UUID uuid, final byte type, final UuidRepresentation uuidRepresentation) {
        byte[] expectedBytes = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
        assertEquals(uuid, UuidHelper.decodeBinaryToUuid(expectedBytes, type, uuidRepresentation));
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, expectedBytes);
    }

    @Test
    void shouldErrorWhenDecodingSubtype3BinaryToStandardRepresentation() {
        byte[] expectedBytes = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
        assertThrows(BSONException.class, () ->
                UuidHelper.decodeBinaryToUuid(expectedBytes, (byte) 3, UuidRepresentation.STANDARD));
    }
}
