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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BsonBinaryUnitTest {

    @ParameterizedTest
    @MethodSource("dataArgs")
    @DisplayName("should initialize with data")
    void shouldInitializeWithData(final byte[] data) {
        BsonBinary bsonBinary = new BsonBinary((byte) 80, data);
        assertArrayEquals(data, bsonBinary.getData());
    }

    private static Stream<Arguments> dataArgs() {
        return Stream.of(
                Arguments.of((Object) new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
                Arguments.of((Object) new byte[]{2, 5, 4, 67, 3, 4, 5, 2, 4, 2, 5, 6, 7, 4, 5, 12}),
                Arguments.of((Object) new byte[]{34, 24, 56, 76, 3, 4, 1, 12, 1, 9, 8, 7, 56, 46, 3, 9})
        );
    }

    @ParameterizedTest
    @MethodSource("subTypeArgs")
    @DisplayName("should initialize with data and BsonBinarySubType")
    void shouldInitializeWithDataAndSubType(final BsonBinarySubType subType) {
        byte[] data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
        BsonBinary bsonBinary = new BsonBinary(subType, data);

        assertEquals(subType.getValue(), bsonBinary.getType());
        assertArrayEquals(data, bsonBinary.getData());
    }

    private static Stream<Arguments> subTypeArgs() {
        return Stream.of(
                Arguments.of(BsonBinarySubType.BINARY),
                Arguments.of(BsonBinarySubType.FUNCTION),
                Arguments.of(BsonBinarySubType.MD5),
                Arguments.of(BsonBinarySubType.OLD_BINARY),
                Arguments.of(BsonBinarySubType.USER_DEFINED),
                Arguments.of(BsonBinarySubType.UUID_LEGACY),
                Arguments.of(BsonBinarySubType.UUID_STANDARD),
                Arguments.of(BsonBinarySubType.VECTOR)
        );
    }

    @ParameterizedTest
    @MethodSource("uuidArgs")
    @DisplayName("should initialize with UUID")
    void shouldInitializeWithUuid(final UUID uuid) {
        BsonBinary bsonBinary = new BsonBinary(uuid);
        assertEquals(uuid, bsonBinary.asUuid());
    }

    private static Stream<Arguments> uuidArgs() {
        return Stream.of(
                Arguments.of(UUID.fromString("ffadee18-b533-11e8-96f8-529269fb1459")),
                Arguments.of(UUID.fromString("a5dc280e-b534-11e8-96f8-529269fb1459")),
                Arguments.of(UUID.fromString("4ef2a357-cb16-45a6-a6f6-a11ae1972917"))
        );
    }

    @ParameterizedTest
    @MethodSource("uuidRepresentationArgs")
    @DisplayName("should initialize with UUID and UUID representation")
    void shouldInitializeWithUuidAndRepresentation(final UuidRepresentation uuidRepresentation) {
        UUID uuid = UUID.fromString("ffadee18-b533-11e8-96f8-529269fb1459");
        BsonBinary bsonBinary = new BsonBinary(uuid, uuidRepresentation);
        assertEquals(uuid, bsonBinary.asUuid(uuidRepresentation));
    }

    private static Stream<Arguments> uuidRepresentationArgs() {
        return Stream.of(
                Arguments.of(UuidRepresentation.STANDARD),
                Arguments.of(UuidRepresentation.C_SHARP_LEGACY),
                Arguments.of(UuidRepresentation.JAVA_LEGACY),
                Arguments.of(UuidRepresentation.PYTHON_LEGACY)
        );
    }
}
