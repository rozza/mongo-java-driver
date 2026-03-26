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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BsonBinarySubTypeTest {

    @ParameterizedTest
    @MethodSource("isUuidArgs")
    @DisplayName("should be uuid only for legacy and uuid types")
    void shouldBeUuidOnlyForLegacyAndUuidTypes(final byte value, final boolean isUuid) {
        assertEquals(isUuid, BsonBinarySubType.isUuid(value));
    }

    private static Stream<Arguments> isUuidArgs() {
        return Stream.of(
                Arguments.of((byte) 1, false),
                Arguments.of((byte) 2, false),
                Arguments.of((byte) 3, true),
                Arguments.of((byte) 4, true),
                Arguments.of((byte) 5, false),
                Arguments.of((byte) 6, false),
                Arguments.of((byte) 7, false),
                Arguments.of((byte) 8, false),
                Arguments.of((byte) 9, false)
        );
    }
}
