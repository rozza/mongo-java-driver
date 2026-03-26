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

package com.mongodb.client.model;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CollationStrengthTest {

    private static Stream<Arguments> shouldReturnTheExpectedIntValue() {
        return Stream.of(
                Arguments.of(CollationStrength.PRIMARY, 1),
                Arguments.of(CollationStrength.SECONDARY, 2),
                Arguments.of(CollationStrength.TERTIARY, 3),
                Arguments.of(CollationStrength.QUATERNARY, 4),
                Arguments.of(CollationStrength.IDENTICAL, 5)
        );
    }

    @ParameterizedTest
    @MethodSource
    void shouldReturnTheExpectedIntValue(final CollationStrength collationStrength, final int expectedInt) {
        assertEquals(expectedInt, collationStrength.getIntRepresentation());
    }

    private static Stream<Arguments> shouldSupportValidIntRepresentations() {
        return Stream.of(
                Arguments.of(CollationStrength.PRIMARY, 1),
                Arguments.of(CollationStrength.SECONDARY, 2),
                Arguments.of(CollationStrength.TERTIARY, 3),
                Arguments.of(CollationStrength.QUATERNARY, 4),
                Arguments.of(CollationStrength.IDENTICAL, 5)
        );
    }

    @ParameterizedTest
    @MethodSource
    void shouldSupportValidIntRepresentations(final CollationStrength collationStrength, final int intValue) {
        assertEquals(collationStrength, CollationStrength.fromInt(intValue));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 6})
    void shouldThrowAnIllegalArgumentExceptionForInvalidValues(final int intValue) {
        assertThrows(IllegalArgumentException.class, () -> CollationStrength.fromInt(intValue));
    }
}
