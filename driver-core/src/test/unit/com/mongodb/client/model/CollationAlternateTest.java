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
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CollationAlternateTest {

    private static Stream<Arguments> shouldReturnTheExpectedStringValue() {
        return Stream.of(
                Arguments.of(CollationAlternate.SHIFTED, "shifted"),
                Arguments.of(CollationAlternate.NON_IGNORABLE, "non-ignorable")
        );
    }

    @ParameterizedTest
    @MethodSource
    void shouldReturnTheExpectedStringValue(final CollationAlternate collationAlternate, final String expectedString) {
        assertEquals(expectedString, collationAlternate.getValue());
    }

    private static Stream<Arguments> shouldSupportValidStringRepresentations() {
        return Stream.of(
                Arguments.of(CollationAlternate.SHIFTED, "shifted"),
                Arguments.of(CollationAlternate.NON_IGNORABLE, "non-ignorable")
        );
    }

    @ParameterizedTest
    @MethodSource
    void shouldSupportValidStringRepresentations(final CollationAlternate collationAlternate, final String stringValue) {
        assertEquals(collationAlternate, CollationAlternate.fromString(stringValue));
    }

    @ParameterizedTest
    @ValueSource(strings = {"info"})
    @NullAndEmptySource
    void shouldThrowAnIllegalArgumentExceptionForInvalidValues(final String stringValue) {
        assertThrows(IllegalArgumentException.class, () -> CollationAlternate.fromString(stringValue));
    }
}
