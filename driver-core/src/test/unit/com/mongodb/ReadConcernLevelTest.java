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

package com.mongodb;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReadConcernLevelTest {

    @ParameterizedTest
    @MethodSource("expectedStringValues")
    @DisplayName("should return the expected string value")
    void shouldReturnExpectedStringValue(final ReadConcernLevel readConcernLevel, final String expectedString) {
        assertEquals(expectedString, readConcernLevel.getValue());
    }

    static Stream<Object[]> expectedStringValues() {
        return Stream.of(
                new Object[]{ReadConcernLevel.LOCAL, "local"},
                new Object[]{ReadConcernLevel.MAJORITY, "majority"},
                new Object[]{ReadConcernLevel.LINEARIZABLE, "linearizable"},
                new Object[]{ReadConcernLevel.SNAPSHOT, "snapshot"},
                new Object[]{ReadConcernLevel.AVAILABLE, "available"}
        );
    }

    @ParameterizedTest
    @MethodSource("validStringRepresentations")
    @DisplayName("should support valid string representations")
    void shouldSupportValidStringRepresentations(final String readConcernLevel) {
        assertInstanceOf(ReadConcernLevel.class, ReadConcernLevel.fromString(readConcernLevel));
    }

    static Stream<String> validStringRepresentations() {
        return Stream.of("local", "majority", "linearizable", "snapshot", "available",
                "LOCAL", "MAJORITY", "LINEARIZABLE", "SNAPSHOT", "AVAILABLE");
    }

    @Test
    @DisplayName("should throw an illegal Argument exception for invalid values")
    void shouldThrowForInvalidValues() {
        assertThrows(IllegalArgumentException.class, () -> ReadConcernLevel.fromString(null));
        assertThrows(IllegalArgumentException.class, () -> ReadConcernLevel.fromString("pickThree"));
    }
}
