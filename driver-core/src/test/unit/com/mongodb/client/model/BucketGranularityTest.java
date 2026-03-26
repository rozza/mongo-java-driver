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

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BucketGranularityTest {

    private static Stream<Arguments> shouldReturnTheExpectedStringValue() {
        return Stream.of(
                Arguments.of(BucketGranularity.R5, "R5"),
                Arguments.of(BucketGranularity.R10, "R10"),
                Arguments.of(BucketGranularity.R20, "R20"),
                Arguments.of(BucketGranularity.R40, "R40"),
                Arguments.of(BucketGranularity.R80, "R80"),
                Arguments.of(BucketGranularity.SERIES_125, "1-2-5"),
                Arguments.of(BucketGranularity.E6, "E6"),
                Arguments.of(BucketGranularity.E12, "E12"),
                Arguments.of(BucketGranularity.E24, "E24"),
                Arguments.of(BucketGranularity.E48, "E48"),
                Arguments.of(BucketGranularity.E96, "E96"),
                Arguments.of(BucketGranularity.E192, "E192"),
                Arguments.of(BucketGranularity.POWERSOF2, "POWERSOF2")
        );
    }

    @ParameterizedTest
    @MethodSource
    void shouldReturnTheExpectedStringValue(final BucketGranularity granularity, final String expectedString) {
        assertEquals(expectedString, granularity.getValue());
    }
}
