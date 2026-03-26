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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BsonTimestampTest {

    @Test
    @DisplayName("bsonType should get expected value")
    void bsonTypeShouldGetExpectedValue() {
        assertEquals(BsonType.TIMESTAMP, new BsonTimestamp().getBsonType());
    }

    @Test
    @DisplayName("compareTo should sort the timestamps as unsigned values")
    void compareToShouldSortAsUnsigned() {
        List<BsonTimestamp> timestamps = Arrays.asList(
                new BsonTimestamp(Long.MIN_VALUE),
                new BsonTimestamp(Long.MAX_VALUE),
                new BsonTimestamp(1),
                new BsonTimestamp(2),
                new BsonTimestamp(-1),
                new BsonTimestamp(-2));

        Collections.sort(timestamps);

        assertEquals(Arrays.asList(
                new BsonTimestamp(1),
                new BsonTimestamp(2),
                new BsonTimestamp(Long.MAX_VALUE),
                new BsonTimestamp(Long.MIN_VALUE),
                new BsonTimestamp(-2),
                new BsonTimestamp(-1)),
                timestamps);
    }

    @ParameterizedTest
    @MethodSource("constructorArgs")
    @DisplayName("constructors should initialize instance")
    void constructorsShouldInitializeInstance(final int seconds, final int increment, final long value) {
        BsonTimestamp tsFromValue = new BsonTimestamp(value);
        BsonTimestamp tsFromSecondsAndIncrement = new BsonTimestamp(seconds, increment);

        assertEquals(seconds, tsFromValue.getTime());
        assertEquals(increment, tsFromValue.getInc());
        assertEquals(value, tsFromValue.getValue());

        assertEquals(seconds, tsFromSecondsAndIncrement.getTime());
        assertEquals(increment, tsFromSecondsAndIncrement.getInc());
        assertEquals(value, tsFromSecondsAndIncrement.getValue());
    }

    private static Stream<Arguments> constructorArgs() {
        return Stream.of(
                Arguments.of(0, 0, 0L),
                Arguments.of(1, 2, 0x100000002L),
                Arguments.of(-1, -2, 0xfffffffffffffffeL),
                Arguments.of(123456789, 42, 530242871224172586L),
                Arguments.of(Integer.MIN_VALUE, Integer.MIN_VALUE, 0x8000000080000000L),
                Arguments.of(Integer.MIN_VALUE, Integer.MAX_VALUE, 0x800000007fffffffL),
                Arguments.of(Integer.MAX_VALUE, Integer.MIN_VALUE, 0x7fffffff80000000L),
                Arguments.of(Integer.MAX_VALUE, Integer.MAX_VALUE, 0x7fffffff7fffffffL)
        );
    }

    @Test
    @DisplayName("no args constructor should initialize instance")
    void noArgsConstructorShouldInitializeInstance() {
        BsonTimestamp tsFromValue = new BsonTimestamp();

        assertEquals(0, tsFromValue.getTime());
        assertEquals(0, tsFromValue.getInc());
        assertEquals(0, tsFromValue.getValue());
    }
}
