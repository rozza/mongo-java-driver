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

import org.bson.types.Decimal128;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BsonNumberTest {

    @Test
    @DisplayName("should convert to int value")
    void shouldConvertToIntValue() {
        assertEquals(1, new BsonInt32(1).intValue());

        assertEquals(1, new BsonInt64(1L).intValue());
        assertEquals(-1, new BsonInt64(Long.MAX_VALUE).intValue());
        assertEquals(0, new BsonInt64(Long.MIN_VALUE).intValue());

        assertEquals(3, new BsonDouble(3.14).intValue());

        assertEquals(1, new BsonDecimal128(new Decimal128(1L)).intValue());
    }

    @Test
    @DisplayName("should convert to long value")
    void shouldConvertToLongValue() {
        assertEquals(1L, new BsonInt32(1).longValue());

        assertEquals(1L, new BsonInt64(1L).longValue());

        assertEquals(3L, new BsonDouble(3.14).longValue());

        assertEquals(1L, new BsonDecimal128(new Decimal128(1L)).longValue());
    }

    @Test
    @DisplayName("should convert to double value")
    void shouldConvertToDoubleValue() {
        assertEquals(1.0d, new BsonInt32(1).doubleValue());

        assertEquals(1.0d, new BsonInt64(1L).doubleValue());
        assertEquals(9.223372036854776E18d, new BsonInt64(Long.MAX_VALUE).doubleValue());
        assertEquals(-9.223372036854776E18d, new BsonInt64(Long.MIN_VALUE).doubleValue());

        assertEquals(3.14d, new BsonDouble(3.14d).doubleValue());

        assertEquals(3.14d, new BsonDecimal128(Decimal128.parse("3.14")).doubleValue());
    }

    @Test
    @DisplayName("should convert to decimal128 value")
    void shouldConvertToDecimal128Value() {
        assertEquals(Decimal128.parse("1"), new BsonInt32(1).decimal128Value());

        assertEquals(Decimal128.parse("1"), new BsonInt64(1L).decimal128Value());
        assertEquals(Decimal128.parse("9223372036854775807"), new BsonInt64(Long.MAX_VALUE).decimal128Value());
        assertEquals(Decimal128.parse("-9223372036854775808"), new BsonInt64(Long.MIN_VALUE).decimal128Value());

        assertEquals(Decimal128.parse("1"), new BsonDouble(1.0d).decimal128Value());
        assertEquals(Decimal128.NaN, new BsonDouble(Double.NaN).decimal128Value());
        assertEquals(Decimal128.POSITIVE_INFINITY, new BsonDouble(Double.POSITIVE_INFINITY).decimal128Value());
        assertEquals(Decimal128.NEGATIVE_INFINITY, new BsonDouble(Double.NEGATIVE_INFINITY).decimal128Value());

        assertEquals(Decimal128.parse("3.14"), new BsonDecimal128(Decimal128.parse("3.14")).decimal128Value());
    }
}
