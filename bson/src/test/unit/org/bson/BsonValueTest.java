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
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Date;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BsonValueTest {

    @Test
    @DisplayName("is methods should return true for the correct type")
    void isMethodsShouldReturnTrueForCorrectType() {
        assertTrue(new BsonNull().isNull());
        assertTrue(new BsonInt32(42).isInt32());
        assertTrue(new BsonInt32(42).isNumber());
        assertTrue(new BsonInt64(52L).isInt64());
        assertTrue(new BsonInt64(52L).isNumber());
        assertTrue(new BsonDecimal128(Decimal128.parse("1")).isDecimal128());
        assertTrue(new BsonDecimal128(Decimal128.parse("1")).isNumber());
        assertTrue(new BsonDouble(62.0).isDouble());
        assertTrue(new BsonDouble(62.0).isNumber());
        assertTrue(new BsonBoolean(true).isBoolean());
        assertTrue(new BsonDateTime(new Date().getTime()).isDateTime());
        assertTrue(new BsonString("the fox ...").isString());
        assertTrue(new BsonJavaScript("int i = 0;").isJavaScript());
        assertTrue(new BsonObjectId(new ObjectId()).isObjectId());
        assertTrue(new BsonJavaScriptWithScope("int x = y", new BsonDocument("y", new BsonInt32(1))).isJavaScriptWithScope());
        assertTrue(new BsonRegularExpression("^test.*regex.*xyz$", "i").isRegularExpression());
        assertTrue(new BsonSymbol("ruby stuff").isSymbol());
        assertTrue(new BsonTimestamp(0x12345678, 5).isTimestamp());
        assertTrue(new BsonBinary((byte) 80, new byte[]{5, 4, 3, 2, 1}).isBinary());
        assertTrue(new BsonDbPointer("n", new ObjectId()).isDBPointer());
        assertTrue(new BsonArray().isArray());
        assertTrue(new BsonDocument().isDocument());
    }

    @Test
    @DisplayName("is methods should return false for the incorrect type")
    void isMethodsShouldReturnFalseForIncorrectType() {
        assertFalse(new BsonBoolean(false).isNull());
        assertFalse(new BsonNull().isInt32());
        assertFalse(new BsonNull().isNumber());
        assertFalse(new BsonNull().isInt64());
        assertFalse(new BsonNull().isDecimal128());
        assertFalse(new BsonNull().isDouble());
        assertFalse(new BsonNull().isBoolean());
        assertFalse(new BsonNull().isDateTime());
        assertFalse(new BsonNull().isString());
        assertFalse(new BsonNull().isJavaScript());
        assertFalse(new BsonNull().isObjectId());
        assertFalse(new BsonNull().isJavaScriptWithScope());
        assertFalse(new BsonNull().isRegularExpression());
        assertFalse(new BsonNull().isSymbol());
        assertFalse(new BsonNull().isTimestamp());
        assertFalse(new BsonNull().isBinary());
        assertFalse(new BsonNull().isDBPointer());
        assertFalse(new BsonNull().isArray());
        assertFalse(new BsonNull().isDocument());
    }

    @ParameterizedTest
    @MethodSource("numberArgs")
    @DisplayName("support BsonNumber interface for all number types")
    void supportBsonNumberInterface(final BsonNumber bsonValue, final int intValue, final long longValue,
                                     final double doubleValue, final Decimal128 decimal128Value) {
        assertEquals(bsonValue, bsonValue.asNumber());
        assertEquals(intValue, bsonValue.asNumber().intValue());
        assertEquals(longValue, bsonValue.asNumber().longValue());
        assertEquals(doubleValue, bsonValue.asNumber().doubleValue());
        assertEquals(decimal128Value, bsonValue.asNumber().decimal128Value());
    }

    private static Stream<Arguments> numberArgs() {
        return Stream.of(
                Arguments.of(new BsonInt32(42), 42, 42L, 42.0, Decimal128.parse("42")),
                Arguments.of(new BsonInt64(42), 42, 42L, 42.0, Decimal128.parse("42")),
                Arguments.of(new BsonDouble(42), 42, 42L, 42.0, Decimal128.parse("42")),
                Arguments.of(new BsonDecimal128(Decimal128.parse("42")), 42, 42L, 42.0, Decimal128.parse("42")),
                Arguments.of(new BsonDecimal128(Decimal128.POSITIVE_INFINITY), Integer.MAX_VALUE, Long.MAX_VALUE, Double.POSITIVE_INFINITY, Decimal128.POSITIVE_INFINITY),
                Arguments.of(new BsonDecimal128(Decimal128.NEGATIVE_INFINITY), Integer.MIN_VALUE, Long.MIN_VALUE, Double.NEGATIVE_INFINITY, Decimal128.NEGATIVE_INFINITY),
                Arguments.of(new BsonDecimal128(Decimal128.NaN), 0, 0L, Double.NaN, Decimal128.NaN)
        );
    }

    @Test
    @DisplayName("as methods should throw for the incorrect type")
    void asMethodsShouldThrowForIncorrectType() {
        assertThrows(BsonInvalidOperationException.class, () -> new BsonNull().asInt32());
        assertThrows(BsonInvalidOperationException.class, () -> new BsonNull().asNumber());
        assertThrows(BsonInvalidOperationException.class, () -> new BsonNull().asInt64());
        assertThrows(BsonInvalidOperationException.class, () -> new BsonNull().asDouble());
        assertThrows(BsonInvalidOperationException.class, () -> new BsonNull().asDecimal128());
        assertThrows(BsonInvalidOperationException.class, () -> new BsonNull().asBoolean());
        assertThrows(BsonInvalidOperationException.class, () -> new BsonNull().asDateTime());
        assertThrows(BsonInvalidOperationException.class, () -> new BsonNull().asString());
        assertThrows(BsonInvalidOperationException.class, () -> new BsonNull().asJavaScript());
        assertThrows(BsonInvalidOperationException.class, () -> new BsonNull().asObjectId());
        assertThrows(BsonInvalidOperationException.class, () -> new BsonNull().asJavaScriptWithScope());
        assertThrows(BsonInvalidOperationException.class, () -> new BsonNull().asRegularExpression());
        assertThrows(BsonInvalidOperationException.class, () -> new BsonNull().asSymbol());
        assertThrows(BsonInvalidOperationException.class, () -> new BsonNull().asTimestamp());
        assertThrows(BsonInvalidOperationException.class, () -> new BsonNull().asBinary());
        assertThrows(BsonInvalidOperationException.class, () -> new BsonNull().asDBPointer());
        assertThrows(BsonInvalidOperationException.class, () -> new BsonNull().asArray());
        assertThrows(BsonInvalidOperationException.class, () -> new BsonNull().asDocument());
    }
}
