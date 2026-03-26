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

import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.bson.BsonHelper.toBson;
import static org.bson.BsonHelper.valuesOfEveryType;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LazyBSONObjectTest {

    @ParameterizedTest
    @MethodSource("readTypeArgs")
    @DisplayName("should read types")
    void shouldReadType(final Object value, final byte[] bytes) {
        LazyBSONObject lazyBSONObject = new LazyBSONObject(bytes, new LazyBSONCallback());

        if (value instanceof Pattern) {
            Pattern expected = (Pattern) value;
            Pattern actual = (Pattern) lazyBSONObject.get("f");
            assertEquals(expected.pattern(), actual.pattern());
            assertEquals(expected.flags(), actual.flags());
        } else if (value instanceof LazyBSONObject) {
            // LazyBSONObject equality is based on content
            assertEquals(value, lazyBSONObject.get("f"));
        } else {
            assertEquals(value, lazyBSONObject.get("f"));
        }
        assertTrue(lazyBSONObject.keySet().contains("f"));
    }

    private static Stream<Arguments> readTypeArgs() {
        return Stream.of(
                Arguments.of(-1.01, new byte[]{16, 0, 0, 0, 1, 102, 0, 41, 92, -113, -62, -11, 40, -16, -65, 0}),
                Arguments.of((double) Float.MIN_VALUE, new byte[]{16, 0, 0, 0, 1, 102, 0, 0, 0, 0, 0, 0, 0, -96, 54, 0}),
                Arguments.of(Double.MAX_VALUE, new byte[]{16, 0, 0, 0, 1, 102, 0, -1, -1, -1, -1, -1, -1, -17, 127, 0}),
                Arguments.of(0.0, new byte[]{16, 0, 0, 0, 1, 102, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
                Arguments.of("", new byte[]{13, 0, 0, 0, 2, 102, 0, 1, 0, 0, 0, 0, 0}),
                Arguments.of("danke", new byte[]{18, 0, 0, 0, 2, 102, 0, 6, 0, 0, 0, 100, 97, 110, 107, 101, 0, 0}),
                Arguments.of(",+\\\"<>;[]{}@#$%^&*()+_", new byte[]{35, 0, 0, 0, 2, 102, 0, 23, 0, 0, 0, 44, 43, 92, 34, 60, 62, 59, 91, 93, 123, 125, 64, 35, 36, 37, 94, 38, 42, 40, 41, 43, 95, 0, 0}),
                Arguments.of("a\u00e9\u3042\u0430\u0432\u0431\u0434", new byte[]{27, 0, 0, 0, 2, 102, 0, 15, 0, 0, 0, 97, -61, -87, -29, -127, -126, -48, -80, -48, -78, -48, -79, -48, -76, 0, 0}),
                Arguments.of(new Binary((byte) 0x01, new byte[]{115, 116, 11}), new byte[]{16, 0, 0, 0, 5, 102, 0, 3, 0, 0, 0, 1, 115, 116, 11, 0}),
                Arguments.of(new Binary((byte) 0x03, new byte[]{115, 116, 11}), new byte[]{16, 0, 0, 0, 5, 102, 0, 3, 0, 0, 0, 3, 115, 116, 11, 0}),
                Arguments.of(new Binary((byte) 0x04, new byte[]{115, 116, 11}), new byte[]{16, 0, 0, 0, 5, 102, 0, 3, 0, 0, 0, 4, 115, 116, 11, 0}),
                Arguments.of(new ObjectId("50d3332018c6a1d8d1662b61"), new byte[]{20, 0, 0, 0, 7, 102, 0, 80, -45, 51, 32, 24, -58, -95, -40, -47, 102, 43, 97, 0}),
                Arguments.of(true, new byte[]{9, 0, 0, 0, 8, 102, 0, 1, 0}),
                Arguments.of(false, new byte[]{9, 0, 0, 0, 8, 102, 0, 0, 0}),
                Arguments.of(new Date(582163200), new byte[]{16, 0, 0, 0, 9, 102, 0, 0, 27, -77, 34, 0, 0, 0, 0, 0}),
                Arguments.of(null, new byte[]{8, 0, 0, 0, 10, 102, 0, 0}),
                Arguments.of(null, new byte[]{8, 0, 0, 0, 6, 102, 0, 0}),
                Arguments.of(Pattern.compile("[a]*", Pattern.CASE_INSENSITIVE), new byte[]{15, 0, 0, 0, 11, 102, 0, 91, 97, 93, 42, 0, 105, 0, 0}),
                Arguments.of(new Code("var i = 0"), new byte[]{22, 0, 0, 0, 13, 102, 0, 10, 0, 0, 0, 118, 97, 114, 32, 105, 32, 61, 32, 48, 0, 0}),
                Arguments.of(new Symbol("c"), new byte[]{14, 0, 0, 0, 14, 102, 0, 2, 0, 0, 0, 99, 0, 0}),
                Arguments.of(new CodeWScope("i++", new BasicBSONObject("x", 1)), new byte[]{32, 0, 0, 0, 15, 102, 0, 24, 0, 0, 0, 4, 0, 0, 0, 105, 43, 43, 0, 12, 0, 0, 0, 16, 120, 0, 1, 0, 0, 0, 0, 0}),
                Arguments.of(-12, new byte[]{12, 0, 0, 0, 16, 102, 0, -12, -1, -1, -1, 0}),
                Arguments.of(Integer.MIN_VALUE, new byte[]{12, 0, 0, 0, 16, 102, 0, 0, 0, 0, -128, 0}),
                Arguments.of(0, new byte[]{12, 0, 0, 0, 16, 102, 0, 0, 0, 0, 0, 0}),
                Arguments.of(new BSONTimestamp(123999401, 44332), new byte[]{16, 0, 0, 0, 17, 102, 0, 44, -83, 0, 0, -87, 20, 100, 7, 0}),
                Arguments.of(Long.MAX_VALUE, new byte[]{16, 0, 0, 0, 18, 102, 0, -1, -1, -1, -1, -1, -1, -1, 127, 0}),
                Arguments.of(new MinKey(), new byte[]{8, 0, 0, 0, -1, 102, 0, 0}),
                Arguments.of(new MaxKey(), new byte[]{8, 0, 0, 0, 127, 102, 0, 0}),
                Arguments.of(Decimal128.parse("0E-6176"), new byte[]{24, 0, 0, 0, 19, 102, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
        );
    }

    @ParameterizedTest
    @MethodSource("valuesOfEveryTypeArgs")
    @DisplayName("should read value of every type")
    void shouldReadValueOfEveryType(final BsonValue value) {
        BsonDocument bsonDocument = new BsonDocument("name", value);
        BasicBSONCallback callback = new BasicBSONCallback();
        new BasicBSONDecoder().decode(toBson(bsonDocument).array(), callback);
        BasicBSONObject dbObject = (BasicBSONObject) callback.get();
        LazyBSONObject lazyBSONObject = new LazyBSONObject(toBson(bsonDocument).array(), new LazyBSONCallback());

        assertTrue(lazyBSONObject.keySet().contains("name"));

        Object expectedValue;
        if (value.getBsonType() == BsonType.UNDEFINED) {
            expectedValue = null;
        } else if (value.getBsonType() == BsonType.SYMBOL) {
            expectedValue = new Symbol(((BsonSymbol) value).getSymbol());
        } else {
            expectedValue = dbObject.get("name");
        }
        if (value.getBsonType() == BsonType.REGULAR_EXPRESSION) {
            Pattern expected = (Pattern) expectedValue;
            Pattern actual = (Pattern) lazyBSONObject.get("name");
            assertEquals(expected.pattern(), actual.pattern());
            assertEquals(expected.flags(), actual.flags());
        } else {
            assertEquals(expectedValue, lazyBSONObject.get("name"));
        }
    }

    private static Stream<Arguments> valuesOfEveryTypeArgs() {
        return valuesOfEveryType().stream().map(Arguments::of);
    }

    @Test
    @DisplayName("should have nested items as lazy")
    void shouldHaveNestedItemsAsLazy() {
        byte[] bytes = {
                53, 0, 0, 0, 4, 97, 0, 26, 0, 0, 0, 16, 48, 0, 1, 0, 0, 0, 16, 49, 0, 2, 0, 0, 0, 16, 50, 0,
                3, 0, 0, 0, 0, 3, 111, 0, 16, 0, 0, 0, 1, 122, 0, -102, -103, -103, -103, -103, -103, -71, 63, 0, 0
        };

        LazyBSONObject document = new LazyBSONObject(bytes, new LazyBSONCallback());

        assertTrue(document.get("a") instanceof LazyBSONList);
        assertTrue(document.get("o") instanceof LazyBSONObject);
    }

    @Test
    @DisplayName("should not understand DBRefs")
    void shouldNotUnderstandDBRefs() {
        byte[] bytes = {
                44, 0, 0, 0, 3, 102, 0, 36, 0, 0, 0, 2, 36, 114, 101, 102,
                0, 4, 0, 0, 0, 97, 46, 98, 0, 7, 36, 105, 100, 0, 18, 52,
                86, 120, -112, 18, 52, 86, 120, -112, 18, 52, 0, 0,
        };

        LazyBSONObject document = new LazyBSONObject(bytes, new LazyBSONCallback());

        assertTrue(document.get("f") instanceof LazyBSONObject);
        assertEquals(new HashSet<>(Arrays.asList("$ref", "$id")), ((BSONObject) document.get("f")).keySet());
    }

    @Test
    @DisplayName("should retain fields order")
    void shouldRetainFieldsOrder() {
        byte[] bytes = {
                47, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 16, 98, 0, 2, 0, 0, 0, 16, 100, 0, 3, 0, 0,
                0, 16, 99, 0, 4, 0, 0, 0, 16, 101, 0, 5, 0, 0, 0, 16, 48, 0, 6, 0, 0, 0, 0
        };

        Iterator<String> iterator = new LazyBSONObject(bytes, new LazyBSONCallback()).keySet().iterator();

        assertEquals("a", iterator.next());
        assertEquals("b", iterator.next());
        assertEquals("d", iterator.next());
        assertEquals("c", iterator.next());
        assertEquals("e", iterator.next());
        assertEquals("0", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    @DisplayName("should be able to compare itself to others")
    void shouldBeAbleToCompareItselfToOthers() {
        byte[] bytes = {
                39, 0, 0, 0, 3, 97, 0,
                14, 0, 0, 0, 2, 120, 0, 2, 0, 0, 0, 121, 0, 0,
                3, 98, 0,
                14, 0, 0, 0, 2, 120, 0, 2, 0, 0, 0, 121, 0, 0,
                0
        };

        LazyBSONObject bsonObject1 = new LazyBSONObject(bytes, new LazyBSONCallback());
        LazyBSONObject bsonObject2 = new LazyBSONObject(bytes, new LazyBSONCallback());
        LazyBSONObject bsonObject3 = new LazyBSONObject(bytes, 7, new LazyBSONCallback());
        LazyBSONObject bsonObject4 = new LazyBSONObject(bytes, 24, new LazyBSONCallback());
        LazyBSONObject bsonObject5 = new LazyBSONObject(new byte[]{14, 0, 0, 0, 2, 120, 0, 2, 0, 0, 0, 121, 0, 0}, new LazyBSONCallback());
        LazyBSONObject bsonObject6 = new LazyBSONObject(new byte[]{5, 0, 0, 0, 0}, new LazyBSONCallback());

        assertTrue(bsonObject1.equals(bsonObject1));
        assertFalse(bsonObject1.equals(null));
        assertFalse(bsonObject1.equals("not equal"));
        assertTrue(bsonObject1.equals(bsonObject2));
        assertTrue(bsonObject3.equals(bsonObject4));
        assertFalse(bsonObject1.equals(bsonObject3));
        assertTrue(bsonObject4.equals(bsonObject5));
        assertFalse(bsonObject1.equals(bsonObject6));

        assertEquals(bsonObject1.hashCode(), bsonObject2.hashCode());
        assertEquals(bsonObject3.hashCode(), bsonObject4.hashCode());
        assertNotEquals(bsonObject1.hashCode(), bsonObject3.hashCode());
        assertEquals(bsonObject4.hashCode(), bsonObject5.hashCode());
        assertNotEquals(bsonObject1.hashCode(), bsonObject6.hashCode());
    }

    @Test
    @DisplayName("should return the size of a document")
    void shouldReturnSizeOfDocument() {
        byte[] bytes = {12, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0};
        LazyBSONObject document = new LazyBSONObject(bytes, new LazyBSONCallback());
        assertEquals(12, document.getBSONSize());
    }

    @Test
    @DisplayName("should understand that object is empty")
    void shouldUnderstandObjectIsEmpty() {
        byte[] bytes = {5, 0, 0, 0, 0};
        LazyBSONObject document = new LazyBSONObject(bytes, new LazyBSONCallback());
        assertTrue(document.isEmpty());
    }

    @Test
    @DisplayName("should implement Map.keySet()")
    void shouldImplementKeySet() {
        byte[] bytes = {16, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 8, 98, 0, 1, 0};
        LazyBSONObject document = new LazyBSONObject(bytes, new LazyBSONCallback());

        assertTrue(document.containsField("a"));
        assertFalse(document.containsField("z"));
        assertNull(document.get("z"));
        assertEquals(new HashSet<>(Arrays.asList("a", "b")), document.keySet());
    }

    @Test
    @DisplayName("should implement Map.entrySet()")
    void shouldImplementEntrySet() {
        byte[] bytes = {16, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 8, 98, 0, 1, 0};
        LazyBSONObject document = new LazyBSONObject(bytes, new LazyBSONCallback());

        Set<Map.Entry<String, Object>> entrySet = document.entrySet();

        assertEquals(2, entrySet.size());
        assertFalse(entrySet.isEmpty());
        assertTrue(entrySet.contains(new AbstractMap.SimpleImmutableEntry<>("a", 1)));
        assertFalse(entrySet.contains(new AbstractMap.SimpleImmutableEntry<>("a", 2)));
        assertTrue(entrySet.containsAll(Arrays.asList(
                new AbstractMap.SimpleImmutableEntry<>("a", 1),
                new AbstractMap.SimpleImmutableEntry<>("b", true))));
        assertFalse(entrySet.containsAll(Arrays.asList(
                new AbstractMap.SimpleImmutableEntry<>("a", 1),
                new AbstractMap.SimpleImmutableEntry<>("b", false))));

        Iterator<Map.Entry<String, Object>> iterator = entrySet.iterator();
        assertTrue(iterator.hasNext());
        assertEquals(new AbstractMap.SimpleImmutableEntry<>("a", 1), iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(new AbstractMap.SimpleImmutableEntry<>("b", true), iterator.next());
        assertFalse(iterator.hasNext());

        assertThrows(UnsupportedOperationException.class, () -> entrySet.add(new AbstractMap.SimpleImmutableEntry<>("key", null)));
        assertThrows(UnsupportedOperationException.class, () -> entrySet.addAll(
                Arrays.asList(new AbstractMap.SimpleImmutableEntry<>("key", null))));
        assertThrows(UnsupportedOperationException.class, entrySet::clear);
        assertThrows(UnsupportedOperationException.class, () -> entrySet.remove(new AbstractMap.SimpleImmutableEntry<>("key", null)));
        assertThrows(UnsupportedOperationException.class, () -> entrySet.removeAll(
                Arrays.asList(new AbstractMap.SimpleImmutableEntry<>("key", null))));
        assertThrows(UnsupportedOperationException.class, () -> entrySet.retainAll(
                Arrays.asList(new AbstractMap.SimpleImmutableEntry<>("key", null))));
    }

    @Test
    @DisplayName("should throw on modification")
    void shouldThrowOnModification() {
        LazyBSONObject document = new LazyBSONObject(
                new byte[]{16, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 8, 98, 0, 1, 0},
                new LazyBSONCallback());

        assertThrows(UnsupportedOperationException.class, () -> document.keySet().add("c"));
        assertThrows(UnsupportedOperationException.class, () -> document.put("c", 2));
        assertThrows(UnsupportedOperationException.class, () -> document.removeField("a"));
        assertThrows(UnsupportedOperationException.class, () -> document.toMap().put("a", 22));
    }

    @Test
    @DisplayName("should pipe to stream")
    void shouldPipeToStream() throws java.io.IOException {
        byte[] bytes = {16, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 8, 98, 0, 1, 0};
        LazyBSONObject document = new LazyBSONObject(bytes, new LazyBSONCallback());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        document.pipe(baos);

        assertArrayEquals(bytes, baos.toByteArray());
    }
}
