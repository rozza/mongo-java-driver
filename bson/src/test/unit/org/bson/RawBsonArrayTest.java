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

import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RawBsonArrayTest {

    private static final BsonArray BSON_ARRAY = new BsonArray(asList(
            new BsonInt32(1), new BsonInt32(2), new BsonDocument("x", BsonBoolean.TRUE),
            new BsonArray(asList(new BsonDocument("y", BsonBoolean.FALSE), new BsonArray(asList(new BsonInt32(1)))))));

    private static final BsonArray EMPTY_BSON_ARRAY = new BsonArray();
    private static final RawBsonArray EMPTY_RAW_BSON_ARRAY =
            (RawBsonArray) new RawBsonDocument(new BsonDocument("a", EMPTY_BSON_ARRAY), new BsonDocumentCodec()).get("a");

    @Test
    @DisplayName("constructors should throw if parameters are invalid")
    void constructorsShouldThrowIfParametersAreInvalid() {
        assertThrows(IllegalArgumentException.class, () -> new RawBsonArray(null));
        assertThrows(IllegalArgumentException.class, () -> new RawBsonArray(null, 0, 5));
        assertThrows(IllegalArgumentException.class, () -> new RawBsonArray(new byte[5], -1, 5));
        assertThrows(IllegalArgumentException.class, () -> new RawBsonArray(new byte[5], 5, 5));
        assertThrows(IllegalArgumentException.class, () -> new RawBsonArray(new byte[5], 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new RawBsonArray(new byte[10], 6, 5));
    }

    @Test
    @DisplayName("byteBuffer should contain the correct bytes")
    void byteBufferShouldContainTheCorrectBytes() {
        for (RawBsonArray rawBsonArray : createRawBsonArrayVariants()) {
            ByteBuf byteBuf = rawBsonArray.getByteBuffer();
            assertEquals(rawBsonArray, BSON_ARRAY);
            assertEquals(ByteOrder.LITTLE_ENDIAN, byteBuf.asNIO().order());
            assertEquals(66, byteBuf.remaining());

            byte[] actualBytes = new byte[66];
            byteBuf.get(actualBytes);
            assertArrayEquals(getBytesFromBsonArray(), actualBytes);
        }
    }

    @Test
    @DisplayName("contains should find existing values")
    void containsShouldFindExistingValues() {
        for (RawBsonArray rawBsonArray : createRawBsonArrayVariants()) {
            assertTrue(rawBsonArray.contains(BSON_ARRAY.get(0)));
            assertTrue(rawBsonArray.contains(BSON_ARRAY.get(1)));
            assertTrue(rawBsonArray.contains(BSON_ARRAY.get(2)));
            assertTrue(rawBsonArray.contains(BSON_ARRAY.get(3)));
        }
    }

    @Test
    @DisplayName("containsAll should return true if contains all")
    void containsAllShouldReturnTrue() {
        for (RawBsonArray rawBsonArray : createRawBsonArrayVariants()) {
            assertTrue(rawBsonArray.containsAll(BSON_ARRAY.getValues()));
        }
    }

    @Test
    @DisplayName("should return RawBsonDocument for sub documents and RawBsonArray for arrays")
    void shouldReturnCorrectTypesForSubDocumentsAndArrays() {
        for (RawBsonArray rawBsonArray : createRawBsonArrayVariants()) {
            assertTrue(rawBsonArray.get(0) instanceof BsonInt32);
            assertTrue(rawBsonArray.get(1) instanceof BsonInt32);
            assertTrue(rawBsonArray.get(2) instanceof RawBsonDocument);
            assertTrue(rawBsonArray.get(3) instanceof RawBsonArray);
            assertTrue(rawBsonArray.get(3).asArray().get(0) instanceof RawBsonDocument);
            assertTrue(rawBsonArray.get(3).asArray().get(1) instanceof RawBsonArray);

            assertTrue(rawBsonArray.get(2).asDocument().getBoolean("x").getValue());
            assertFalse(rawBsonArray.get(3).asArray().get(0).asDocument().getBoolean("y").getValue());
            assertEquals(1, rawBsonArray.get(3).asArray().get(1).asArray().get(0).asInt32().getValue());
        }
    }

    @Test
    @DisplayName("get should throw if index out of bounds")
    void getShouldThrowIfIndexOutOfBounds() {
        for (RawBsonArray rawBsonArray : createRawBsonArrayVariants()) {
            assertThrows(IndexOutOfBoundsException.class, () -> rawBsonArray.get(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> rawBsonArray.get(5));
        }
    }

    @Test
    @DisplayName("isEmpty should return false when the BsonArray is not empty")
    void isEmptyShouldReturnFalseWhenNotEmpty() {
        for (RawBsonArray rawBsonArray : createRawBsonArrayVariants()) {
            assertFalse(rawBsonArray.isEmpty());
        }
    }

    @Test
    @DisplayName("isEmpty should return true when the BsonArray is empty")
    void isEmptyShouldReturnTrueWhenEmpty() {
        assertTrue(EMPTY_RAW_BSON_ARRAY.isEmpty());
    }

    @Test
    @DisplayName("should get correct size when the BsonArray is empty")
    void shouldGetCorrectSizeWhenEmpty() {
        assertEquals(0, EMPTY_RAW_BSON_ARRAY.size());
    }

    @Test
    @DisplayName("should get correct values set when the BsonArray is empty")
    void shouldGetCorrectValuesWhenEmpty() {
        assertTrue(EMPTY_RAW_BSON_ARRAY.getValues().isEmpty());
    }

    @Test
    @DisplayName("should get correct size")
    void shouldGetCorrectSize() {
        for (RawBsonArray rawBsonArray : createRawBsonArrayVariants()) {
            assertEquals(4, rawBsonArray.size());
        }
    }

    @Test
    @DisplayName("should get correct values set")
    void shouldGetCorrectValuesSet() {
        for (RawBsonArray rawBsonArray : createRawBsonArrayVariants()) {
            assertEquals(BSON_ARRAY.getValues(), rawBsonArray.getValues());
        }
    }

    @Test
    @DisplayName("all write methods should throw UnsupportedOperationException")
    void allWriteMethodsShouldThrow() {
        RawBsonArray rawBsonArray = createRawBsonArrayFromBsonArray();

        assertThrows(UnsupportedOperationException.class, rawBsonArray::clear);
        assertThrows(UnsupportedOperationException.class, () -> rawBsonArray.add(BsonNull.VALUE));
        assertThrows(UnsupportedOperationException.class, () -> rawBsonArray.add(1, BsonNull.VALUE));
        assertThrows(UnsupportedOperationException.class, () -> rawBsonArray.addAll(Collections.singletonList(BsonNull.VALUE)));
        assertThrows(UnsupportedOperationException.class, () -> rawBsonArray.addAll(1, Collections.singletonList(BsonNull.VALUE)));
        assertThrows(UnsupportedOperationException.class, () -> rawBsonArray.remove(BsonNull.VALUE));
        assertThrows(UnsupportedOperationException.class, () -> rawBsonArray.remove(1));
        assertThrows(UnsupportedOperationException.class, () -> rawBsonArray.removeAll(Collections.singletonList(BsonNull.VALUE)));
        assertThrows(UnsupportedOperationException.class, () -> rawBsonArray.retainAll(Collections.singletonList(BsonNull.VALUE)));
        assertThrows(UnsupportedOperationException.class, () -> rawBsonArray.set(0, BsonNull.VALUE));
    }

    @Test
    @DisplayName("should find the indexOf a value")
    void shouldFindIndexOfValue() {
        for (RawBsonArray rawBsonArray : createRawBsonArrayVariants()) {
            assertEquals(2, rawBsonArray.indexOf(BSON_ARRAY.get(2)));
        }
    }

    @Test
    @DisplayName("should find the lastIndexOf a value")
    void shouldFindLastIndexOfValue() {
        RawBsonArray rawBsonArray = (RawBsonArray) RawBsonDocument.parse("{a: [1, 2, 3, 1]}").get("a");
        assertEquals(3, rawBsonArray.lastIndexOf(rawBsonArray.get(0)));
    }

    @Test
    @DisplayName("should return a valid iterator for empty Bson Arrays")
    void shouldReturnValidIteratorForEmptyArrays() {
        Iterator<BsonValue> iterator = EMPTY_RAW_BSON_ARRAY.iterator();
        assertFalse(iterator.hasNext());
        assertFalse(iterator.hasNext());
    }

    @Test
    @DisplayName("should return a listIterator")
    void shouldReturnListIterator() {
        RawBsonArray rawBsonArray = (RawBsonArray) RawBsonDocument.parse("{a: [1, 2, 3, 1]}").get("a");
        List<BsonValue> fromIterator = new ArrayList<>();
        rawBsonArray.listIterator().forEachRemaining(fromIterator::add);
        assertEquals(rawBsonArray.getValues(), fromIterator);
    }

    @Test
    @DisplayName("should return a listIterator with index")
    void shouldReturnListIteratorWithIndex() {
        RawBsonArray rawBsonArray = (RawBsonArray) RawBsonDocument.parse("{a: [1, 2, 3, 1]}").get("a");
        List<BsonValue> fromIterator = new ArrayList<>();
        rawBsonArray.listIterator(1).forEachRemaining(fromIterator::add);
        assertEquals(rawBsonArray.getValues().subList(1, 4), fromIterator);
    }

    @Test
    @DisplayName("should iterate forwards and backwards through a list iterator")
    void shouldIterateForwardsAndBackwards() {
        RawBsonArray rawBsonArray = (RawBsonArray) RawBsonDocument.parse("{a: [1, 2, 3, 4]}").get("a");
        ListIterator<BsonValue> iter = rawBsonArray.listIterator();

        assertEquals(new BsonInt32(1), iter.next());
        assertEquals(new BsonInt32(1), iter.previous());
        assertEquals(new BsonInt32(1), iter.next());
        assertEquals(new BsonInt32(2), iter.next());
        assertEquals(new BsonInt32(2), iter.previous());
        assertEquals(new BsonInt32(1), iter.previous());

        assertThrows(NoSuchElementException.class, iter::previous);
    }

    @Test
    @DisplayName("should return a sublist")
    void shouldReturnSublist() {
        RawBsonArray rawBsonArray = (RawBsonArray) RawBsonDocument.parse("{a: [1, 2, 3, 1]}").get("a");
        assertEquals(rawBsonArray.getValues().subList(2, 3), rawBsonArray.subList(2, 3));
    }

    @Test
    @DisplayName("hashCode should equal hash code of identical BsonArray")
    void hashCodeShouldEqualBsonArrayHashCode() {
        for (RawBsonArray rawBsonArray : createRawBsonArrayVariants()) {
            assertEquals(BSON_ARRAY.hashCode(), rawBsonArray.hashCode());
        }
    }

    @Test
    @DisplayName("equals should equal identical BsonArray")
    void equalsShouldEqualBsonArray() {
        for (RawBsonArray rawBsonArray : createRawBsonArrayVariants()) {
            assertTrue(rawBsonArray.equals(BSON_ARRAY));
            assertTrue(BSON_ARRAY.equals(rawBsonArray));
            assertTrue(rawBsonArray.equals(rawBsonArray));
            assertFalse(rawBsonArray.equals(EMPTY_RAW_BSON_ARRAY));
        }
    }

    @Test
    @DisplayName("clone should make a deep copy")
    void cloneShouldMakeDeepCopy() {
        for (RawBsonArray rawBsonArray : createRawBsonArrayVariants()) {
            RawBsonArray cloned = (RawBsonArray) rawBsonArray.clone();
            assertNotSame(cloned.getByteBuffer().array(), createRawBsonArrayFromBsonArray().getByteBuffer().array());
            assertEquals(rawBsonArray.getByteBuffer().remaining(), cloned.getByteBuffer().remaining());
            assertEquals(createRawBsonArrayFromBsonArray(), cloned);
        }
    }

    @Test
    @DisplayName("should serialize and deserialize")
    void shouldSerializeAndDeserialize() throws Exception {
        for (RawBsonArray rawBsonArray : createRawBsonArrayVariants()) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(rawBsonArray);
            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bais);
            Object deserializedDocument = ois.readObject();
            assertEquals(BSON_ARRAY, deserializedDocument);
        }
    }

    private static List<RawBsonArray> createRawBsonArrayVariants() {
        return Arrays.asList(
                createRawBsonArrayFromBsonArray(),
                createRawBsonArrayFromByteArray(),
                createRawBsonArrayFromByteArrayOffsetLength()
        );
    }

    private static RawBsonArray createRawBsonArrayFromBsonArray() {
        return (RawBsonArray) new RawBsonDocument(new BsonDocument("a", BSON_ARRAY), new BsonDocumentCodec()).get("a");
    }

    private static byte[] getBytesFromBsonArray() {
        ByteBuf byteBuffer = createRawBsonArrayFromBsonArray().getByteBuffer();
        byte[] strippedBytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(strippedBytes);
        return strippedBytes;
    }

    private static RawBsonArray createRawBsonArrayFromByteArray() {
        return new RawBsonArray(getBytesFromBsonArray());
    }

    private static RawBsonArray createRawBsonArrayFromByteArrayOffsetLength() {
        byte[] strippedBytes = getBytesFromBsonArray();
        byte[] unstrippedBytes = new byte[strippedBytes.length + 2];
        System.arraycopy(strippedBytes, 0, unstrippedBytes, 1, strippedBytes.length);
        return new RawBsonArray(unstrippedBytes, 1, strippedBytes.length);
    }
}
