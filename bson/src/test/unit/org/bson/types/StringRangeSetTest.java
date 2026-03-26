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

package org.bson.types;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StringRangeSetTest {

    @Test
    void shouldBeEmptyIfSizeIsZero() {
        StringRangeSet stringSet = new StringRangeSet(0);
        assertEquals(0, stringSet.size());
        assertTrue(stringSet.isEmpty());
    }

    @Test
    void shouldContainAllStringsBetweenZeroAndSize() {
        StringRangeSet stringSet = new StringRangeSet(5);
        assertEquals(5, stringSet.size());
        assertFalse(stringSet.contains("-1"));
        assertTrue(stringSet.contains("0"));
        assertTrue(stringSet.contains("1"));
        assertTrue(stringSet.contains("2"));
        assertTrue(stringSet.contains("3"));
        assertTrue(stringSet.contains("4"));
        assertFalse(stringSet.contains("5"));
        assertTrue(stringSet.containsAll(Arrays.asList("0", "1", "2", "3", "4")));
        assertFalse(stringSet.containsAll(Arrays.asList("0", "1", "2", "3", "4", "5")));
    }

    @Test
    void shouldNotContainIntegers() {
        StringRangeSet stringSet = new StringRangeSet(5);
        assertFalse(stringSet.contains(0));
        assertFalse(stringSet.containsAll(Arrays.asList(0, 1, 2)));
    }

    @Test
    void shouldNotContainStringsThatDoNotParseAsIntegers() {
        StringRangeSet stringSet = new StringRangeSet(5);
        assertFalse(stringSet.contains("foo"));
        assertFalse(stringSet.containsAll(Arrays.asList("foo", "bar", "baz")));
    }

    @Test
    void setShouldBeOrderedStringRepresentationsOfTheRange() {
        int size = 2000;
        List<String> expectedKeys = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            expectedKeys.add(Integer.toString(i));
        }

        StringRangeSet stringSet = new StringRangeSet(size);
        List<String> keys = new ArrayList<>();
        Iterator<String> iter = stringSet.iterator();
        while (iter.hasNext()) {
            keys.add(iter.next());
        }
        assertEquals(expectedKeys, keys);

        assertThrows(NoSuchElementException.class, iter::next);
    }

    @Test
    void shouldConvertToObjectArray() {
        StringRangeSet stringSet = new StringRangeSet(5);
        Object[] array = stringSet.toArray();
        assertArrayEquals(new Object[]{"0", "1", "2", "3", "4"}, array);
    }

    @Test
    void shouldModifyStringArrayThatIsLargeEnough() {
        StringRangeSet stringSet = new StringRangeSet(5);
        String[] stringArray = new String[]{"6", "5", "4", "3", "2", "1", "0"};
        String[] array = stringSet.toArray(stringArray);
        assertArrayEquals(new String[]{"0", "1", "2", "3", "4", null, "0"}, array);
    }

    @Test
    void shouldAllocateStringArrayWhenSpecifiedOneIsTooSmall() {
        StringRangeSet stringSet = new StringRangeSet(5);
        String[] stringArray = new String[]{"3", "2", "1", "0"};
        String[] array = stringSet.toArray(stringArray);
        assertArrayEquals(new String[]{"0", "1", "2", "3", "4"}, array);
    }

    @Test
    void shouldThrowArrayStoreExceptionIfArrayIsOfWrongType() {
        StringRangeSet stringSet = new StringRangeSet(5);
        assertThrows(ArrayStoreException.class, () -> stringSet.toArray(new Integer[5]));
    }

    @Test
    void modifyingOperationsShouldThrowUnsupportedOperationException() {
        StringRangeSet stringSet = new StringRangeSet(5);

        assertThrows(UnsupportedOperationException.class, () -> stringSet.iterator().remove());
        assertThrows(UnsupportedOperationException.class, () -> stringSet.add("1"));
        assertThrows(UnsupportedOperationException.class, () -> stringSet.addAll(Arrays.asList("1", "2")));
        assertThrows(UnsupportedOperationException.class, stringSet::clear);
        assertThrows(UnsupportedOperationException.class, () -> stringSet.remove("1"));
        assertThrows(UnsupportedOperationException.class, () -> stringSet.removeAll(Arrays.asList("0", "1")));
        assertThrows(UnsupportedOperationException.class, () -> stringSet.retainAll(Arrays.asList("0", "1")));
    }
}
