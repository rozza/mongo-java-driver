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

import org.bson.BSONObject;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BasicBSONListTest {

    @Test
    void shouldSupportIntKeys() {
        BasicBSONList obj = new BasicBSONList();
        obj.put(0, "a");
        obj.put(1, "b");
        obj.put(2, "c");

        BasicBSONList expected = new BasicBSONList();
        expected.add("a");
        expected.add("b");
        expected.add("c");
        assertEquals(expected, obj);
    }

    @Test
    void shouldSupportStringKeysConvertibleToInts() {
        BSONObject obj = new BasicBSONList();
        obj.put("0", "a");
        obj.put("1", "b");
        obj.put("2", "c");

        BasicBSONList expected = new BasicBSONList();
        expected.add("a");
        expected.add("b");
        expected.add("c");
        assertEquals(expected, obj);
    }

    @Test
    void shouldThrowIllegalArgumentExceptionIfPassedInvalidStringKey() {
        BSONObject obj = new BasicBSONList();
        assertThrows(IllegalArgumentException.class, () -> obj.put("ZERO", "a"));
    }

    @Test
    void shouldInsertNullValuesForMissingKeys() {
        BasicBSONList obj = new BasicBSONList();
        obj.put(0, "a");
        obj.put(1, "b");
        obj.put(5, "c");

        BasicBSONList expected = new BasicBSONList();
        expected.add("a");
        expected.add("b");
        expected.add(null);
        expected.add(null);
        expected.add(null);
        expected.add("c");
        assertEquals(expected, obj);
    }

    @Test
    void shouldProvideIterableKeySet() {
        BasicBSONList obj = new BasicBSONList();
        obj.put(0, "a");
        obj.put(1, "b");
        obj.put(5, "c");

        Iterator<String> iter = obj.keySet().iterator();
        assertTrue(iter.hasNext());
        assertEquals("0", iter.next());
        assertTrue(iter.hasNext());
        assertEquals("1", iter.next());
        assertTrue(iter.hasNext());
        assertEquals("2", iter.next());
        assertTrue(iter.hasNext());
        assertEquals("3", iter.next());
        assertTrue(iter.hasNext());
        assertEquals("4", iter.next());
        assertTrue(iter.hasNext());
        assertEquals("5", iter.next());
        assertFalse(iter.hasNext());
    }
}
