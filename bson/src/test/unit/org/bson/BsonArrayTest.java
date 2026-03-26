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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BsonArrayTest {

    @Test
    @DisplayName("should be array type")
    void shouldBeArrayType() {
        assertEquals(BsonType.ARRAY, new BsonArray().getBsonType());
    }

    @Test
    @DisplayName("should construct empty array")
    void shouldConstructEmptyArray() {
        BsonArray array = new BsonArray();
        assertTrue(array.isEmpty());
        assertEquals(0, array.size());
        assertTrue(array.getValues().isEmpty());
    }

    @Test
    @DisplayName("should construct from a list")
    void shouldConstructFromAList() {
        List<BsonValue> list = new ArrayList<>(Arrays.asList(BsonBoolean.TRUE, BsonBoolean.FALSE));

        BsonArray array = new BsonArray(list);
        assertFalse(array.isEmpty());
        assertEquals(2, array.size());
        assertEquals(list, array.getValues());

        list.remove(BsonBoolean.TRUE);
        assertNotEquals(list, array.getValues());
    }

    @Test
    @DisplayName("should parse json")
    void shouldParseJson() {
        assertEquals(
                new BsonArray(Arrays.asList(new BsonInt32(1), BsonBoolean.TRUE)),
                BsonArray.parse("[1, true]"));
    }
}
