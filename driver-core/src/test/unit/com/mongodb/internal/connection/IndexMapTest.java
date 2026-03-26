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

package com.mongodb.internal.connection;

import com.mongodb.MongoInternalException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IndexMapTest {

    @Test
    void shouldMapContiguousIndexes() {
        IndexMap indexMap = IndexMap.create();
        indexMap = indexMap.add(0, 1);
        indexMap = indexMap.add(1, 2);

        assertEquals(1, indexMap.map(0));
        assertEquals(2, indexMap.map(1));
    }

    @Test
    void shouldMapNonContiguousIndexes() {
        IndexMap indexMap = IndexMap.create();
        indexMap = indexMap.add(0, 1);
        indexMap = indexMap.add(1, 2);
        indexMap = indexMap.add(2, 5);

        assertEquals(1, indexMap.map(0));
        assertEquals(2, indexMap.map(1));
        assertEquals(5, indexMap.map(2));
    }

    static Stream<IndexMap> unmappedIndexProvider() {
        return Stream.of(
                IndexMap.create().add(0, 1),
                IndexMap.create(1000, 3).add(5, 1005)
        );
    }

    @ParameterizedTest
    @MethodSource("unmappedIndexProvider")
    void shouldThrowOnUnmappedIndex(IndexMap indexMap) {
        assertThrows(MongoInternalException.class, () -> indexMap.map(-1));
        assertThrows(MongoInternalException.class, () -> indexMap.map(4));
    }

    @Test
    void shouldMapIndexesWhenCountIsProvidedUpFront() {
        IndexMap indexMap = IndexMap.create(1, 2);

        assertEquals(1, indexMap.map(0));
        assertEquals(2, indexMap.map(1));
    }

    @Test
    void shouldIncludeRangesWhenConvertingFromRangeBasedToHashBasedIndexMap() {
        IndexMap indexMap = IndexMap.create(1000, 3);
        indexMap = indexMap.add(5, 1005);

        assertEquals(1000, indexMap.map(0));
        assertEquals(1001, indexMap.map(1));
        assertEquals(1002, indexMap.map(2));
        assertEquals(1005, indexMap.map(5));
    }

    @Test
    void shouldNotAllowNegativeStartIndexOrCount() {
        assertThrows(IllegalArgumentException.class, () -> IndexMap.create(-1, 10));
        assertThrows(IllegalArgumentException.class, () -> IndexMap.create(1, -10));
    }
}
