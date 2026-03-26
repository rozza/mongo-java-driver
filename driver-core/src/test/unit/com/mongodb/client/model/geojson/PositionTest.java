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

package com.mongodb.client.model.geojson;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PositionTest {

    @Test
    void constructorsShouldSetValues() {
        assertEquals(Arrays.asList(1.0d, 2.0d), new Position(Arrays.asList(1.0d, 2.0d)).getValues());
        assertEquals(Arrays.asList(1.0d, 2.0d), new Position(1.0d, 2.0d).getValues());
        assertEquals(Arrays.asList(1.0d, 2.0d, 3.0d), new Position(1.0d, 2.0d, 3.0d).getValues());
        assertEquals(Arrays.asList(1.0d, 2.0d, 3.0d, 4.0d), new Position(1.0d, 2.0d, 3.0d, 4.0d).getValues());
    }

    @Test
    void constructorsShouldSetUnmodifiable() {
        assertThrows(UnsupportedOperationException.class, () -> new Position(Arrays.asList(1.0d, 2.0d)).getValues().set(0, 3.0d));
        assertThrows(UnsupportedOperationException.class, () -> new Position(1.0d, 2.0d).getValues().set(0, 3.0d));
    }

    @Test
    void constructorShouldThrowWhenPreconditionsAreViolated() {
        assertThrows(IllegalArgumentException.class, () -> new Position(null));
        assertThrows(IllegalArgumentException.class, () -> new Position(Arrays.asList(1.0)));
        assertThrows(IllegalArgumentException.class, () -> new Position(Arrays.asList(1.0, null)));
    }

    @Test
    void equalsHashcodeAndToStringShouldBeOverridden() {
        assertEquals(new Position(1.0d, 2.0d), new Position(1.0d, 2.0d));
        assertEquals(new Position(1.0d, 2.0d).hashCode(), new Position(1.0d, 2.0d).hashCode());
        assertEquals("Position{values=[1.0, 2.0]}", new Position(1.0d, 2.0d).toString());
    }
}
