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

package com.mongodb.connection;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerVersionTest {

    @Test
    void shouldDefaultToVersionZero() {
        ServerVersion version = new ServerVersion();
        assertEquals(Arrays.asList(0, 0, 0), version.getVersionList());
    }

    @Test
    void shouldNotAcceptNullVersionArray() {
        assertThrows(IllegalArgumentException.class, () -> new ServerVersion(null));
    }

    @Test
    void shouldNotAcceptVersionArrayOfLengthUnequalToThree() {
        assertThrows(IllegalStateException.class, () -> new ServerVersion(Arrays.asList(2, 5, 1, 0)));
        assertThrows(IllegalStateException.class, () -> new ServerVersion(Arrays.asList(2, 5)));
    }

    @Test
    void shouldHaveSameVersionArrayAsWhenConstructed() {
        ServerVersion version = new ServerVersion(Arrays.asList(3, 4, 1));
        assertEquals(Arrays.asList(3, 4, 1), version.getVersionList());
    }

    @Test
    void shouldHaveImmutableVersionArray() {
        ServerVersion version = new ServerVersion(Arrays.asList(3, 4, 1));
        assertThrows(UnsupportedOperationException.class, () -> version.getVersionList().set(0, 1));
    }

    @Test
    void identicalVersionsShouldBeEqual() {
        ServerVersion version = new ServerVersion(Arrays.asList(3, 4, 1));
        assertEquals(version, new ServerVersion(Arrays.asList(3, 4, 1)));
    }

    @Test
    void identicalVersionsShouldHaveSameHashCode() {
        ServerVersion version = new ServerVersion(Arrays.asList(3, 4, 1));
        assertEquals(version.hashCode(), new ServerVersion(Arrays.asList(3, 4, 1)).hashCode());
    }

    @Test
    void differentVersionsShouldNotBeEqual() {
        ServerVersion version = new ServerVersion(Arrays.asList(3, 4, 1));
        assertNotEquals(version, new ServerVersion(Arrays.asList(2, 5, 1)));
    }

    @Test
    void lowerVersionShouldCompareLessThan() {
        assertTrue(new ServerVersion(Arrays.asList(1, 5, 1)).compareTo(new ServerVersion(Arrays.asList(2, 5, 1))) < 0);
        assertTrue(new ServerVersion(Arrays.asList(2, 3, 1)).compareTo(new ServerVersion(Arrays.asList(2, 5, 1))) < 0);
        assertTrue(new ServerVersion(Arrays.asList(2, 5, 0)).compareTo(new ServerVersion(Arrays.asList(2, 5, 1))) < 0);
    }

    @Test
    void higherVersionShouldCompareGreaterThan() {
        assertTrue(new ServerVersion(Arrays.asList(3, 6, 0)).compareTo(new ServerVersion(Arrays.asList(3, 4, 1))) > 0);
        assertTrue(new ServerVersion(Arrays.asList(3, 5, 1)).compareTo(new ServerVersion(Arrays.asList(3, 4, 1))) > 0);
        assertTrue(new ServerVersion(Arrays.asList(3, 4, 2)).compareTo(new ServerVersion(Arrays.asList(3, 4, 1))) > 0);
    }

    @Test
    void sameVersionShouldCompareEqual() {
        assertEquals(0, new ServerVersion(Arrays.asList(3, 4, 1)).compareTo(new ServerVersion(Arrays.asList(3, 4, 1))));
    }
}
