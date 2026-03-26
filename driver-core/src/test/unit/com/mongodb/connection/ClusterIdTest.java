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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ClusterIdTest {

    @Test
    void shouldSetValueToStringWithLength24() {
        assertEquals(24, new ClusterId().getValue().length());
    }

    @Test
    void differentIdsShouldHaveDifferentValues() {
        assertNotEquals(new ClusterId().getValue(), new ClusterId().getValue());
    }

    @ParameterizedTest
    @CsvSource(value = {"id1,", "id2,my server"}, nullValues = {""})
    void equivalentIdsShouldBeEqualAndHaveSameHashCode(String id, String description) {
        ClusterId id1 = new ClusterId(id, description);
        ClusterId id2 = new ClusterId(id, description);

        assertEquals(id1, id2);
        assertEquals(id1.hashCode(), id2.hashCode());
    }
}
