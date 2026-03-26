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

package com.mongodb.client.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FieldSpecificationTest {

    @Test
    void shouldValidateName() {
        assertThrows(IllegalArgumentException.class, () -> new Field<>(null, Arrays.asList(1, 2, 3)));
    }

    @Test
    void shouldAcceptNullValues() {
        Field<Object> field = new Field<>("name", null);
        assertEquals("name", field.getName());
        assertNull(field.getValue());
    }

    @Test
    void shouldAcceptProperties() {
        Field<Object> field = new Field<>("name", Arrays.asList(1, 2, 3));
        assertEquals("name", field.getName());
        assertEquals(Arrays.asList(1, 2, 3), field.getValue());
    }
}
