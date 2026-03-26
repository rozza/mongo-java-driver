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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InsertManyOptionsTest {

    @Test
    void shouldHaveTheExpectedDefaults() {
        InsertManyOptions options = new InsertManyOptions();

        assertTrue(options.isOrdered());
        assertNull(options.getBypassDocumentValidation());
    }

    @Test
    void shouldSetOrdered() {
        assertEquals(true, new InsertManyOptions().ordered(true).isOrdered());
        assertEquals(false, new InsertManyOptions().ordered(false).isOrdered());
    }

    @Test
    void shouldSetBypassDocumentValidation() {
        assertNull(new InsertManyOptions().bypassDocumentValidation(null).getBypassDocumentValidation());
        assertEquals(true, new InsertManyOptions().bypassDocumentValidation(true).getBypassDocumentValidation());
        assertEquals(false, new InsertManyOptions().bypassDocumentValidation(false).getBypassDocumentValidation());
    }
}
