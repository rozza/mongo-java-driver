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

import com.mongodb.BasicDBObject;
import com.mongodb.DefaultDBEncoder;
import com.mongodb.WriteConcern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DBCollectionUpdateOptionsTest {

    @Test
    @DisplayName("should have the expected default values")
    void shouldHaveTheExpectedDefaultValues() {
        DBCollectionUpdateOptions options = new DBCollectionUpdateOptions();

        assertFalse(options.isMulti());
        assertFalse(options.isUpsert());
        assertNull(options.getArrayFilters());
        assertNull(options.getBypassDocumentValidation());
        assertNull(options.getEncoder());
        assertNull(options.getWriteConcern());
    }

    @Test
    @DisplayName("should set and return the expected values")
    void shouldSetAndReturnTheExpectedValues() {
        WriteConcern writeConcern = WriteConcern.MAJORITY;
        DefaultDBEncoder encoder = new DefaultDBEncoder();
        List<BasicDBObject> arrayFilters = Collections.singletonList(new BasicDBObject("i.b", 1));

        DBCollectionUpdateOptions options = new DBCollectionUpdateOptions()
                .bypassDocumentValidation(true)
                .encoder(encoder)
                .multi(true)
                .upsert(true)
                .arrayFilters(arrayFilters)
                .writeConcern(writeConcern);

        assertTrue(options.getBypassDocumentValidation());
        assertEquals(encoder, options.getEncoder());
        assertEquals(writeConcern, options.getWriteConcern());
        assertTrue(options.isMulti());
        assertTrue(options.isUpsert());
        assertEquals(arrayFilters, options.getArrayFilters());
    }
}
