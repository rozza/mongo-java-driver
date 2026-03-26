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
import com.mongodb.WriteConcern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DBCollectionFindAndModifyOptionsTest {

    @Test
    @DisplayName("should have the expected default values")
    void shouldHaveTheExpectedDefaultValues() {
        DBCollectionFindAndModifyOptions options = new DBCollectionFindAndModifyOptions();

        assertFalse(options.isRemove());
        assertFalse(options.isUpsert());
        assertFalse(options.returnNew());
        assertNull(options.getBypassDocumentValidation());
        assertNull(options.getCollation());
        assertNull(options.getArrayFilters());
        assertEquals(0, options.getMaxTime(TimeUnit.MILLISECONDS));
        assertNull(options.getProjection());
        assertNull(options.getSort());
        assertNull(options.getUpdate());
        assertNull(options.getWriteConcern());
    }

    @Test
    @DisplayName("should set and return the expected values")
    void shouldSetAndReturnTheExpectedValues() {
        Collation collation = Collation.builder().locale("en").build();
        WriteConcern writeConcern = WriteConcern.MAJORITY;
        BasicDBObject projection = BasicDBObject.parse("{a: 1, _id: 0}");
        BasicDBObject sort = BasicDBObject.parse("{a: 1}");
        BasicDBObject update = BasicDBObject.parse("{$set: {a:  2}}");
        List<BasicDBObject> arrayFilters = Collections.singletonList(new BasicDBObject("i.b", 1));

        DBCollectionFindAndModifyOptions options = new DBCollectionFindAndModifyOptions()
                .bypassDocumentValidation(true)
                .collation(collation)
                .maxTime(1, TimeUnit.MILLISECONDS)
                .projection(projection)
                .remove(true)
                .returnNew(true)
                .sort(sort)
                .update(update)
                .upsert(true)
                .arrayFilters(arrayFilters)
                .writeConcern(writeConcern);

        assertTrue(options.getBypassDocumentValidation());
        assertEquals(collation, options.getCollation());
        assertEquals(1, options.getMaxTime(TimeUnit.MILLISECONDS));
        assertEquals(projection, options.getProjection());
        assertEquals(sort, options.getSort());
        assertEquals(update, options.getUpdate());
        assertEquals(writeConcern, options.getWriteConcern());
        assertTrue(options.isRemove());
        assertTrue(options.isUpsert());
        assertTrue(options.returnNew());
        assertEquals(arrayFilters, options.getArrayFilters());
    }
}
