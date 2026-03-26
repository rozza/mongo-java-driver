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
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DBCollectionCountOptionsTest {

    @Test
    @DisplayName("should have the expected default values")
    void shouldHaveTheExpectedDefaultValues() {
        DBCollectionCountOptions options = new DBCollectionCountOptions();

        assertNull(options.getCollation());
        assertNull(options.getHint());
        assertNull(options.getHintString());
        assertEquals(0, options.getLimit());
        assertEquals(0, options.getMaxTime(TimeUnit.MILLISECONDS));
        assertNull(options.getReadConcern());
        assertNull(options.getReadPreference());
        assertEquals(0, options.getSkip());
    }

    @Test
    @DisplayName("should set and return the expected values")
    void shouldSetAndReturnTheExpectedValues() {
        Collation collation = Collation.builder().locale("en").build();
        ReadConcern readConcern = ReadConcern.LOCAL;
        ReadPreference readPreference = ReadPreference.nearest();
        BasicDBObject hint = BasicDBObject.parse("{a: 1}");
        String hintString = "a_1";

        DBCollectionCountOptions options = new DBCollectionCountOptions()
                .collation(collation)
                .hint(hint)
                .hintString(hintString)
                .limit(1)
                .maxTime(1, TimeUnit.MILLISECONDS)
                .readConcern(readConcern)
                .readPreference(readPreference)
                .skip(1);

        assertEquals(collation, options.getCollation());
        assertEquals(hint, options.getHint());
        assertEquals(hintString, options.getHintString());
        assertEquals(1, options.getLimit());
        assertEquals(1, options.getMaxTime(TimeUnit.MILLISECONDS));
        assertEquals(readConcern, options.getReadConcern());
        assertEquals(readPreference, options.getReadPreference());
        assertEquals(1, options.getSkip());
    }
}
