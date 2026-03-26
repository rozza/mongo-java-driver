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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DBCollectionDistinctOptionsTest {

    @Test
    @DisplayName("should have the expected default values")
    void shouldHaveTheExpectedDefaultValues() {
        DBCollectionDistinctOptions options = new DBCollectionDistinctOptions();

        assertNull(options.getCollation());
        assertNull(options.getFilter());
        assertNull(options.getReadConcern());
        assertNull(options.getReadPreference());
    }

    @Test
    @DisplayName("should set and return the expected values")
    void shouldSetAndReturnTheExpectedValues() {
        Collation collation = Collation.builder().locale("en").build();
        ReadConcern readConcern = ReadConcern.LOCAL;
        ReadPreference readPreference = ReadPreference.nearest();
        BasicDBObject filter = BasicDBObject.parse("{a: 1}");

        DBCollectionDistinctOptions options = new DBCollectionDistinctOptions()
                .collation(collation)
                .filter(filter)
                .readConcern(readConcern)
                .readPreference(readPreference);

        assertEquals(collation, options.getCollation());
        assertEquals(filter, options.getFilter());
        assertEquals(readConcern, options.getReadConcern());
        assertEquals(readPreference, options.getReadPreference());
    }
}
