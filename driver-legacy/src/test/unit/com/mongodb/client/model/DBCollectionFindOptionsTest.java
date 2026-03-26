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
import com.mongodb.CursorType;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DBCollectionFindOptionsTest {

    @Test
    @DisplayName("should have the expected default values")
    void shouldHaveTheExpectedDefaultValues() {
        DBCollectionFindOptions options = new DBCollectionFindOptions();

        assertFalse(options.isNoCursorTimeout());
        assertFalse(options.isPartial());
        assertEquals(0, options.getBatchSize());
        assertNull(options.getCollation());
        assertEquals(CursorType.NonTailable, options.getCursorType());
        assertEquals(0, options.getLimit());
        assertEquals(0, options.getMaxAwaitTime(TimeUnit.MILLISECONDS));
        assertEquals(0, options.getMaxTime(TimeUnit.MILLISECONDS));
        assertNull(options.getProjection());
        assertNull(options.getReadConcern());
        assertNull(options.getReadPreference());
        assertEquals(0, options.getSkip());
        assertNull(options.getSort());
        assertNull(options.getComment());
        assertNull(options.getHint());
        assertNull(options.getHintString());
        assertNull(options.getMax());
        assertNull(options.getMin());
        assertFalse(options.isReturnKey());
        assertFalse(options.isShowRecordId());
    }

    @Test
    @DisplayName("should set and return the expected values")
    void shouldSetAndReturnTheExpectedValues() {
        Collation collation = Collation.builder().locale("en").build();
        BasicDBObject projection = BasicDBObject.parse("{a: 1, _id: 0}");
        BasicDBObject sort = BasicDBObject.parse("{a: 1}");
        CursorType cursorType = CursorType.TailableAwait;
        ReadConcern readConcern = ReadConcern.LOCAL;
        ReadPreference readPreference = ReadPreference.nearest();
        String comment = "comment";
        BasicDBObject hint = BasicDBObject.parse("{x : 1}");
        String hintString = "a_1";
        BasicDBObject min = BasicDBObject.parse("{y : 1}");
        BasicDBObject max = BasicDBObject.parse("{y : 100}");

        DBCollectionFindOptions options = new DBCollectionFindOptions()
                .batchSize(1)
                .collation(collation)
                .cursorType(cursorType)
                .limit(1)
                .maxAwaitTime(1, TimeUnit.MILLISECONDS)
                .maxTime(1, TimeUnit.MILLISECONDS)
                .noCursorTimeout(true)
                .partial(true)
                .projection(projection)
                .readConcern(readConcern)
                .readPreference(readPreference)
                .skip(1)
                .sort(sort)
                .comment(comment)
                .hint(hint)
                .hintString(hintString)
                .max(max)
                .min(min)
                .returnKey(true)
                .showRecordId(true);

        assertEquals(1, options.getBatchSize());
        assertEquals(collation, options.getCollation());
        assertEquals(cursorType, options.getCursorType());
        assertEquals(1, options.getLimit());
        assertEquals(1, options.getMaxAwaitTime(TimeUnit.MILLISECONDS));
        assertEquals(1, options.getMaxTime(TimeUnit.MILLISECONDS));
        assertEquals(projection, options.getProjection());
        assertEquals(readConcern, options.getReadConcern());
        assertEquals(readPreference, options.getReadPreference());
        assertEquals(1, options.getSkip());
        assertEquals(sort, options.getSort());
        assertTrue(options.isNoCursorTimeout());
        assertTrue(options.isPartial());
        assertEquals(comment, options.getComment());
        assertEquals(hint, options.getHint());
        assertEquals(hintString, options.getHintString());
        assertEquals(max, options.getMax());
        assertEquals(min, options.getMin());
        assertTrue(options.isReturnKey());
        assertTrue(options.isShowRecordId());
    }

    @Test
    @DisplayName("it should copy and return the expected values")
    void itShouldCopyAndReturnTheExpectedValues() {
        Collation collation = Collation.builder().locale("en").build();
        BasicDBObject projection = BasicDBObject.parse("{a: 1, _id: 0}");
        BasicDBObject sort = BasicDBObject.parse("{a: 1}");
        CursorType cursorType = CursorType.TailableAwait;
        ReadConcern readConcern = ReadConcern.LOCAL;
        ReadPreference readPreference = ReadPreference.nearest();
        String comment = "comment";
        BasicDBObject hint = BasicDBObject.parse("{x : 1}");
        String hintString = "a_1";
        BasicDBObject min = BasicDBObject.parse("{y : 1}");
        BasicDBObject max = BasicDBObject.parse("{y : 100}");

        DBCollectionFindOptions original = new DBCollectionFindOptions()
                .batchSize(1)
                .collation(collation)
                .cursorType(cursorType)
                .limit(1)
                .maxAwaitTime(1, TimeUnit.MILLISECONDS)
                .maxTime(1, TimeUnit.MILLISECONDS)
                .noCursorTimeout(true)
                .partial(true)
                .projection(projection)
                .readConcern(readConcern)
                .readPreference(readPreference)
                .skip(1)
                .sort(sort)
                .comment(comment)
                .hint(hint)
                .hintString(hintString)
                .max(max)
                .min(min)
                .returnKey(true)
                .showRecordId(true);

        DBCollectionFindOptions options = original.copy();

        assertNotSame(original, options);

        assertEquals(1, options.getBatchSize());
        assertEquals(collation, options.getCollation());
        assertEquals(cursorType, options.getCursorType());
        assertEquals(1, options.getLimit());
        assertEquals(1, options.getMaxAwaitTime(TimeUnit.MILLISECONDS));
        assertEquals(1, options.getMaxTime(TimeUnit.MILLISECONDS));
        assertEquals(projection, options.getProjection());
        assertEquals(readConcern, options.getReadConcern());
        assertEquals(readPreference, options.getReadPreference());
        assertEquals(1, options.getSkip());
        assertEquals(sort, options.getSort());
        assertTrue(options.isNoCursorTimeout());
        assertTrue(options.isPartial());
        assertEquals(comment, options.getComment());
        assertEquals(hint, options.getHint());
        assertEquals(hintString, options.getHintString());
        assertEquals(max, options.getMax());
        assertEquals(min, options.getMin());
        assertTrue(options.isReturnKey());
        assertTrue(options.isShowRecordId());
    }
}
