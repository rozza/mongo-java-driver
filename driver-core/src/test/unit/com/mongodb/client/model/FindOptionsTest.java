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

import com.mongodb.CursorType;
import com.mongodb.internal.client.model.FindOptions;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class FindOptionsTest {

    @Test
    void shouldHaveTheExpectedDefaults() {
        FindOptions options = new FindOptions();

        assertNull(options.getCollation());
        assertEquals(0, options.getMaxTime(MILLISECONDS));
        assertEquals(0, options.getMaxAwaitTime(MILLISECONDS));
        assertNull(options.getProjection());
        assertNull(options.getSort());
        assertNull(options.getHint());
        assertNull(options.getHintString());
        assertEquals(0, options.getLimit());
        assertEquals(0, options.getSkip());
        assertEquals(0, options.getBatchSize());
        assertEquals(CursorType.NonTailable, options.getCursorType());
        assertFalse(options.isNoCursorTimeout());
        assertFalse(options.isPartial());
        assertNull(options.isAllowDiskUse());
    }

    @Test
    void shouldSetCollation() {
        assertNull(new FindOptions().collation(null).getCollation());
        assertEquals(Collation.builder().locale("en").build(),
                new FindOptions().collation(Collation.builder().locale("en").build()).getCollation());
    }

    @Test
    void shouldSetProjection() {
        assertNull(new FindOptions().projection(null).getProjection());
        assertEquals(BsonDocument.parse("{a: 1}"), new FindOptions().projection(BsonDocument.parse("{a: 1}")).getProjection());
    }

    @Test
    void shouldSetSort() {
        assertNull(new FindOptions().sort(null).getSort());
        assertEquals(BsonDocument.parse("{a: 1}"), new FindOptions().sort(BsonDocument.parse("{a: 1}")).getSort());
    }

    @Test
    void shouldSetLimit() {
        assertEquals(-1, new FindOptions().limit(-1).getLimit());
        assertEquals(0, new FindOptions().limit(0).getLimit());
        assertEquals(1, new FindOptions().limit(1).getLimit());
    }

    @Test
    void shouldSetSkip() {
        assertEquals(-1, new FindOptions().skip(-1).getSkip());
        assertEquals(0, new FindOptions().skip(0).getSkip());
        assertEquals(1, new FindOptions().skip(1).getSkip());
    }

    @Test
    void shouldSetBatchSize() {
        assertEquals(-1, new FindOptions().batchSize(-1).getBatchSize());
        assertEquals(0, new FindOptions().batchSize(0).getBatchSize());
        assertEquals(1, new FindOptions().batchSize(1).getBatchSize());
    }

    @Test
    void shouldSetCursorType() {
        assertEquals(CursorType.NonTailable, new FindOptions().cursorType(CursorType.NonTailable).getCursorType());
        assertEquals(CursorType.TailableAwait, new FindOptions().cursorType(CursorType.TailableAwait).getCursorType());
        assertEquals(CursorType.Tailable, new FindOptions().cursorType(CursorType.Tailable).getCursorType());
    }

    @Test
    void shouldSetPartial() {
        assertEquals(true, new FindOptions().partial(true).isPartial());
        assertEquals(false, new FindOptions().partial(false).isPartial());
    }

    @Test
    void shouldSetNoCursorTimeout() {
        assertEquals(true, new FindOptions().noCursorTimeout(true).isNoCursorTimeout());
        assertEquals(false, new FindOptions().noCursorTimeout(false).isNoCursorTimeout());
    }

    @Test
    void shouldConvertMaxTime() {
        FindOptions options = new FindOptions();
        assertEquals(0, options.getMaxTime(SECONDS));

        options.maxTime(100, MILLISECONDS);
        assertEquals(100, options.getMaxTime(MILLISECONDS));

        options.maxTime(1004, MILLISECONDS);
        assertEquals(1, options.getMaxTime(SECONDS));
    }

    @Test
    void shouldConvertMaxAwaitTime() {
        FindOptions options = new FindOptions();
        assertEquals(0, options.getMaxAwaitTime(SECONDS));

        options.maxAwaitTime(100, MILLISECONDS);
        assertEquals(100, options.getMaxAwaitTime(MILLISECONDS));

        options.maxAwaitTime(1004, MILLISECONDS);
        assertEquals(1, options.getMaxAwaitTime(SECONDS));
    }

    @Test
    void shouldSetHint() {
        assertNull(new FindOptions().hint(null).getHint());
        assertEquals(new BsonDocument(), new FindOptions().hint(new BsonDocument()).getHint());
        assertEquals(new Document("a", 1), new FindOptions().hint(new Document("a", 1)).getHint());
    }

    @Test
    void shouldSetHintString() {
        assertNull(new FindOptions().hintString(null).getHintString());
        assertEquals("a_1", new FindOptions().hintString("a_1").getHintString());
    }

    @Test
    void shouldSetAllowDiskUse() {
        assertEquals(true, new FindOptions().allowDiskUse(true).isAllowDiskUse());
        assertEquals(false, new FindOptions().allowDiskUse(false).isAllowDiskUse());
    }
}
