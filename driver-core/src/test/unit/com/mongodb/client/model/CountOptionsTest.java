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

import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class CountOptionsTest {

    @Test
    void shouldHaveTheExpectedDefaults() {
        CountOptions options = new CountOptions();

        assertNull(options.getCollation());
        assertNull(options.getHint());
        assertNull(options.getHintString());
        assertEquals(0, options.getLimit());
        assertEquals(0, options.getMaxTime(MILLISECONDS));
        assertEquals(0, options.getSkip());
    }

    @Test
    void shouldSetCollation() {
        assertNull(new CountOptions().collation(null).getCollation());
        assertEquals(Collation.builder().locale("en").build(),
                new CountOptions().collation(Collation.builder().locale("en").build()).getCollation());
    }

    @Test
    void shouldSetHint() {
        assertNull(new CountOptions().hint(null).getHint());
        assertEquals(new BsonDocument(), new CountOptions().hint(new BsonDocument()).getHint());
        assertEquals(new Document("a", 1), new CountOptions().hint(new Document("a", 1)).getHint());
    }

    @Test
    void shouldSetHintString() {
        assertNull(new CountOptions().hintString(null).getHintString());
        assertEquals("a_1", new CountOptions().hintString("a_1").getHintString());
    }

    @Test
    void shouldSetLimit() {
        assertEquals(-1, new CountOptions().limit(-1).getLimit());
        assertEquals(0, new CountOptions().limit(0).getLimit());
        assertEquals(1, new CountOptions().limit(1).getLimit());
    }

    @Test
    void shouldSetSkip() {
        assertEquals(-1, new CountOptions().skip(-1).getSkip());
        assertEquals(0, new CountOptions().skip(0).getSkip());
        assertEquals(1, new CountOptions().skip(1).getSkip());
    }

    @Test
    void shouldConvertMaxTime() {
        CountOptions options = new CountOptions();
        assertEquals(0, options.getMaxTime(SECONDS));

        options.maxTime(100, MILLISECONDS);
        assertEquals(100, options.getMaxTime(MILLISECONDS));

        options.maxTime(1004, MILLISECONDS);
        assertEquals(1, options.getMaxTime(SECONDS));
    }
}
