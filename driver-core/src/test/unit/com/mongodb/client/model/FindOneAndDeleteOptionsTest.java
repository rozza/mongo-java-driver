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

class FindOneAndDeleteOptionsTest {

    @Test
    void shouldHaveTheExpectedDefaults() {
        FindOneAndDeleteOptions options = new FindOneAndDeleteOptions();

        assertNull(options.getCollation());
        assertEquals(0, options.getMaxTime(MILLISECONDS));
        assertNull(options.getProjection());
        assertNull(options.getSort());
        assertNull(options.getHint());
        assertNull(options.getHintString());
    }

    @Test
    void shouldSetCollation() {
        assertNull(new FindOneAndDeleteOptions().collation(null).getCollation());
        assertEquals(Collation.builder().locale("en").build(),
                new FindOneAndDeleteOptions().collation(Collation.builder().locale("en").build()).getCollation());
    }

    @Test
    void shouldSetProjection() {
        assertNull(new FindOneAndDeleteOptions().projection(null).getProjection());
        assertEquals(BsonDocument.parse("{ a: 1}"),
                new FindOneAndDeleteOptions().projection(BsonDocument.parse("{ a: 1}")).getProjection());
    }

    @Test
    void shouldSetSort() {
        assertNull(new FindOneAndDeleteOptions().sort(null).getSort());
        assertEquals(BsonDocument.parse("{ a: 1}"),
                new FindOneAndDeleteOptions().sort(BsonDocument.parse("{ a: 1}")).getSort());
    }

    @Test
    void shouldConvertMaxTime() {
        FindOneAndDeleteOptions options = new FindOneAndDeleteOptions();
        assertEquals(0, options.getMaxTime(SECONDS));

        options.maxTime(100, MILLISECONDS);
        assertEquals(100, options.getMaxTime(MILLISECONDS));

        options.maxTime(1004, MILLISECONDS);
        assertEquals(1, options.getMaxTime(SECONDS));
    }

    @Test
    void shouldSetHint() {
        assertNull(new FindOneAndDeleteOptions().hint(null).getHint());
        assertEquals(new BsonDocument(), new FindOneAndDeleteOptions().hint(new BsonDocument()).getHint());
        assertEquals(new Document("a", 1), new FindOneAndDeleteOptions().hint(new Document("a", 1)).getHint());
    }

    @Test
    void shouldSetHintString() {
        assertNull(new FindOneAndDeleteOptions().hintString(null).getHintString());
        assertEquals("a_1", new FindOneAndDeleteOptions().hintString("a_1").getHintString());
    }
}
