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
import org.bson.BsonInt32;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class FindOneAndReplaceOptionsTest {

    @Test
    void shouldHaveTheExpectedDefaults() {
        FindOneAndReplaceOptions options = new FindOneAndReplaceOptions();

        assertNull(options.getCollation());
        assertEquals(0, options.getMaxTime(MILLISECONDS));
        assertNull(options.getProjection());
        assertNull(options.getSort());
        assertNull(options.getBypassDocumentValidation());
        assertEquals(ReturnDocument.BEFORE, options.getReturnDocument());
        assertFalse(options.isUpsert());
    }

    @Test
    void shouldSetCollation() {
        assertNull(new FindOneAndReplaceOptions().collation(null).getCollation());
        assertEquals(Collation.builder().locale("en").build(),
                new FindOneAndReplaceOptions().collation(Collation.builder().locale("en").build()).getCollation());
    }

    @Test
    void shouldSetProjection() {
        assertNull(new FindOneAndReplaceOptions().projection(null).getProjection());
        assertEquals(BsonDocument.parse("{ a: 1}"),
                new FindOneAndReplaceOptions().projection(BsonDocument.parse("{ a: 1}")).getProjection());
    }

    @Test
    void shouldSetSort() {
        assertNull(new FindOneAndReplaceOptions().sort(null).getSort());
        assertEquals(BsonDocument.parse("{ a: 1}"),
                new FindOneAndReplaceOptions().sort(BsonDocument.parse("{ a: 1}")).getSort());
    }

    @Test
    void shouldConvertMaxTime() {
        FindOneAndReplaceOptions options = new FindOneAndReplaceOptions();
        assertEquals(0, options.getMaxTime(SECONDS));

        options.maxTime(100, MILLISECONDS);
        assertEquals(100, options.getMaxTime(MILLISECONDS));

        options.maxTime(1004, MILLISECONDS);
        assertEquals(1, options.getMaxTime(SECONDS));
    }

    @Test
    void shouldSetUpsert() {
        assertEquals(true, new FindOneAndReplaceOptions().upsert(true).isUpsert());
        assertEquals(false, new FindOneAndReplaceOptions().upsert(false).isUpsert());
    }

    @Test
    void shouldSetBypassDocumentValidation() {
        assertNull(new FindOneAndReplaceOptions().bypassDocumentValidation(null).getBypassDocumentValidation());
        assertEquals(true, new FindOneAndReplaceOptions().bypassDocumentValidation(true).getBypassDocumentValidation());
        assertEquals(false, new FindOneAndReplaceOptions().bypassDocumentValidation(false).getBypassDocumentValidation());
    }

    @Test
    void shouldSetReturnDocument() {
        assertEquals(ReturnDocument.BEFORE, new FindOneAndReplaceOptions().returnDocument(ReturnDocument.BEFORE).getReturnDocument());
        assertEquals(ReturnDocument.AFTER, new FindOneAndReplaceOptions().returnDocument(ReturnDocument.AFTER).getReturnDocument());
    }

    @Test
    void shouldSetHint() {
        assertNull(new FindOneAndReplaceOptions().hint(null).getHint());
        assertEquals(new BsonDocument("_id", new BsonInt32(1)),
                new FindOneAndReplaceOptions().hint(new BsonDocument("_id", new BsonInt32(1))).getHint());
    }

    @Test
    void shouldSetHintString() {
        assertNull(new FindOneAndReplaceOptions().hintString(null).getHintString());
        assertEquals("_id_", new FindOneAndReplaceOptions().hintString("_id_").getHintString());
    }
}
