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

import java.util.Arrays;
import java.util.Collections;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class FindOneAndUpdateOptionsTest {

    @Test
    void shouldHaveTheExpectedDefaults() {
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();

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
        assertNull(new FindOneAndUpdateOptions().collation(null).getCollation());
        assertEquals(Collation.builder().locale("en").build(),
                new FindOneAndUpdateOptions().collation(Collation.builder().locale("en").build()).getCollation());
    }

    @Test
    void shouldSetProjection() {
        assertNull(new FindOneAndUpdateOptions().projection(null).getProjection());
        assertEquals(BsonDocument.parse("{ a: 1}"),
                new FindOneAndUpdateOptions().projection(BsonDocument.parse("{ a: 1}")).getProjection());
    }

    @Test
    void shouldSetSort() {
        assertNull(new FindOneAndUpdateOptions().sort(null).getSort());
        assertEquals(BsonDocument.parse("{ a: 1}"),
                new FindOneAndUpdateOptions().sort(BsonDocument.parse("{ a: 1}")).getSort());
    }

    @Test
    void shouldConvertMaxTime() {
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        assertEquals(0, options.getMaxTime(SECONDS));

        options.maxTime(100, MILLISECONDS);
        assertEquals(100, options.getMaxTime(MILLISECONDS));

        options.maxTime(1004, MILLISECONDS);
        assertEquals(1, options.getMaxTime(SECONDS));
    }

    @Test
    void shouldSetUpsert() {
        assertEquals(true, new FindOneAndUpdateOptions().upsert(true).isUpsert());
        assertEquals(false, new FindOneAndUpdateOptions().upsert(false).isUpsert());
    }

    @Test
    void shouldSetBypassDocumentValidation() {
        assertNull(new FindOneAndUpdateOptions().bypassDocumentValidation(null).getBypassDocumentValidation());
        assertEquals(true, new FindOneAndUpdateOptions().bypassDocumentValidation(true).getBypassDocumentValidation());
        assertEquals(false, new FindOneAndUpdateOptions().bypassDocumentValidation(false).getBypassDocumentValidation());
    }

    @Test
    void shouldSetReturnDocument() {
        assertEquals(ReturnDocument.BEFORE, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE).getReturnDocument());
        assertEquals(ReturnDocument.AFTER, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).getReturnDocument());
    }

    @Test
    void shouldSetArrayFilters() {
        assertNull(new FindOneAndUpdateOptions().arrayFilters(null).getArrayFilters());
        assertEquals(Collections.emptyList(), new FindOneAndUpdateOptions().arrayFilters(Collections.emptyList()).getArrayFilters());
        assertEquals(Arrays.asList(new BsonDocument("a.b", new BsonInt32(1))),
                new FindOneAndUpdateOptions().arrayFilters(Arrays.asList(new BsonDocument("a.b", new BsonInt32(1)))).getArrayFilters());
    }

    @Test
    void shouldSetHint() {
        assertNull(new FindOneAndUpdateOptions().hint(null).getHint());
        assertEquals(new BsonDocument("_id", new BsonInt32(1)),
                new FindOneAndUpdateOptions().hint(new BsonDocument("_id", new BsonInt32(1))).getHint());
    }

    @Test
    void shouldSetHintString() {
        assertNull(new UpdateOptions().hintString(null).getHintString());
        assertEquals("_id_", new UpdateOptions().hintString("_id_").getHintString());
    }
}
