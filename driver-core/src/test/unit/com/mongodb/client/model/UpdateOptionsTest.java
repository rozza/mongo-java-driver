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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class UpdateOptionsTest {

    @Test
    void shouldHaveTheExpectedDefaults() {
        UpdateOptions options = new UpdateOptions();

        assertFalse(options.isUpsert());
        assertNull(options.getBypassDocumentValidation());
        assertNull(options.getCollation());
    }

    @Test
    void shouldSetUpsert() {
        assertEquals(true, new UpdateOptions().upsert(true).isUpsert());
        assertEquals(false, new UpdateOptions().upsert(false).isUpsert());
    }

    @Test
    void shouldSetBypassDocumentValidation() {
        assertNull(new UpdateOptions().bypassDocumentValidation(null).getBypassDocumentValidation());
        assertEquals(true, new UpdateOptions().bypassDocumentValidation(true).getBypassDocumentValidation());
        assertEquals(false, new UpdateOptions().bypassDocumentValidation(false).getBypassDocumentValidation());
    }

    @Test
    void shouldSetCollation() {
        assertNull(new UpdateOptions().collation(null).getCollation());
        assertEquals(Collation.builder().locale("en").build(),
                new UpdateOptions().collation(Collation.builder().locale("en").build()).getCollation());
    }

    @Test
    void shouldSetArrayFilters() {
        assertNull(new UpdateOptions().arrayFilters(null).getArrayFilters());
        assertEquals(Collections.emptyList(), new UpdateOptions().arrayFilters(Collections.emptyList()).getArrayFilters());
        assertEquals(Arrays.asList(new BsonDocument("a.b", new BsonInt32(1))),
                new UpdateOptions().arrayFilters(Arrays.asList(new BsonDocument("a.b", new BsonInt32(1)))).getArrayFilters());
    }

    @Test
    void shouldSetHint() {
        assertNull(new UpdateOptions().hint(null).getHint());
        assertEquals(new BsonDocument("_id", new BsonInt32(1)),
                new UpdateOptions().hint(new BsonDocument("_id", new BsonInt32(1))).getHint());
    }

    @Test
    void shouldSetHintString() {
        assertNull(new UpdateOptions().hintString(null).getHintString());
        assertEquals("_id_", new UpdateOptions().hintString("_id_").getHintString());
    }

    @Test
    void shouldSetSort() {
        assertNull(new UpdateOptions().sort(null).getSort());
        assertEquals(new BsonDocument("_id", new BsonInt32(1)),
                new UpdateOptions().sort(new BsonDocument("_id", new BsonInt32(1))).getSort());
    }
}
