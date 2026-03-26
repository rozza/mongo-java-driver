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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DeleteOptionsTest {

    @Test
    void shouldHaveTheExpectedDefaults() {
        DeleteOptions options = new DeleteOptions();

        assertNull(options.getCollation());
        assertNull(options.getHint());
        assertNull(options.getHintString());
    }

    @Test
    void shouldSetCollation() {
        assertNull(new DeleteOptions().collation(null).getCollation());
        assertEquals(Collation.builder().locale("en").build(),
                new DeleteOptions().collation(Collation.builder().locale("en").build()).getCollation());
    }

    @Test
    void shouldSetHint() {
        assertNull(new DeleteOptions().hint(null).getHint());
        assertEquals(new BsonDocument(), new DeleteOptions().hint(new BsonDocument()).getHint());
        assertEquals(new Document("a", 1), new DeleteOptions().hint(new Document("a", 1)).getHint());
    }

    @Test
    void shouldSetHintString() {
        assertNull(new DeleteOptions().hintString(null).getHintString());
        assertEquals("a_1", new DeleteOptions().hintString("a_1").getHintString());
    }
}
