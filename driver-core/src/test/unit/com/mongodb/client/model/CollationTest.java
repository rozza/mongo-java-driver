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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CollationTest {

    @Test
    void shouldHaveNullValuesAsDefault() {
        Collation options = Collation.builder().build();

        assertNull(options.getAlternate());
        assertNull(options.getBackwards());
        assertNull(options.getCaseFirst());
        assertNull(options.getCaseLevel());
        assertNull(options.getLocale());
        assertNull(options.getMaxVariable());
        assertNull(options.getNormalization());
        assertNull(options.getNumericOrdering());
        assertNull(options.getStrength());
    }

    @Test
    void shouldHaveTheSetValuesAsPassedToTheBuilder() {
        Collation options = Collation.builder()
                .locale("en")
                .caseLevel(true)
                .collationCaseFirst(CollationCaseFirst.OFF)
                .collationStrength(CollationStrength.IDENTICAL)
                .numericOrdering(true)
                .collationAlternate(CollationAlternate.SHIFTED)
                .collationMaxVariable(CollationMaxVariable.SPACE)
                .backwards(true)
                .normalization(true)
                .build();

        assertEquals(CollationAlternate.SHIFTED, options.getAlternate());
        assertTrue(options.getBackwards());
        assertEquals(CollationCaseFirst.OFF, options.getCaseFirst());
        assertTrue(options.getCaseLevel());
        assertEquals("en", options.getLocale());
        assertEquals(CollationMaxVariable.SPACE, options.getMaxVariable());
        assertTrue(options.getNormalization());
        assertTrue(options.getNumericOrdering());
        assertEquals(CollationStrength.IDENTICAL, options.getStrength());
    }

    @Test
    void shouldCreateTheExpectedBsonDocumentForEmpty() {
        assertEquals(BsonDocument.parse("{}"), Collation.builder().build().asDocument());
    }

    @Test
    void shouldCreateTheExpectedBsonDocumentForLocaleOnly() {
        assertEquals(BsonDocument.parse("{locale: \"en\"}"),
                Collation.builder().locale("en").build().asDocument());
    }

    @Test
    void shouldCreateTheExpectedBsonDocumentForAllOptions() {
        Collation collation = Collation.builder()
                .locale("en")
                .caseLevel(true)
                .collationCaseFirst(CollationCaseFirst.OFF)
                .collationStrength(CollationStrength.IDENTICAL)
                .numericOrdering(true)
                .collationAlternate(CollationAlternate.SHIFTED)
                .collationMaxVariable(CollationMaxVariable.SPACE)
                .normalization(true)
                .backwards(true)
                .build();

        assertEquals(BsonDocument.parse("{locale: \"en\", caseLevel: true, caseFirst: \"off\", strength: 5,"
                + " numericOrdering: true, alternate: \"shifted\","
                + " maxVariable: \"space\", normalization: true, backwards: true}"),
                collation.asDocument());
    }
}
