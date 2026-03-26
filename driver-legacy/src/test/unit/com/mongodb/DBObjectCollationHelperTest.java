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

package com.mongodb;

import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DBObjectCollationHelperTest {

    @ParameterizedTest
    @MethodSource("shouldCreateTheExpectedCollationArgs")
    @DisplayName("should create the expected collation")
    void shouldCreateTheExpectedCollation(final Collation expectedCollation, final String options) {
        assertEquals(expectedCollation,
                DBObjectCollationHelper.createCollationFromOptions(new BasicDBObject("collation", BasicDBObject.parse(options))));
    }

    private static Stream<Object[]> shouldCreateTheExpectedCollationArgs() {
        return Stream.of(
                new Object[]{
                        Collation.builder().locale("en").build(),
                        "{locale: \"en\"}"
                },
                new Object[]{
                        Collation.builder()
                                .locale("en")
                                .caseLevel(true)
                                .collationCaseFirst(CollationCaseFirst.OFF)
                                .collationStrength(CollationStrength.IDENTICAL)
                                .numericOrdering(true)
                                .collationAlternate(CollationAlternate.SHIFTED)
                                .collationMaxVariable(CollationMaxVariable.SPACE)
                                .normalization(true)
                                .backwards(true)
                                .build(),
                        "{locale: \"en\", caseLevel: true, caseFirst: \"off\", strength: 5,"
                                + " numericOrdering: true, alternate: \"shifted\","
                                + " maxVariable: \"space\", normalization: true, backwards: true}"
                }
        );
    }

    @Test
    @DisplayName("should return null if no options are set")
    void shouldReturnNullIfNoOptionsAreSet() {
        assertNull(DBObjectCollationHelper.createCollationFromOptions(new BasicDBObject()));
    }

    @ParameterizedTest
    @MethodSource("invalidCollationOptionsArgs")
    @DisplayName("should throw an exception if the collation options are invalid")
    void shouldThrowAnExceptionIfTheCollationOptionsAreInvalid(final String options) {
        assertThrows(IllegalArgumentException.class,
                () -> DBObjectCollationHelper.createCollationFromOptions(
                        new BasicDBObject("collation", BasicDBObject.parse(options))));
    }

    private static Stream<String> invalidCollationOptionsArgs() {
        return Stream.of(
                "{}",
                "{locale: true}",
                "{ locale: \"en\", caseLevel: \"true\"}",
                "{ locale: \"en\", caseFirst: false}",
                "{ locale: \"en\", strength: true }",
                "{ locale: \"en\", numericOrdering: 1}",
                "{ locale: \"en\", alternate: true}",
                "{ locale: \"en\", maxVariable: true}",
                "{ locale: \"en\", normalization: 1}",
                "{ locale: \"en\", backwards: 1}"
        );
    }
}
