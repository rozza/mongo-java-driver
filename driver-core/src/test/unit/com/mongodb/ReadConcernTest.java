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

import org.bson.BsonDocument;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReadConcernTest {

    @ParameterizedTest
    @MethodSource("expectedReadConcernLevels")
    @DisplayName("should have the expected read concern levels")
    void shouldHaveExpectedReadConcernLevels(final ReadConcern staticValue, final ReadConcernLevel expectedLevel,
                                              final ReadConcern expectedReadConcern) {
        assertEquals(expectedReadConcern, staticValue);
        assertEquals(expectedLevel, staticValue.getLevel());
    }

    static Stream<Object[]> expectedReadConcernLevels() {
        return Stream.of(
                new Object[]{ReadConcern.DEFAULT, null, ReadConcern.DEFAULT},
                new Object[]{ReadConcern.LOCAL, ReadConcernLevel.LOCAL, new ReadConcern(ReadConcernLevel.LOCAL)},
                new Object[]{ReadConcern.MAJORITY, ReadConcernLevel.MAJORITY, new ReadConcern(ReadConcernLevel.MAJORITY)},
                new Object[]{ReadConcern.LINEARIZABLE, ReadConcernLevel.LINEARIZABLE,
                        new ReadConcern(ReadConcernLevel.LINEARIZABLE)},
                new Object[]{ReadConcern.SNAPSHOT, ReadConcernLevel.SNAPSHOT, new ReadConcern(ReadConcernLevel.SNAPSHOT)},
                new Object[]{ReadConcern.AVAILABLE, ReadConcernLevel.AVAILABLE, new ReadConcern(ReadConcernLevel.AVAILABLE)}
        );
    }

    @ParameterizedTest
    @MethodSource("expectedDocuments")
    @DisplayName("should create the expected Documents")
    void shouldCreateExpectedDocuments(final ReadConcern staticValue, final BsonDocument expected) {
        assertEquals(expected, staticValue.asDocument());
    }

    static Stream<Object[]> expectedDocuments() {
        return Stream.of(
                new Object[]{ReadConcern.DEFAULT, BsonDocument.parse("{}")},
                new Object[]{ReadConcern.LOCAL, BsonDocument.parse("{level: \"local\"}")},
                new Object[]{ReadConcern.MAJORITY, BsonDocument.parse("{level: \"majority\"}")},
                new Object[]{ReadConcern.LINEARIZABLE, BsonDocument.parse("{level: \"linearizable\"}")},
                new Object[]{ReadConcern.SNAPSHOT, BsonDocument.parse("{level: \"snapshot\"}")},
                new Object[]{ReadConcern.AVAILABLE, BsonDocument.parse("{level: \"available\"}")}
        );
    }

    @ParameterizedTest
    @MethodSource("isServerDefaultData")
    @DisplayName("should have the correct value for isServerDefault")
    void shouldHaveCorrectIsServerDefault(final ReadConcern staticValue, final boolean expected) {
        assertEquals(expected, staticValue.isServerDefault());
    }

    static Stream<Object[]> isServerDefaultData() {
        return Stream.of(
                new Object[]{ReadConcern.DEFAULT, true},
                new Object[]{ReadConcern.LOCAL, false},
                new Object[]{ReadConcern.MAJORITY, false},
                new Object[]{ReadConcern.LINEARIZABLE, false},
                new Object[]{ReadConcern.SNAPSHOT, false},
                new Object[]{ReadConcern.AVAILABLE, false}
        );
    }
}
