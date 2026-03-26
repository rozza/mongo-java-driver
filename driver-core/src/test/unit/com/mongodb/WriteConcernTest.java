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

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteConcernTest {

    @ParameterizedTest
    @MethodSource("constructorData")
    @DisplayName("constructors should set up write concern correctly")
    void constructorsShouldSetUpCorrectly(final WriteConcern wc, final Object w, final Integer wTimeout,
                                           final Boolean journal) {
        assertEquals(w, wc.getWObject());
        assertEquals(wTimeout, wc.getWTimeout(MILLISECONDS));
        assertEquals(journal, wc.getJournal());
    }

    static Stream<Object[]> constructorData() {
        return Stream.of(
                new Object[]{new WriteConcern(1), 1, null, null},
                new Object[]{new WriteConcern(1, 10), 1, 10, null},
                new Object[]{WriteConcern.ACKNOWLEDGED.withWTimeout(0, MILLISECONDS).withJournal(false), null, 0, false},
                new Object[]{new WriteConcern("majority"), "majority", null, null}
        );
    }

    @ParameterizedTest
    @MethodSource("journalData")
    @DisplayName("test journal getters")
    void testJournalGetters(final WriteConcern wc, final Boolean journal) {
        assertEquals(journal, wc.getJournal());
    }

    static Stream<Object[]> journalData() {
        return Stream.of(
                new Object[]{WriteConcern.ACKNOWLEDGED, null},
                new Object[]{WriteConcern.ACKNOWLEDGED.withJournal(false), false},
                new Object[]{WriteConcern.ACKNOWLEDGED.withJournal(true), true}
        );
    }

    @ParameterizedTest
    @MethodSource("wTimeoutData")
    @DisplayName("test wTimeout getters")
    void testWTimeoutGetters(final WriteConcern wc, final Integer wTimeout) {
        assertEquals(wTimeout, wc.getWTimeout(MILLISECONDS));
        assertEquals(wTimeout == null ? null : wTimeout * 1000, wc.getWTimeout(MICROSECONDS));
    }

    static Stream<Object[]> wTimeoutData() {
        return Stream.of(
                new Object[]{WriteConcern.ACKNOWLEDGED, null},
                new Object[]{WriteConcern.ACKNOWLEDGED.withWTimeout(1000, MILLISECONDS), 1000}
        );
    }

    @Test
    @DisplayName("test wTimeout getter error conditions")
    void testWTimeoutGetterErrorConditions() {
        assertThrows(IllegalArgumentException.class, () -> WriteConcern.ACKNOWLEDGED.getWTimeout(null));
    }

    @ParameterizedTest
    @MethodSource("wObjectData")
    @DisplayName("test getWObject")
    void testGetWObject(final WriteConcern wc, final Object wObject) {
        assertEquals(wObject, wc.getWObject());
    }

    static Stream<Object[]> wObjectData() {
        return Stream.of(
                new Object[]{WriteConcern.ACKNOWLEDGED, null},
                new Object[]{WriteConcern.W1, 1},
                new Object[]{WriteConcern.MAJORITY, "majority"}
        );
    }

    @Test
    @DisplayName("test getWString")
    void testGetWString() {
        assertEquals("majority", WriteConcern.MAJORITY.getWString());
    }

    @Test
    @DisplayName("test getWString error conditions")
    void testGetWStringErrorConditions() {
        assertThrows(IllegalStateException.class, () -> WriteConcern.ACKNOWLEDGED.getWString());
        assertThrows(IllegalStateException.class, () -> WriteConcern.W1.getWString());
    }

    @Test
    @DisplayName("test getW")
    void testGetW() {
        assertEquals(0, WriteConcern.UNACKNOWLEDGED.getW());
        assertEquals(1, WriteConcern.W1.getW());
    }

    @Test
    @DisplayName("test getW error conditions")
    void testGetWErrorConditions() {
        assertThrows(IllegalStateException.class, () -> WriteConcern.ACKNOWLEDGED.getW());
        assertThrows(IllegalStateException.class, () -> WriteConcern.MAJORITY.getW());
    }

    @Test
    @DisplayName("test withW methods")
    void testWithWMethods() {
        assertEquals(new WriteConcern(1), WriteConcern.UNACKNOWLEDGED.withW(1));
        assertEquals(new WriteConcern("dc1"), WriteConcern.UNACKNOWLEDGED.withW("dc1"));

        assertThrows(IllegalArgumentException.class, () -> WriteConcern.UNACKNOWLEDGED.withW(null));
        assertThrows(IllegalArgumentException.class, () -> WriteConcern.UNACKNOWLEDGED.withW(-1));
        assertThrows(IllegalArgumentException.class, () -> WriteConcern.UNACKNOWLEDGED.withJournal(true));
    }

    @Test
    @DisplayName("test withJournal methods")
    void testWithJournalMethods() {
        assertEquals(WriteConcern.ACKNOWLEDGED.withJournal(true),
                WriteConcern.ACKNOWLEDGED.withJournal(true));
    }

    @Test
    @DisplayName("test withWTimeout methods")
    void testWithWTimeoutMethods() {
        assertEquals(WriteConcern.ACKNOWLEDGED.withWTimeout(0, MILLISECONDS),
                WriteConcern.ACKNOWLEDGED.withWTimeout(0, MILLISECONDS));
        assertEquals(WriteConcern.ACKNOWLEDGED.withWTimeout(1000, MILLISECONDS),
                WriteConcern.ACKNOWLEDGED.withWTimeout(1000, MILLISECONDS));

        assertThrows(IllegalArgumentException.class, () -> WriteConcern.ACKNOWLEDGED.withWTimeout(0, null));
        assertThrows(IllegalArgumentException.class, () -> WriteConcern.ACKNOWLEDGED.withWTimeout(-1, MILLISECONDS));
        assertThrows(IllegalArgumentException.class,
                () -> WriteConcern.ACKNOWLEDGED.withWTimeout((long) Integer.MAX_VALUE + 1, MILLISECONDS));
    }

    @ParameterizedTest
    @MethodSource("asDocumentData")
    @DisplayName("should return write concern document")
    void shouldReturnWriteConcernDocument(final WriteConcern wc, final BsonDocument commandDocument) {
        assertEquals(commandDocument, wc.asDocument());
    }

    static Stream<Object[]> asDocumentData() {
        return Stream.of(
                new Object[]{WriteConcern.UNACKNOWLEDGED, new BsonDocument("w", new BsonInt32(0))},
                new Object[]{WriteConcern.ACKNOWLEDGED, new BsonDocument()},
                new Object[]{WriteConcern.W2, new BsonDocument("w", new BsonInt32(2))},
                new Object[]{WriteConcern.JOURNALED, new BsonDocument("j", BsonBoolean.TRUE)},
                new Object[]{new WriteConcern("majority"), new BsonDocument("w", new BsonString("majority"))},
                new Object[]{new WriteConcern(2, 100),
                        new BsonDocument("w", new BsonInt32(2)).append("wtimeout", new BsonInt32(100))}
        );
    }

    @ParameterizedTest
    @MethodSource("equalsData")
    @DisplayName("test equals")
    void testEquals(final WriteConcern wc, final Object compareTo, final boolean expectedResult) {
        assertEquals(expectedResult, wc.equals(compareTo));
    }

    static Stream<Object[]> equalsData() {
        return Stream.of(
                new Object[]{WriteConcern.ACKNOWLEDGED, WriteConcern.ACKNOWLEDGED, true},
                new Object[]{WriteConcern.ACKNOWLEDGED, null, false},
                new Object[]{WriteConcern.ACKNOWLEDGED, WriteConcern.UNACKNOWLEDGED, false},
                new Object[]{new WriteConcern(1, 0), new WriteConcern(1, 1), false}
        );
    }

    @ParameterizedTest
    @MethodSource("hashCodeData")
    @DisplayName("test hashCode")
    void testHashCode(final WriteConcern wc, final int hashCode) {
        assertEquals(hashCode, wc.hashCode());
    }

    static Stream<Object[]> hashCodeData() {
        return Stream.of(
                new Object[]{WriteConcern.ACKNOWLEDGED, 0},
                new Object[]{WriteConcern.W1, 961},
                new Object[]{WriteConcern.W2, 1922},
                new Object[]{WriteConcern.MAJORITY, -322299115}
        );
    }

    @ParameterizedTest
    @MethodSource("constantsData")
    @DisplayName("test constants")
    void testConstants(final WriteConcern constructedWriteConcern, final WriteConcern constantWriteConcern) {
        assertEquals(constantWriteConcern, constructedWriteConcern);
    }

    static Stream<Object[]> constantsData() {
        return Stream.of(
                new Object[]{WriteConcern.ACKNOWLEDGED, WriteConcern.ACKNOWLEDGED},
                new Object[]{new WriteConcern(1), WriteConcern.W1},
                new Object[]{new WriteConcern(2), WriteConcern.W2},
                new Object[]{new WriteConcern(3), WriteConcern.W3},
                new Object[]{new WriteConcern(0), WriteConcern.UNACKNOWLEDGED},
                new Object[]{WriteConcern.ACKNOWLEDGED.withJournal(true), WriteConcern.JOURNALED},
                new Object[]{new WriteConcern("majority"), WriteConcern.MAJORITY}
        );
    }

    @ParameterizedTest
    @MethodSource("isAcknowledgedData")
    @DisplayName("test isAcknowledged")
    void testIsAcknowledged(final WriteConcern writeConcern, final boolean acknowledged) {
        assertEquals(acknowledged, writeConcern.isAcknowledged());
    }

    static Stream<Object[]> isAcknowledgedData() {
        return Stream.of(
                new Object[]{WriteConcern.ACKNOWLEDGED, true},
                new Object[]{WriteConcern.W1, true},
                new Object[]{WriteConcern.W2, true},
                new Object[]{WriteConcern.W3, true},
                new Object[]{WriteConcern.MAJORITY, true},
                new Object[]{WriteConcern.UNACKNOWLEDGED, false},
                new Object[]{WriteConcern.UNACKNOWLEDGED.withWTimeout(10, MILLISECONDS), false},
                new Object[]{WriteConcern.UNACKNOWLEDGED.withJournal(false), false}
        );
    }

    @ParameterizedTest
    @MethodSource("valueOfData")
    @DisplayName("test value of")
    void testValueOf(final WriteConcern wc, final WriteConcern valueOf) {
        assertEquals(wc, valueOf);
    }

    static Stream<Object[]> valueOfData() {
        return Stream.of(
                new Object[]{WriteConcern.ACKNOWLEDGED, WriteConcern.valueOf("ACKNOWLEDGED")},
                new Object[]{WriteConcern.ACKNOWLEDGED, WriteConcern.valueOf("acknowledged")},
                new Object[]{null, WriteConcern.valueOf("blahblah")}
        );
    }

    @Test
    @DisplayName("write concern should know if it is the server default")
    void writeConcernShouldKnowIfServerDefault() {
        assertTrue(WriteConcern.ACKNOWLEDGED.isServerDefault());
        assertFalse(WriteConcern.UNACKNOWLEDGED.isServerDefault());
        assertFalse(WriteConcern.ACKNOWLEDGED.withJournal(false).isServerDefault());
        assertFalse(WriteConcern.ACKNOWLEDGED.withWTimeout(0, MILLISECONDS).isServerDefault());
    }

    @Test
    @DisplayName("should throw when w is -1")
    void shouldThrowWhenWIsNegative() {
        assertThrows(IllegalArgumentException.class, () -> new WriteConcern(-1));
    }

    @Test
    @DisplayName("should throw when w is null")
    void shouldThrowWhenWIsNull() {
        assertThrows(IllegalArgumentException.class, () -> new WriteConcern((String) null));
    }
}
