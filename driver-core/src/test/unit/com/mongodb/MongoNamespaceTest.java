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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MongoNamespaceTest {

    @ParameterizedTest
    @DisplayName("invalid database name should throw IllegalArgumentException")
    @NullAndEmptySource
    @ValueSource(strings = {"a\0b", "a b", "a.b", "a/b", "a\\b", "a\"b"})
    void invalidDatabaseNameShouldThrow(final String databaseName) {
        assertThrows(IllegalArgumentException.class, () -> new MongoNamespace(databaseName, "test"));
        assertThrows(IllegalArgumentException.class, () -> MongoNamespace.checkDatabaseNameValidity(databaseName));
    }

    @ParameterizedTest
    @DisplayName("invalid collection name should throw IllegalArgumentException")
    @NullAndEmptySource
    void invalidCollectionNameShouldThrow(final String collectionName) {
        assertThrows(IllegalArgumentException.class, () -> new MongoNamespace("test", collectionName));
        assertThrows(IllegalArgumentException.class, () -> MongoNamespace.checkCollectionNameValidity(collectionName));
    }

    @ParameterizedTest
    @DisplayName("invalid full name should throw IllegalArgumentException")
    @MethodSource("invalidFullNames")
    void invalidFullNameShouldThrow(final String fullName) {
        assertThrows(IllegalArgumentException.class, () -> new MongoNamespace(fullName));
    }

    static Stream<String> invalidFullNames() {
        return Stream.of(null, "", "db", ".db", "db.", "a .b");
    }

    @Test
    @DisplayName("test getters")
    void testGetters() {
        MongoNamespace namespace1 = new MongoNamespace("db", "a.b");
        MongoNamespace namespace2 = new MongoNamespace("db.a.b");

        for (MongoNamespace namespace : new MongoNamespace[]{namespace1, namespace2}) {
            assertEquals("db", namespace.getDatabaseName());
            assertEquals("a.b", namespace.getCollectionName());
            assertEquals("db.a.b", namespace.getFullName());
        }
    }

    @Test
    @DisplayName("testEqualsAndHashCode")
    void testEqualsAndHashCode() {
        MongoNamespace namespace1 = new MongoNamespace("db1", "coll1");
        MongoNamespace namespace2 = new MongoNamespace("db1", "coll1");
        MongoNamespace namespace3 = new MongoNamespace("db2", "coll1");
        MongoNamespace namespace4 = new MongoNamespace("db1", "coll2");

        assertNotEquals(namespace1, new Object());
        assertEquals(namespace1, namespace1);
        assertEquals(namespace1, namespace2);
        assertNotEquals(namespace1, namespace3);
        assertNotEquals(namespace1, namespace4);
        assertEquals(97917362, namespace1.hashCode());
    }
}
