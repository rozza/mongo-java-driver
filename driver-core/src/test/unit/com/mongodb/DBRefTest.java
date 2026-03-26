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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class DBRefTest {

    @Test
    @DisplayName("should set properties")
    void shouldSetProperties() {
        DBRef referenceA = new DBRef("foo.bar", 5);
        DBRef referenceB = new DBRef("mydb", "foo.bar", 5);
        DBRef referenceC = new DBRef(null, "foo.bar", 5);

        assertNull(referenceA.getDatabaseName());
        assertEquals("foo.bar", referenceA.getCollectionName());
        assertEquals(5, referenceA.getId());
        assertEquals("mydb", referenceB.getDatabaseName());
        assertEquals("foo.bar", referenceB.getCollectionName());
        assertEquals(5, referenceB.getId());
        assertNull(referenceC.getDatabaseName());
        assertEquals("foo.bar", referenceC.getCollectionName());
        assertEquals(5, referenceC.getId());
    }

    @Test
    @DisplayName("constructor should throw if collection name is null")
    void constructorShouldThrowIfCollectionNameIsNull() {
        assertThrows(IllegalArgumentException.class, () -> new DBRef(null, 5));
    }

    @Test
    @DisplayName("constructor should throw if id is null")
    void constructorShouldThrowIfIdIsNull() {
        assertThrows(IllegalArgumentException.class, () -> new DBRef("foo.bar", null));
    }

    @Test
    @DisplayName("equivalent instances should be equal and have the same hash code")
    void equivalentInstancesShouldBeEqual() {
        DBRef referenceA = new DBRef("foo.bar", 4);
        DBRef referenceB = new DBRef("foo.bar", 4);
        DBRef referenceC = new DBRef("mydb", "foo.bar", 4);
        DBRef referenceD = new DBRef("mydb", "foo.bar", 4);

        assertEquals(referenceA, referenceA);
        assertEquals(referenceA, referenceB);
        assertEquals(referenceC, referenceD);
        assertEquals(referenceA.hashCode(), referenceB.hashCode());
        assertEquals(referenceC.hashCode(), referenceD.hashCode());
    }

    @Test
    @DisplayName("non-equivalent instances should not be equal and have different hash codes")
    void nonEquivalentInstancesShouldNotBeEqual() {
        DBRef referenceA = new DBRef("foo.bar", 4);
        DBRef referenceB = new DBRef("foo.baz", 4);
        DBRef referenceC = new DBRef("foo.bar", 5);
        DBRef referenceD = new DBRef("mydb", "foo.bar", 4);
        DBRef referenceE = new DBRef("yourdb", "foo.bar", 4);

        assertFalse(referenceA.equals(null));
        assertFalse(referenceA.equals("some other class instance"));
        assertNotEquals(referenceA, referenceB);
        assertNotEquals(referenceA.hashCode(), referenceB.hashCode());
        assertNotEquals(referenceA, referenceC);
        assertNotEquals(referenceA.hashCode(), referenceC.hashCode());
        assertNotEquals(referenceA, referenceD);
        assertNotEquals(referenceA.hashCode(), referenceD.hashCode());
        assertNotEquals(referenceD, referenceE);
        assertNotEquals(referenceD.hashCode(), referenceE.hashCode());
    }

    @Test
    @DisplayName("should stringify")
    void shouldStringify() {
        assertEquals("{ \"$ref\" : \"foo.bar\", \"$id\" : \"4\" }", new DBRef("foo.bar", 4).toString());
        assertEquals("{ \"$ref\" : \"foo.bar\", \"$id\" : \"4\", \"$db\" : \"mydb\" }",
                new DBRef("mydb", "foo.bar", 4).toString());
    }

    @Test
    @DisplayName("testSerialization")
    void testSerialization() throws Exception {
        DBRef originalDBRef = new DBRef("col", 42);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(originalDBRef);
        ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
        DBRef deserializedDBRef = (DBRef) objectInputStream.readObject();

        assertEquals(originalDBRef, deserializedDBRef);
    }
}
