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

import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class DocumentToDBRefTransformerTest {
    private final DocumentToDBRefTransformer transformer = new DocumentToDBRefTransformer();

    @Test
    @DisplayName("should not transform a value that is not a Document")
    void shouldNotTransformNonDocument() {
        String str = "some string";
        assertSame(str, transformer.transform(str));
    }

    @Test
    @DisplayName("should not transform a Document that does not have both $ref and $id fields")
    void shouldNotTransformDocumentWithoutRefAndId() {
        Document doc1 = new Document();
        Document doc2 = new Document("foo", "bar");
        Document doc3 = new Document("$ref", "bar");
        Document doc4 = new Document("$id", "bar");

        assertSame(doc1, transformer.transform(doc1));
        assertSame(doc2, transformer.transform(doc2));
        assertSame(doc3, transformer.transform(doc3));
        assertSame(doc4, transformer.transform(doc4));
    }

    @Test
    @DisplayName("should transform a Document that has both $ref and $id fields to a DBRef")
    void shouldTransformDocumentWithRefAndId() {
        Document doc = new Document("$ref", "foo").append("$id", 1);
        assertEquals(new DBRef("foo", 1), transformer.transform(doc));
    }

    @Test
    @DisplayName("should transform a Document that has $ref and $id and $db fields to a DBRef")
    void shouldTransformDocumentWithRefIdAndDb() {
        Document doc = new Document("$ref", "foo").append("$id", 1).append("$db", "mydb");
        assertEquals(new DBRef("mydb", "foo", 1), transformer.transform(doc));
    }

    @Test
    @DisplayName("should be equal to another instance")
    void shouldBeEqualToAnotherInstance() {
        assertEquals(transformer, new DocumentToDBRefTransformer());
    }

    @Test
    @DisplayName("should not be equal to anything else")
    void shouldNotBeEqualToAnythingElse() {
        assertNotEquals(transformer, 1);
    }
}
