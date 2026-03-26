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

package org.bson.types;

import org.bson.BsonRegularExpression;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.json.JsonParseException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DocumentTest {

    @Test
    void shouldReturnCorrectTypeForEachTypedMethod() {
        Date date = new Date();
        ObjectId objectId = new ObjectId();

        Document doc = new Document()
                .append("int", 1).append("long", 2L).append("double", 3.0d).append("string", "hi").append("boolean", true)
                .append("objectId", objectId).append("date", date);

        assertEquals(1, (int) doc.getInteger("int"));
        assertEquals(42, (int) doc.getInteger("intNoVal", 42));
        assertEquals(2L, (long) doc.getLong("long"));
        assertEquals(3.0d, doc.getDouble("double"));
        assertEquals("hi", doc.getString("string"));
        assertTrue(doc.getBoolean("boolean"));
        assertTrue(doc.getBoolean("booleanNoVal", true));
        assertEquals(objectId, doc.getObjectId("objectId"));
        assertEquals(date, doc.getDate("date"));

        assertEquals(objectId, doc.get("objectId", ObjectId.class));
        assertEquals(1, (int) doc.get("int", Integer.class));
        assertEquals(2L, (long) doc.get("long", Long.class));
        assertEquals(3.0d, doc.get("double", Double.class));
        assertEquals("hi", doc.get("string", String.class));
        assertTrue(doc.get("boolean", Boolean.class));
        assertEquals(date, doc.get("date", Date.class));

        assertEquals(42L, (long) doc.get("noVal", 42L));
        assertEquals(3.1d, doc.get("noVal", 3.1d));
        assertEquals("defVal", doc.get("noVal", "defVal"));
        assertTrue(doc.get("noVal", true));
        assertEquals(objectId, doc.get("noVal", objectId));
        assertEquals(date, doc.get("noVal", date));
    }

    @Test
    void shouldReturnListWithElementsOfSpecifiedClass() {
        Document doc = Document.parse("{x: 1, y: ['two', 'three'], z: [{a: 'one'}, {b:2}], w: {a: ['One', 'Two']}}")
                .append("numberList", Arrays.asList(10, 20.5d, 30L))
                .append("listWithNullElement", Arrays.asList(10, null, 20));
        List<String> defaultList = Arrays.asList("a", "b", "c");

        assertEquals("two", doc.getList("y", String.class).get(0));
        assertEquals("three", doc.getList("y", String.class).get(1));
        assertEquals("one", doc.getList("z", Document.class).get(0).getString("a"));
        assertEquals(2, (int) doc.getList("z", Document.class).get(1).getInteger("b"));
        assertEquals("One", doc.get("w", Document.class).getList("a", String.class).get(0));
        assertEquals("Two", doc.get("w", Document.class).getList("a", String.class).get(1));
        assertEquals("a", doc.getList("invalidKey", String.class, defaultList).get(0));
        assertEquals("b", doc.getList("invalidKey", String.class, defaultList).get(1));
        assertEquals("c", doc.getList("invalidKey", String.class, defaultList).get(2));
        assertEquals(10, doc.getList("numberList", Number.class).get(0));
        assertEquals(20.5d, doc.getList("numberList", Number.class).get(1));
        assertEquals(30L, doc.getList("numberList", Number.class).get(2));
        assertEquals(10, doc.getList("listWithNullElement", Number.class).get(0));
        assertNull(doc.getList("listWithNullElement", Number.class).get(1));
        assertEquals(20, doc.getList("listWithNullElement", Number.class).get(2));
    }

    @Test
    void shouldReturnNullListWhenKeyIsNotFound() {
        Document doc = Document.parse("{x: 1}");
        assertNull(doc.getList("a", String.class));
    }

    @Test
    void shouldReturnSpecifiedDefaultValueWhenKeyIsNotFound() {
        Document doc = Document.parse("{x: 1}");
        List<String> defaultList = Arrays.asList("a", "b", "c");
        assertEquals(defaultList, doc.getList("a", String.class, defaultList));
    }

    @Test
    void shouldThrowExceptionWhenListElementsAreNotOfSpecifiedClass() {
        Document doc = Document.parse("{x: 1, y: [{a: 1}, {b: 2}], z: [1, 2]}");
        assertThrows(ClassCastException.class, () -> doc.getList("x", String.class));
        assertThrows(ClassCastException.class, () -> doc.getList("y", String.class));
        assertThrows(ClassCastException.class, () -> doc.getList("z", String.class));
    }

    @Test
    void shouldReturnNullWhenGettingEmbeddedValue() {
        Document document = Document.parse("{a: 1, b: {x: [2, 3, 4], y: {m: 'one', len: 3}}, 'a.b': 'two'}");
        assertNull(document.getEmbedded(Arrays.asList("notAKey"), String.class));
        assertNull(document.getEmbedded(Arrays.asList("b", "y", "notAKey"), String.class));
        assertNull(document.getEmbedded(Arrays.asList("b", "b", "m"), String.class));
        assertNull(Document.parse("{}").getEmbedded(Arrays.asList("a", "b"), Integer.class));
        assertNull(Document.parse("{b: 1}").getEmbedded(Arrays.asList("a"), Integer.class));
        assertNull(Document.parse("{b: 1}").getEmbedded(Arrays.asList("a", "b"), Integer.class));
        assertNull(Document.parse("{a: {c: 1}}").getEmbedded(Arrays.asList("a", "b"), Integer.class));
        assertNull(Document.parse("{a: {c: 1}}").getEmbedded(Arrays.asList("a", "b", "c"), Integer.class));
    }

    @Test
    void shouldReturnEmbeddedValue() {
        Date date = new Date();
        ObjectId objectId = new ObjectId();

        Document document = Document.parse("{a: 1, b: {x: [2, 3, 4], y: {m: 'one', len: 3}}, 'a.b': 'two'}")
                .append("l", new Document("long", 2L))
                .append("d", new Document("double", 3.0d))
                .append("t", new Document("boolean", true))
                .append("o", new Document("objectId", objectId))
                .append("n", new Document("date", date));

        assertEquals(1, (int) document.getEmbedded(Arrays.asList("a"), Integer.class));
        assertEquals(2, document.getEmbedded(Arrays.asList("b", "x"), List.class).get(0));
        assertEquals(3, document.getEmbedded(Arrays.asList("b", "x"), List.class).get(1));
        assertEquals(4, document.getEmbedded(Arrays.asList("b", "x"), List.class).get(2));
        assertEquals("one", document.getEmbedded(Arrays.asList("b", "y", "m"), String.class));
        assertEquals(3, (int) document.getEmbedded(Arrays.asList("b", "y", "len"), Integer.class));
        assertEquals("two", document.getEmbedded(Arrays.asList("a.b"), String.class));
        assertEquals("one", document.getEmbedded(Arrays.asList("b", "y"), Document.class).getString("m"));
        assertEquals(3, (int) document.getEmbedded(Arrays.asList("b", "y"), Document.class).getInteger("len"));

        assertEquals(2L, (long) document.getEmbedded(Arrays.asList("l", "long"), Long.class));
        assertEquals(3.0d, document.getEmbedded(Arrays.asList("d", "double"), Double.class));
        assertEquals(2L, document.getEmbedded(Arrays.asList("l", "long"), Number.class));
        assertEquals(3.0d, document.getEmbedded(Arrays.asList("d", "double"), Number.class));
        assertEquals(true, document.getEmbedded(Arrays.asList("t", "boolean"), Boolean.class));
        assertEquals(false, document.getEmbedded(Arrays.asList("t", "x"), false));
        assertEquals(objectId, document.getEmbedded(Arrays.asList("o", "objectId"), ObjectId.class));
        assertEquals(date, document.getEmbedded(Arrays.asList("n", "date"), Date.class));
    }

    @Test
    void shouldThrowExceptionGettingEmbeddedValue() {
        Document document = Document.parse("{a: 1, b: {x: [2, 3, 4], y: {m: 'one', len: 3}}, 'a.b': 'two'}");

        assertThrows(IllegalArgumentException.class, () -> document.getEmbedded(null, String.class));
        assertThrows(IllegalStateException.class, () -> document.getEmbedded(Arrays.asList(), String.class));
        assertThrows(ClassCastException.class, () -> document.getEmbedded(Arrays.asList("a", "b"), Integer.class));
        assertThrows(ClassCastException.class, () -> document.getEmbedded(Arrays.asList("b", "y", "m"), Integer.class));
        assertThrows(ClassCastException.class, () -> document.getEmbedded(Arrays.asList("b", "x"), Document.class));
        assertThrows(ClassCastException.class, () -> document.getEmbedded(Arrays.asList("b", "x", "m"), String.class));
        assertThrows(ClassCastException.class, () -> document.getEmbedded(Arrays.asList("b", "x", "m"), "invalid"));
    }

    @Test
    void shouldParseValidJsonStringToDocument() {
        Document document = Document.parse("{ 'int' : 1, 'string' : 'abc' }");
        assertNotNull(document);
        assertEquals(2, document.keySet().size());
        assertEquals(1, (int) document.getInteger("int"));
        assertEquals("abc", document.getString("string"));

        document = Document.parse("{ 'int' : 1, 'string' : 'abc' }", new DocumentCodec());
        assertNotNull(document);
        assertEquals(2, document.keySet().size());
        assertEquals(1, (int) document.getInteger("int"));
        assertEquals("abc", document.getString("string"));
    }

    @Test
    void testParseMethodWithMode() {
        Document document = Document.parse("{'regex' : /abc/im }");
        assertNotNull(document);
        assertEquals(1, document.keySet().size());

        BsonRegularExpression regularExpression = (BsonRegularExpression) document.get("regex");
        assertEquals("im", regularExpression.getOptions());
        assertEquals("abc", regularExpression.getPattern());
    }

    @Test
    void shouldThrowExceptionWhenParsingInvalidJsonString() {
        assertThrows(JsonParseException.class, () -> Document.parse("{ 'int' : 1, 'string' : }"));
    }

    @Test
    void shouldCastToCorrectType() {
        Document document = new Document("str", "a string");
        String s = document.get("str", String.class);
        assertEquals(document.get("str"), s);
    }

    @Test
    void shouldThrowClassCastExceptionWhenValueIsWrongType() {
        Document document = new Document("int", "not an int");
        assertThrows(ClassCastException.class, () -> document.get("int", Integer.class));
    }
}
