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

import org.bson.BSONEncoder;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.compile;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LazyDBObjectTest {

    private BSONEncoder encoder;
    private OutputBuffer buf;
    private ByteArrayOutputStream bios;
    private LazyDBDecoder lazyDBDecoder;
    private DefaultDBDecoder defaultDBDecoder;

    @BeforeEach
    void setUp() {
        encoder = new DefaultDBEncoder();
        buf = new BasicOutputBuffer();
        encoder.set(buf);
        bios = new ByteArrayOutputStream();
        lazyDBDecoder = new LazyDBDecoder();
        defaultDBDecoder = new DefaultDBDecoder();
    }

    @Test
    @DisplayName("should lazily decode a DBRef")
    void shouldLazilyDecodeADBRef() {
        byte[] bytes = {
                44, 0, 0, 0, 3, 102, 0, 36, 0, 0, 0, 2, 36, 114, 101, 102,
                0, 4, 0, 0, 0, 97, 46, 98, 0, 7, 36, 105, 100, 0, 18, 52,
                86, 120, -112, 18, 52, 86, 120, -112, 18, 52, 0, 0,
        };

        LazyDBObject document = new LazyDBObject(bytes, new LazyDBCallback(null));

        assertInstanceOf(DBRef.class, document.get("f"));
        assertEquals(new DBRef("a.b", new ObjectId("123456789012345678901234")), document.get("f"));
    }

    @Test
    @DisplayName("should lazily decode a DBRef with $db")
    void shouldLazilyDecodeADBRefWithDb() {
        byte[] bytes = {
                58, 0, 0, 0, 3, 102, 0, 50, 0, 0, 0, 2, 36, 114, 101, 102,
                0, 4, 0, 0, 0, 97, 46, 98, 0, 7, 36, 105, 100, 0, 18, 52,
                86, 120, -112, 18, 52, 86, 120, -112, 18, 52,
                2, 36, 100, 98, 0, 5, 0, 0, 0, 109, 121, 100, 98, 0, 0, 0
        };

        LazyDBObject document = new LazyDBObject(bytes, new LazyDBCallback(null));

        assertInstanceOf(DBRef.class, document.get("f"));
        assertEquals(new DBRef("mydb", "a.b", new ObjectId("123456789012345678901234")), document.get("f"));
    }

    @Test
    @DisplayName("should toString correctly")
    void testToString() throws IOException {
        DBObject origDoc = new BasicDBObject("x", true);
        encoder.putObject(origDoc);
        buf.pipe(bios);

        DBObject doc = lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);

        assertEquals("{\"x\": true}", doc.toString());
    }

    @Test
    @DisplayName("should decode all types")
    void testDecodeAllTypes() throws IOException {
        DBObject origDoc = getTestDocument();
        encoder.putObject(origDoc);
        buf.pipe(bios);

        DBObject doc = defaultDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);

        assertDocsSame(origDoc, doc);
    }

    @Test
    @DisplayName("should lazy decode all types")
    void testLazyDecodeAllTypes() throws IOException {
        DBObject origDoc = getTestDocument();
        encoder.putObject(origDoc);
        buf.pipe(bios);

        DBObject doc = lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);

        assertDocsSame(origDoc, doc);
    }

    @Test
    @DisplayName("should return null for missing key")
    void testMissingKey() throws IOException {
        encoder.putObject(getSimpleTestDocument());
        buf.pipe(bios);

        DBObject decodedObj = lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);

        assertNull(decodedObj.get("missingKey"));
    }

    @Test
    @DisplayName("should return correct key set")
    void testKeySet() throws IOException {
        DBObject obj = getSimpleTestDocument();
        encoder.putObject(obj);
        buf.pipe(bios);

        DBObject decodedObj = lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);

        assertNotNull(decodedObj);
        assertInstanceOf(LazyDBObject.class, decodedObj);
        Set<String> keySet = decodedObj.keySet();

        assertEquals(6, keySet.size());
        assertFalse(keySet.isEmpty());

        assertEquals(6, keySet.toArray().length);

        String[] typedArray = keySet.toArray(new String[0]);
        assertEquals(6, typedArray.length);

        String[] array = keySet.toArray(new String[7]);
        assertEquals(7, array.length);
        assertNull(array[6]);

        assertTrue(keySet.contains("first"));
        assertFalse(keySet.contains("x"));

        assertTrue(keySet.containsAll(Arrays.asList("first", "second", "_id", "third", "fourth", "fifth")));
        assertFalse(keySet.containsAll(Arrays.asList("first", "notFound")));

        assertEquals(obj.get("_id"), decodedObj.get("_id"));
        assertEquals(obj.get("first"), decodedObj.get("first"));
        assertEquals(obj.get("second"), decodedObj.get("second"));
        assertEquals(obj.get("third"), decodedObj.get("third"));
        assertEquals(obj.get("fourth"), decodedObj.get("fourth"));
        assertEquals(obj.get("fifth"), decodedObj.get("fifth"));
    }

    @Test
    @DisplayName("should return correct entry set")
    void testEntrySet() throws IOException {
        DBObject obj = getSimpleTestDocument();
        encoder.putObject(obj);
        buf.pipe(bios);

        DBObject decodedObj = lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);

        Set<Map.Entry<String, Object>> entrySet = ((LazyDBObject) decodedObj).entrySet();
        assertEquals(6, entrySet.size());
        assertFalse(entrySet.isEmpty());

        assertEquals(6, entrySet.toArray().length);

        @SuppressWarnings("unchecked")
        Map.Entry<String, Object>[] typedArray = entrySet.toArray(new Map.Entry[entrySet.size()]);
        assertEquals(6, typedArray.length);

        @SuppressWarnings("unchecked")
        Map.Entry<String, Object>[] array = entrySet.toArray(new Map.Entry[7]);
        assertEquals(7, array.length);
        assertNull(array[6]);
    }

    @Test
    @DisplayName("should pipe correctly")
    void testPipe() throws IOException {
        DBObject obj = getSimpleTestDocument();
        encoder.putObject(obj);
        buf.pipe(bios);

        LazyDBObject lazyDBObj = (LazyDBObject) lazyDBDecoder.decode(
                new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);
        bios.reset();
        int byteCount = lazyDBObj.pipe(bios);

        assertEquals(lazyDBObj.getBSONSize(), byteCount);

        LazyDBObject lazyDBObjectFromPipe = (LazyDBObject) lazyDBDecoder.decode(
                new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);

        assertEquals(lazyDBObj, lazyDBObjectFromPipe);
    }

    @Test
    @DisplayName("should encode with LazyDBEncoder")
    void testLazyDBEncoder() throws IOException {
        DBObject obj = getSimpleTestDocument();
        encoder.putObject(obj);
        buf.pipe(bios);
        LazyDBObject lazyDBObj = (LazyDBObject) lazyDBDecoder.decode(
                new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);

        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        int size = new LazyDBEncoder().writeObject(outputBuffer, lazyDBObj);

        assertEquals(lazyDBObj.getBSONSize(), size);
        assertEquals(lazyDBObj.getBSONSize(), outputBuffer.size());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        lazyDBObj.pipe(baos);
        assertArrayEquals(baos.toByteArray(), outputBuffer.toByteArray());
    }

    private DBObject getSimpleTestDocument() {
        return new BasicDBObject("_id", new ObjectId())
                .append("first", 1)
                .append("second", "str1")
                .append("third", true)
                .append("fourth", null)
                .append("fifth", new BasicDBObject("firstNested", 1));
    }

    @SuppressWarnings("unchecked")
    private DBObject getTestDocument() {
        return new BasicDBObject("_id", new ObjectId())
                .append("null", null)
                .append("max", new MaxKey())
                .append("min", new MinKey())
                .append("booleanTrue", true)
                .append("booleanFalse", false)
                .append("int1", 1)
                .append("int1500", 1500)
                .append("int3753", 3753)
                .append("tsp", new BSONTimestamp())
                .append("date", new Date())
                .append("long5", 5L)
                .append("long3254525", 3254525L)
                .append("float324_582", 324.582f)
                .append("double245_6289", (double) 245.6289)
                .append("oid", new ObjectId())
                .append("symbol", new Symbol("foobar"))
                .append("code", new Code("var x = 12345;"))
                .append("str", "foobarbaz")
                .append("ref", new DBRef("testRef", new ObjectId()))
                .append("object", new BasicDBObject("abc", "12345"))
                .append("array", Arrays.asList("foo", "bar", "baz", "x", "y", "z"))
                .append("binary", new Binary("scott".getBytes()))
                .append("regex", compile("^test.*regex.*xyz$", CASE_INSENSITIVE));
    }

    @SuppressWarnings("unchecked")
    private void assertDocsSame(final DBObject origDoc, final DBObject doc) {
        assertEquals(origDoc.get("str"), doc.get("str"));
        assertEquals(origDoc.get("_id"), doc.get("_id"));
        assertNull(doc.get("null"));
        assertEquals(origDoc.get("max"), doc.get("max"));
        assertEquals(origDoc.get("min"), doc.get("min"));
        assertTrue((Boolean) doc.get("booleanTrue"));
        assertFalse((Boolean) doc.get("booleanFalse"));
        assertEquals(origDoc.get("int1"), doc.get("int1"));
        assertEquals(origDoc.get("int1500"), doc.get("int1500"));
        assertEquals(origDoc.get("int3753"), doc.get("int3753"));
        assertEquals(origDoc.get("tsp"), doc.get("tsp"));
        assertEquals(doc.get("date"), doc.get("date"));
        assertEquals(5L, doc.get("long5"));
        assertEquals(3254525L, doc.get("long3254525"));
        assertEquals(324.5820007324219f, ((Number) doc.get("float324_582")).floatValue());
        assertEquals((double) 245.6289, ((Number) doc.get("double245_6289")).doubleValue());
        assertEquals(origDoc.get("oid"), doc.get("oid"));
        assertEquals("foobarbaz", doc.get("str"));
        assertEquals(origDoc.get("ref"), doc.get("ref"));
        assertEquals(((DBObject) origDoc.get("object")).get("abc"), ((DBObject) doc.get("object")).get("abc"));
        assertEquals("foo", ((List<String>) doc.get("array")).get(0));
        assertEquals("bar", ((List<String>) doc.get("array")).get(1));
        assertEquals("baz", ((List<String>) doc.get("array")).get(2));
        assertEquals("x", ((List<String>) doc.get("array")).get(3));
        assertEquals("y", ((List<String>) doc.get("array")).get(4));
        assertEquals("z", ((List<String>) doc.get("array")).get(5));
        assertArrayEquals(((Binary) origDoc.get("binary")).getData(), (byte[]) doc.get("binary"));
        assertEquals(((Pattern) origDoc.get("regex")).pattern(), ((Pattern) doc.get("regex")).pattern());
        assertEquals(((Pattern) origDoc.get("regex")).flags(), ((Pattern) doc.get("regex")).flags());
    }
}
