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

package org.bson;

import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.io.BasicOutputBuffer;
import org.bson.json.JsonMode;
import org.bson.json.JsonReader;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StringWriter;
import java.nio.ByteOrder;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RawBsonDocumentTest {

    private static final BsonDocument EMPTY_DOCUMENT = new BsonDocument();
    private static final RawBsonDocument EMPTY_RAW_DOCUMENT = new RawBsonDocument(EMPTY_DOCUMENT, new BsonDocumentCodec());
    private static final BsonDocument DOCUMENT = new BsonDocument()
            .append("a", new BsonInt32(1))
            .append("b", new BsonInt32(2))
            .append("c", new BsonDocument("x", BsonBoolean.TRUE))
            .append("d", new BsonArray(asList(new BsonDocument("y", BsonBoolean.FALSE), new BsonArray(asList(new BsonInt32(1))))));

    @Test
    @DisplayName("constructors should throw if parameters are invalid")
    void constructorsShouldThrowIfParametersAreInvalid() {
        assertThrows(IllegalArgumentException.class, () -> new RawBsonDocument(null));
        assertThrows(IllegalArgumentException.class, () -> new RawBsonDocument(null, 0, 5));
        assertThrows(IllegalArgumentException.class, () -> new RawBsonDocument(new byte[5], -1, 5));
        assertThrows(IllegalArgumentException.class, () -> new RawBsonDocument(new byte[5], 5, 5));
        assertThrows(IllegalArgumentException.class, () -> new RawBsonDocument(new byte[5], 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new RawBsonDocument(new byte[10], 6, 5));
        assertThrows(IllegalArgumentException.class, () -> new RawBsonDocument(null, new DocumentCodec()));
        assertThrows(IllegalArgumentException.class, () -> new RawBsonDocument(new Document(), null));
    }

    @Test
    @DisplayName("byteBuffer should contain the correct bytes")
    void byteBufferShouldContainCorrectBytes() {
        for (RawBsonDocument rawDocument : createRawDocumentVariants()) {
            ByteBuf byteBuf = rawDocument.getByteBuffer();
            assertEquals(rawDocument, DOCUMENT);
            assertEquals(ByteOrder.LITTLE_ENDIAN, byteBuf.asNIO().order());
            assertEquals(66, byteBuf.remaining());

            byte[] actualBytes = new byte[66];
            byteBuf.get(actualBytes);
            assertArrayEquals(getBytesFromDocument(), actualBytes);
        }
    }

    @Test
    @DisplayName("parse should throw if parameter is invalid")
    void parseShouldThrowForInvalidParameter() {
        assertThrows(IllegalArgumentException.class, () -> RawBsonDocument.parse(null));
    }

    @Test
    @DisplayName("should parse json")
    void shouldParseJson() {
        assertEquals(new BsonDocument("a", new BsonInt32(1)), RawBsonDocument.parse("{a : 1}"));
    }

    @Test
    @DisplayName("containKey should throw if the key name is null")
    void containsKeyShouldThrowForNull() {
        for (RawBsonDocument rawDocument : createRawDocumentVariants()) {
            assertThrows(IllegalArgumentException.class, () -> rawDocument.containsKey(null));
        }
    }

    @Test
    @DisplayName("containsKey should find an existing key")
    void containsKeyShouldFindExistingKey() {
        for (RawBsonDocument rawDocument : createRawDocumentVariants()) {
            assertTrue(rawDocument.containsKey("a"));
            assertTrue(rawDocument.containsKey("b"));
            assertTrue(rawDocument.containsKey("c"));
            assertTrue(rawDocument.containsKey("d"));
        }
    }

    @Test
    @DisplayName("containsKey should not find a non-existing key")
    void containsKeyShouldNotFindNonExistingKey() {
        for (RawBsonDocument rawDocument : createRawDocumentVariants()) {
            assertFalse(rawDocument.containsKey("e"));
            assertFalse(rawDocument.containsKey("x"));
            assertFalse(rawDocument.containsKey("y"));
            assertNull(rawDocument.get("e"));
            assertNull(rawDocument.get("x"));
            assertNull(rawDocument.get("y"));
        }
    }

    @Test
    @DisplayName("should return RawBsonDocument for sub documents and RawBsonArray for arrays")
    void shouldReturnCorrectTypesForSubDocumentsAndArrays() {
        for (RawBsonDocument rawDocument : createRawDocumentVariants()) {
            assertTrue(rawDocument.get("a") instanceof BsonInt32);
            assertTrue(rawDocument.get("b") instanceof BsonInt32);
            assertTrue(rawDocument.get("c") instanceof RawBsonDocument);
            assertTrue(rawDocument.get("d") instanceof RawBsonArray);
            assertTrue(rawDocument.get("d").asArray().get(0) instanceof RawBsonDocument);
            assertTrue(rawDocument.get("d").asArray().get(1) instanceof RawBsonArray);

            assertTrue(rawDocument.getDocument("c").getBoolean("x").getValue());
            assertFalse(rawDocument.get("d").asArray().get(0).asDocument().getBoolean("y").getValue());
            assertEquals(1, rawDocument.get("d").asArray().get(1).asArray().get(0).asInt32().getValue());
        }
    }

    @Test
    @DisplayName("containValue should find an existing value")
    void containsValueShouldFindExistingValue() {
        for (RawBsonDocument rawDocument : createRawDocumentVariants()) {
            assertTrue(rawDocument.containsValue(DOCUMENT.get("a")));
            assertTrue(rawDocument.containsValue(DOCUMENT.get("b")));
            assertTrue(rawDocument.containsValue(DOCUMENT.get("c")));
            assertTrue(rawDocument.containsValue(DOCUMENT.get("d")));
        }
    }

    @Test
    @DisplayName("containValue should not find a non-existing value")
    void containsValueShouldNotFindNonExistingValue() {
        for (RawBsonDocument rawDocument : createRawDocumentVariants()) {
            assertFalse(rawDocument.containsValue(new BsonInt32(3)));
            assertFalse(rawDocument.containsValue(new BsonDocument("e", BsonBoolean.FALSE)));
            assertFalse(rawDocument.containsValue(new BsonArray(asList(new BsonInt32(2), new BsonInt32(4)))));
        }
    }

    @Test
    @DisplayName("isEmpty should return false when the document is not empty")
    void isEmptyShouldReturnFalseWhenNotEmpty() {
        for (RawBsonDocument rawDocument : createRawDocumentVariants()) {
            assertFalse(rawDocument.isEmpty());
        }
    }

    @Test
    @DisplayName("isEmpty should return true when the document is empty")
    void isEmptyShouldReturnTrueWhenEmpty() {
        assertTrue(EMPTY_RAW_DOCUMENT.isEmpty());
    }

    @Test
    @DisplayName("should get correct size when the document is empty")
    void shouldGetCorrectSizeWhenEmpty() {
        assertEquals(0, EMPTY_RAW_DOCUMENT.size());
    }

    @Test
    @DisplayName("should get correct key set when the document is empty")
    void shouldGetCorrectKeySetWhenEmpty() {
        assertTrue(EMPTY_RAW_DOCUMENT.keySet().isEmpty());
    }

    @Test
    @DisplayName("should get correct values set when the document is empty")
    void shouldGetCorrectValuesWhenEmpty() {
        assertTrue(EMPTY_RAW_DOCUMENT.values().isEmpty());
    }

    @Test
    @DisplayName("should get correct entry set when the document is empty")
    void shouldGetCorrectEntrySetWhenEmpty() {
        assertTrue(EMPTY_RAW_DOCUMENT.entrySet().isEmpty());
    }

    @Test
    @DisplayName("should get correct size")
    void shouldGetCorrectSize() {
        for (RawBsonDocument rawDocument : createRawDocumentVariants()) {
            assertEquals(4, rawDocument.size());
        }
    }

    @Test
    @DisplayName("should get correct key set")
    void shouldGetCorrectKeySet() {
        for (RawBsonDocument rawDocument : createRawDocumentVariants()) {
            assertEquals(new HashSet<>(asList("a", "b", "c", "d")), rawDocument.keySet());
        }
    }

    @Test
    @DisplayName("should get correct values set")
    void shouldGetCorrectValuesSet() {
        for (RawBsonDocument rawDocument : createRawDocumentVariants()) {
            assertEquals(
                    new HashSet<>(asList(DOCUMENT.get("a"), DOCUMENT.get("b"), DOCUMENT.get("c"), DOCUMENT.get("d"))),
                    new HashSet<>(rawDocument.values()));
        }
    }

    @Test
    @DisplayName("should get correct entry set")
    void shouldGetCorrectEntrySet() {
        for (RawBsonDocument rawDocument : createRawDocumentVariants()) {
            Set<Map.Entry<String, BsonValue>> expected = new HashSet<>();
            expected.add(new AbstractMap.SimpleEntry<>("a", DOCUMENT.get("a")));
            expected.add(new AbstractMap.SimpleEntry<>("b", DOCUMENT.get("b")));
            expected.add(new AbstractMap.SimpleEntry<>("c", DOCUMENT.get("c")));
            expected.add(new AbstractMap.SimpleEntry<>("d", DOCUMENT.get("d")));
            assertEquals(expected, rawDocument.entrySet());
        }
    }

    @Test
    @DisplayName("should get first key")
    void shouldGetFirstKey() {
        assertEquals("a", DOCUMENT.getFirstKey());
    }

    @Test
    @DisplayName("getFirstKey should throw NoSuchElementException if the document is empty")
    void getFirstKeyShouldThrowForEmptyDocument() {
        assertThrows(NoSuchElementException.class, EMPTY_RAW_DOCUMENT::getFirstKey);
    }

    @Test
    @DisplayName("should create BsonReader")
    void shouldCreateBsonReader() {
        BsonReader reader = DOCUMENT.asBsonReader();
        try {
            assertEquals(DOCUMENT, new BsonDocumentCodec().decode(reader, DecoderContext.builder().build()));
        } finally {
            reader.close();
        }
    }

    @Test
    @DisplayName("toJson should return equivalent JSON")
    void toJsonShouldReturnEquivalentJson() {
        for (RawBsonDocument rawDocument : createRawDocumentVariants()) {
            assertEquals(DOCUMENT, new RawBsonDocumentCodec().decode(new JsonReader(rawDocument.toJson()), DecoderContext.builder().build()));
        }
    }

    @Test
    @DisplayName("toJson should respect default JsonWriterSettings")
    void toJsonShouldRespectDefaultJsonWriterSettings() {
        for (RawBsonDocument rawDocument : createRawDocumentVariants()) {
            StringWriter writer = new StringWriter();
            new BsonDocumentCodec().encode(new JsonWriter(writer), DOCUMENT, EncoderContext.builder().build());
            assertEquals(writer.toString(), rawDocument.toJson());
        }
    }

    @Test
    @DisplayName("toJson should respect JsonWriterSettings")
    void toJsonShouldRespectJsonWriterSettings() {
        JsonWriterSettings jsonWriterSettings = JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build();
        for (RawBsonDocument rawDocument : createRawDocumentVariants()) {
            StringWriter writer = new StringWriter();
            new RawBsonDocumentCodec().encode(new JsonWriter(writer, jsonWriterSettings), rawDocument, EncoderContext.builder().build());
            assertEquals(writer.toString(), rawDocument.toJson(jsonWriterSettings));
        }
    }

    @Test
    @DisplayName("all write methods should throw UnsupportedOperationException")
    void allWriteMethodsShouldThrow() {
        RawBsonDocument rawDocument = createRawDocumentFromDocument();

        assertThrows(UnsupportedOperationException.class, rawDocument::clear);
        assertThrows(UnsupportedOperationException.class, () -> rawDocument.put("x", BsonNull.VALUE));
        assertThrows(UnsupportedOperationException.class, () -> rawDocument.append("x", BsonNull.VALUE));
        assertThrows(UnsupportedOperationException.class, () -> rawDocument.putAll(new BsonDocument("x", BsonNull.VALUE)));
        assertThrows(UnsupportedOperationException.class, () -> rawDocument.remove(BsonNull.VALUE));
    }

    @Test
    @DisplayName("should decode")
    void shouldDecode() {
        for (RawBsonDocument rawDocument : createRawDocumentVariants()) {
            assertEquals(DOCUMENT, rawDocument.decode(new BsonDocumentCodec()));
        }
    }

    @Test
    @DisplayName("hashCode should equal hash code of identical BsonDocument")
    void hashCodeShouldEqualBsonDocumentHashCode() {
        for (RawBsonDocument rawDocument : createRawDocumentVariants()) {
            assertEquals(DOCUMENT.hashCode(), rawDocument.hashCode());
        }
    }

    @Test
    @DisplayName("equals should equal identical BsonDocument")
    void equalsShouldEqualBsonDocument() {
        for (RawBsonDocument rawDocument : createRawDocumentVariants()) {
            assertTrue(rawDocument.equals(DOCUMENT));
            assertTrue(DOCUMENT.equals(rawDocument));
            assertTrue(rawDocument.equals(rawDocument));
            assertFalse(rawDocument.equals(EMPTY_RAW_DOCUMENT));
        }
    }

    @Test
    @DisplayName("clone should make a deep copy")
    void cloneShouldMakeDeepCopy() {
        for (RawBsonDocument rawDocument : createRawDocumentVariants()) {
            RawBsonDocument cloned = (RawBsonDocument) rawDocument.clone();
            assertNotSame(cloned.getByteBuffer().array(), createRawDocumentFromDocument().getByteBuffer().array());
            assertEquals(rawDocument.getByteBuffer().remaining(), cloned.getByteBuffer().remaining());
            assertEquals(createRawDocumentFromDocument(), cloned);
        }
    }

    @Test
    @DisplayName("should serialize and deserialize")
    void shouldSerializeAndDeserialize() throws Exception {
        for (RawBsonDocument rawDocument : createRawDocumentVariants()) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(rawDocument);
            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bais);
            Object deserializedDocument = ois.readObject();
            assertEquals(DOCUMENT, deserializedDocument);
        }
    }

    private static List<RawBsonDocument> createRawDocumentVariants() {
        return Arrays.asList(
                createRawDocumentFromDocument(),
                createRawDocumentFromByteArray(),
                createRawDocumentFromByteArrayOffsetLength()
        );
    }

    private static RawBsonDocument createRawDocumentFromDocument() {
        return new RawBsonDocument(DOCUMENT, new BsonDocumentCodec());
    }

    private static RawBsonDocument createRawDocumentFromByteArray() {
        return new RawBsonDocument(getBytesFromDocument());
    }

    private static byte[] getBytesFromDocument() {
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer(1024);
        new BsonDocumentCodec().encode(new BsonBinaryWriter(outputBuffer), DOCUMENT, EncoderContext.builder().build());
        byte[] bytes = outputBuffer.getInternalBuffer();
        int size = outputBuffer.getPosition();
        byte[] strippedBytes = new byte[size];
        System.arraycopy(bytes, 0, strippedBytes, 0, size);
        return strippedBytes;
    }

    private static RawBsonDocument createRawDocumentFromByteArrayOffsetLength() {
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer(1024);
        new BsonDocumentCodec().encode(new BsonBinaryWriter(outputBuffer), DOCUMENT, EncoderContext.builder().build());
        byte[] bytes = outputBuffer.getInternalBuffer();
        int size = outputBuffer.getPosition();
        byte[] unstrippedBytes = new byte[size + 2];
        System.arraycopy(bytes, 0, unstrippedBytes, 1, size);
        return new RawBsonDocument(unstrippedBytes, 1, size);
    }
}
