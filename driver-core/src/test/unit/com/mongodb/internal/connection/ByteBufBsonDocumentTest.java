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

package com.mongodb.internal.connection;

import org.bson.BsonArray;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ByteBufBsonDocumentTest {

    private final ByteBuf emptyDocumentByteBuf = new ByteBufNIO(ByteBuffer.wrap(new byte[]{5, 0, 0, 0, 0}));
    private ByteBuf documentByteBuf;
    private final ByteBufBsonDocument emptyByteBufDocument = new ByteBufBsonDocument(emptyDocumentByteBuf);
    private final BsonDocument document = new BsonDocument()
            .append("a", new BsonInt32(1))
            .append("b", new BsonInt32(2))
            .append("c", new BsonDocument("x", BsonBoolean.TRUE))
            .append("d", new BsonArray(Arrays.asList(new BsonDocument("y", BsonBoolean.FALSE), new BsonInt32(1))));

    private ByteBufBsonDocument byteBufDocument;

    @BeforeEach
    void setUp() throws IOException {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        new BsonDocumentCodec().encode(new BsonBinaryWriter(buffer), document, EncoderContext.builder().build());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        buffer.pipe(baos);
        documentByteBuf = new ByteBufNIO(ByteBuffer.wrap(baos.toByteArray()));
        byteBufDocument = new ByteBufBsonDocument(documentByteBuf);
    }

    @Test
    void getShouldGetTheValueOfTheGivenKey() {
        assertNull(emptyByteBufDocument.get("a"));
        assertNull(byteBufDocument.get("z"));
        assertEquals(new BsonInt32(1), byteBufDocument.get("a"));
        assertEquals(new BsonInt32(2), byteBufDocument.get("b"));
    }

    @Test
    void getShouldThrowIfTheKeyIsNull() {
        assertThrows(IllegalArgumentException.class, () -> byteBufDocument.get(null));
        assertEquals(1, documentByteBuf.getReferenceCount());
    }

    @Test
    void containsKeyShouldThrowIfTheKeyNameIsNull() {
        assertThrows(IllegalArgumentException.class, () -> byteBufDocument.containsKey(null));
        assertEquals(1, documentByteBuf.getReferenceCount());
    }

    @Test
    void containsKeyShouldFindAnExistingKey() {
        assertTrue(byteBufDocument.containsKey("a"));
        assertTrue(byteBufDocument.containsKey("b"));
        assertTrue(byteBufDocument.containsKey("c"));
        assertTrue(byteBufDocument.containsKey("d"));
        assertEquals(1, documentByteBuf.getReferenceCount());
    }

    @Test
    void containsKeyShouldNotFindANonExistingKey() {
        assertFalse(byteBufDocument.containsKey("e"));
        assertFalse(byteBufDocument.containsKey("x"));
        assertFalse(byteBufDocument.containsKey("y"));
        assertEquals(1, documentByteBuf.getReferenceCount());
    }

    @Test
    void containsValueShouldFindAnExistingValue() {
        assertTrue(byteBufDocument.containsValue(document.get("a")));
        assertTrue(byteBufDocument.containsValue(document.get("b")));
        assertTrue(byteBufDocument.containsValue(document.get("c")));
        assertTrue(byteBufDocument.containsValue(document.get("d")));
        assertEquals(1, documentByteBuf.getReferenceCount());
    }

    @Test
    void containsValueShouldNotFindANonExistingValue() {
        assertFalse(byteBufDocument.containsValue(new BsonInt32(3)));
        assertFalse(byteBufDocument.containsValue(new BsonDocument("e", BsonBoolean.FALSE)));
        assertFalse(byteBufDocument.containsValue(new BsonArray(Arrays.asList(new BsonInt32(2), new BsonInt32(4)))));
        assertEquals(1, documentByteBuf.getReferenceCount());
    }

    @Test
    void isEmptyShouldReturnFalseWhenTheDocumentIsNotEmpty() {
        assertFalse(byteBufDocument.isEmpty());
        assertEquals(1, documentByteBuf.getReferenceCount());
    }

    @Test
    void isEmptyShouldReturnTrueWhenTheDocumentIsEmpty() {
        assertTrue(emptyByteBufDocument.isEmpty());
        assertEquals(1, emptyDocumentByteBuf.getReferenceCount());
    }

    @Test
    void shouldGetCorrectSize() {
        assertEquals(0, emptyByteBufDocument.size());
        assertEquals(4, byteBufDocument.size());
        assertEquals(1, documentByteBuf.getReferenceCount());
        assertEquals(1, emptyDocumentByteBuf.getReferenceCount());
    }

    @Test
    void shouldGetCorrectKeySet() {
        assertTrue(emptyByteBufDocument.keySet().isEmpty());
        assertEquals(new LinkedHashSet<>(Arrays.asList("a", "b", "c", "d")), byteBufDocument.keySet());
        assertEquals(1, documentByteBuf.getReferenceCount());
        assertEquals(1, emptyDocumentByteBuf.getReferenceCount());
    }

    @Test
    void shouldGetCorrectValuesSet() {
        assertTrue(emptyByteBufDocument.values().isEmpty());
        Set<BsonValue> expectedValues = new HashSet<>(Arrays.asList(
                document.get("a"), document.get("b"), document.get("c"), document.get("d")));
        assertEquals(expectedValues, new HashSet<>(byteBufDocument.values()));
        assertEquals(1, documentByteBuf.getReferenceCount());
        assertEquals(1, emptyDocumentByteBuf.getReferenceCount());
    }

    @Test
    void shouldGetCorrectEntrySet() {
        assertTrue(emptyByteBufDocument.entrySet().isEmpty());
        Set<Map.Entry<String, BsonValue>> expectedEntries = new LinkedHashSet<>(Arrays.asList(
                new AbstractMap.SimpleEntry<>("a", document.get("a")),
                new AbstractMap.SimpleEntry<>("b", document.get("b")),
                new AbstractMap.SimpleEntry<>("c", document.get("c")),
                new AbstractMap.SimpleEntry<>("d", document.get("d"))));
        assertEquals(expectedEntries, byteBufDocument.entrySet());
        assertEquals(1, documentByteBuf.getReferenceCount());
        assertEquals(1, emptyDocumentByteBuf.getReferenceCount());
    }

    @Test
    void allWriteMethodsShouldThrowUnsupportedOperationException() {
        assertThrows(UnsupportedOperationException.class, () -> byteBufDocument.clear());
        assertThrows(UnsupportedOperationException.class, () -> byteBufDocument.put("x", BsonNull.VALUE));
        assertThrows(UnsupportedOperationException.class, () -> byteBufDocument.append("x", BsonNull.VALUE));
        assertThrows(UnsupportedOperationException.class, () -> byteBufDocument.putAll(new BsonDocument("x", BsonNull.VALUE)));
        assertThrows(UnsupportedOperationException.class, () -> byteBufDocument.remove(BsonNull.VALUE));
    }

    @Test
    void shouldGetFirstKey() {
        assertEquals(document.keySet().iterator().next(), byteBufDocument.getFirstKey());
        assertEquals(1, documentByteBuf.getReferenceCount());
    }

    @Test
    void getFirstKeyShouldThrowNoSuchElementExceptionIfTheDocumentIsEmpty() {
        assertThrows(NoSuchElementException.class, () -> emptyByteBufDocument.getFirstKey());
        assertEquals(1, emptyDocumentByteBuf.getReferenceCount());
    }

    @Test
    void shouldCreateBsonReader() {
        org.bson.BsonReader reader = document.asBsonReader();
        try {
            assertEquals(document, new BsonDocumentCodec().decode(reader, DecoderContext.builder().build()));
        } finally {
            reader.close();
        }
    }

    @Test
    void cloneShouldMakeADeepCopy() {
        BsonDocument cloned = byteBufDocument.clone();
        assertEquals(byteBufDocument, cloned);
        assertEquals(1, documentByteBuf.getReferenceCount());
    }

    @Test
    void shouldSerializeAndDeserialize() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);

        oos.writeObject(byteBufDocument);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object deserializedDocument = ois.readObject();

        assertEquals(byteBufDocument, deserializedDocument);
        assertEquals(1, documentByteBuf.getReferenceCount());
    }

    @Test
    void toJsonShouldReturnEquivalent() {
        assertEquals(document.toJson(), byteBufDocument.toJson());
        assertEquals(1, documentByteBuf.getReferenceCount());
    }

    @Test
    void toJsonShouldBeCallableMultipleTimes() {
        byteBufDocument.toJson();
        byteBufDocument.toJson();
        assertEquals(1, documentByteBuf.getReferenceCount());
    }

    @Test
    void sizeShouldBeCallableMultipleTimes() {
        byteBufDocument.size();
        byteBufDocument.size();
        assertEquals(1, documentByteBuf.getReferenceCount());
    }

    @Test
    void toJsonShouldRespectJsonWriteSettings() {
        JsonWriterSettings settings = JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build();
        assertEquals(document.toJson(settings), byteBufDocument.toJson(settings));
    }

    @Test
    void toJsonShouldReturnEquivalentWhenAByteBufBsonDocumentIsNestedInABsonDocument() {
        BsonDocument topLevel = new BsonDocument("nested", byteBufDocument);
        assertEquals(new BsonDocument("nested", document).toJson(), topLevel.toJson());
    }
}
