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

package org.bson.json;

import org.bson.BsonBinary;
import org.bson.BsonDbPointer;
import org.bson.BsonDocumentReader;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.StringWriter;
import java.util.stream.IntStream;

import static org.bson.BsonHelper.documentWithValuesOfEveryType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonWriterUnitTest {

    private StringWriter stringWriter;
    private JsonWriter writer;
    private String jsonWithValuesOfEveryType;

    @BeforeEach
    void setUp() {
        stringWriter = new StringWriter();
        writer = new JsonWriter(stringWriter, JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build());
        jsonWithValuesOfEveryType = documentWithValuesOfEveryType().toJson(JsonWriterSettings.builder().build());
    }

    @Test
    void shouldPipeAllTypes() {
        BsonDocumentReader reader = new BsonDocumentReader(documentWithValuesOfEveryType());
        writer.pipe(reader);
        assertEquals(documentWithValuesOfEveryType().toJson(), stringWriter.toString());
    }

    static IntStream maxLengths() {
        return IntStream.rangeClosed(0, 1000);
    }

    @ParameterizedTest
    @MethodSource("maxLengths")
    void shouldPipeAllTypesWithCappedLength(final int maxLength) {
        StringWriter sw = new StringWriter();
        JsonWriter w = new JsonWriter(sw, JsonWriterSettings.builder().maxLength(maxLength).build());
        BsonDocumentReader reader = new BsonDocumentReader(documentWithValuesOfEveryType());
        String json = documentWithValuesOfEveryType().toJson(JsonWriterSettings.builder().build());

        w.pipe(reader);

        if (maxLength == 0) {
            assertEquals(json, sw.toString());
        } else {
            assertEquals(json.substring(0, Math.min(maxLength, json.length())), sw.toString());
        }
        assertTrue(w.isTruncated() || maxLength == 0 || json.length() <= maxLength);
    }

    @Test
    void shouldThrowAnExceptionWhenWritingNullName() {
        writer.writeStartDocument();
        assertThrows(IllegalArgumentException.class, () -> writer.writeName(null));
    }

    @Test
    void shouldThrowAnExceptionWhenWritingNullValue() {
        writer.writeStartDocument();
        writer.writeName("v");
        assertThrows(IllegalArgumentException.class, () -> writer.writeString(null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeBinaryData(null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeDBPointer(null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeDecimal128(null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeJavaScript(null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeJavaScriptWithScope(null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeObjectId(null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeRegularExpression(null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeSymbol(null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeTimestamp(null));
    }

    @Test
    void shouldThrowAnExceptionWhenWritingNullMemberValue() {
        writer.writeStartDocument();
        assertThrows(IllegalArgumentException.class, () -> writer.writeString("v", null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeBinaryData("v", null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeDBPointer("v", null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeDecimal128("v", null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeJavaScript("v", null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeJavaScriptWithScope("v", null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeObjectId("v", null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeRegularExpression("v", null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeSymbol("v", null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeTimestamp("v", null));
    }

    @Test
    void shouldThrowAnExceptionWhenWritingNullMemberName() {
        writer.writeStartDocument();
        assertThrows(IllegalArgumentException.class, () -> writer.writeStartDocument(null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeStartArray(null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeString(null, "s"));
        assertThrows(IllegalArgumentException.class, () -> writer.writeBinaryData(null, new BsonBinary(new byte[1])));
        assertThrows(IllegalArgumentException.class, () -> writer.writeDBPointer(null, new BsonDbPointer("a.b", new ObjectId())));
        assertThrows(IllegalArgumentException.class, () -> writer.writeDecimal128(null, Decimal128.NaN));
        assertThrows(IllegalArgumentException.class, () -> writer.writeJavaScript(null, "function() {}"));
        assertThrows(IllegalArgumentException.class, () -> writer.writeJavaScriptWithScope(null, "function() {}"));
        assertThrows(IllegalArgumentException.class, () -> writer.writeObjectId(null, new ObjectId()));
        assertThrows(IllegalArgumentException.class, () -> writer.writeRegularExpression(null, new BsonRegularExpression(".*")));
        assertThrows(IllegalArgumentException.class, () -> writer.writeSymbol(null, "s"));
        assertThrows(IllegalArgumentException.class, () -> writer.writeTimestamp(null, new BsonTimestamp(42, 1)));
        assertThrows(IllegalArgumentException.class, () -> writer.writeNull(null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeBoolean(null, true));
        assertThrows(IllegalArgumentException.class, () -> writer.writeInt32(null, 1));
        assertThrows(IllegalArgumentException.class, () -> writer.writeInt64(null, 1L));
        assertThrows(IllegalArgumentException.class, () -> writer.writeDouble(null, 2));
        assertThrows(IllegalArgumentException.class, () -> writer.writeDateTime(null, 100));
        assertThrows(IllegalArgumentException.class, () -> writer.writeMaxKey(null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeMinKey(null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeUndefined(null));
    }
}
