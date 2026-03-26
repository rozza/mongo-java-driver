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
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.NoSuchElementException;

import static org.bson.BsonHelper.documentWithValuesOfEveryType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BsonDocumentUnitTest {

    @Test
    @DisplayName("conversion methods should behave correctly for the happy path")
    void conversionMethodsShouldBehaveCorrectlyForTheHappyPath() {
        BsonNull bsonNull = new BsonNull();
        BsonInt32 bsonInt32 = new BsonInt32(42);
        BsonInt64 bsonInt64 = new BsonInt64(52L);
        BsonDecimal128 bsonDecimal128 = new BsonDecimal128(Decimal128.parse("1.0"));
        BsonBoolean bsonBoolean = new BsonBoolean(true);
        BsonDateTime bsonDateTime = new BsonDateTime(new Date().getTime());
        BsonDouble bsonDouble = new BsonDouble(62.0);
        BsonString bsonString = new BsonString("the fox ...");
        BsonMinKey minKey = new BsonMinKey();
        BsonMaxKey maxKey = new BsonMaxKey();
        BsonJavaScript javaScript = new BsonJavaScript("int i = 0;");
        BsonObjectId objectId = new BsonObjectId(new ObjectId());
        BsonJavaScriptWithScope scope = new BsonJavaScriptWithScope("int x = y", new BsonDocument("y", new BsonInt32(1)));
        BsonRegularExpression regularExpression = new BsonRegularExpression("^test.*regex.*xyz$", "i");
        BsonSymbol symbol = new BsonSymbol("ruby stuff");
        BsonTimestamp timestamp = new BsonTimestamp(0x12345678, 5);
        BsonUndefined undefined = new BsonUndefined();
        BsonBinary binary = new BsonBinary((byte) 80, new byte[]{5, 4, 3, 2, 1});
        BsonArray bsonArray = new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt64(2L), new BsonBoolean(true),
                new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3))),
                new BsonDocument("a", new BsonInt64(2L))));
        BsonDocument bsonDocument = new BsonDocument("a", new BsonInt32(1));

        BsonDocument root = new BsonDocument(Arrays.asList(
                new BsonElement("null", bsonNull),
                new BsonElement("int32", bsonInt32),
                new BsonElement("int64", bsonInt64),
                new BsonElement("decimal128", bsonDecimal128),
                new BsonElement("boolean", bsonBoolean),
                new BsonElement("date", bsonDateTime),
                new BsonElement("double", bsonDouble),
                new BsonElement("string", bsonString),
                new BsonElement("minKey", minKey),
                new BsonElement("maxKey", maxKey),
                new BsonElement("javaScript", javaScript),
                new BsonElement("objectId", objectId),
                new BsonElement("codeWithScope", scope),
                new BsonElement("regex", regularExpression),
                new BsonElement("symbol", symbol),
                new BsonElement("timestamp", timestamp),
                new BsonElement("undefined", undefined),
                new BsonElement("binary", binary),
                new BsonElement("array", bsonArray),
                new BsonElement("document", bsonDocument)
        ));

        assertTrue(root.isNull("null"));
        assertSame(bsonInt32, root.getInt32("int32"));
        assertSame(bsonInt64, root.getInt64("int64"));
        assertSame(bsonDecimal128, root.getDecimal128("decimal128"));
        assertSame(bsonBoolean, root.getBoolean("boolean"));
        assertSame(bsonDateTime, root.getDateTime("date"));
        assertSame(bsonDouble, root.getDouble("double"));
        assertSame(bsonString, root.getString("string"));
        assertSame(objectId, root.getObjectId("objectId"));
        assertSame(regularExpression, root.getRegularExpression("regex"));
        assertSame(binary, root.getBinary("binary"));
        assertSame(timestamp, root.getTimestamp("timestamp"));
        assertSame(bsonArray, root.getArray("array"));
        assertSame(bsonDocument, root.getDocument("document"));
        assertSame(bsonInt32, root.getNumber("int32"));
        assertSame(bsonInt64, root.getNumber("int64"));
        assertSame(bsonDouble, root.getNumber("double"));

        assertSame(bsonInt32, root.getInt32("int32", new BsonInt32(2)));
        assertSame(bsonInt64, root.getInt64("int64", new BsonInt64(4)));
        assertSame(bsonDecimal128, root.getDecimal128("decimal128", new BsonDecimal128(Decimal128.parse("4.0"))));
        assertSame(bsonDouble, root.getDouble("double", new BsonDouble(343.0)));
        assertSame(bsonBoolean, root.getBoolean("boolean", new BsonBoolean(false)));
        assertSame(bsonDateTime, root.getDateTime("date", new BsonDateTime(3453)));
        assertSame(bsonString, root.getString("string", new BsonString("df")));
        assertSame(objectId, root.getObjectId("objectId", new BsonObjectId(new ObjectId())));
        assertSame(regularExpression, root.getRegularExpression("regex", new BsonRegularExpression("^foo", "i")));
        assertSame(binary, root.getBinary("binary", new BsonBinary(new byte[5])));
        assertSame(timestamp, root.getTimestamp("timestamp", new BsonTimestamp(343, 23)));
        assertSame(bsonArray, root.getArray("array", new BsonArray()));
        assertSame(bsonDocument, root.getDocument("document", new BsonDocument()));
        assertSame(bsonInt32, root.getNumber("int32", new BsonInt32(2)));
        assertSame(bsonInt64, root.getNumber("int64", new BsonInt32(2)));
        assertSame(bsonDouble, root.getNumber("double", new BsonInt32(2)));

        assertSame(bsonInt32, root.get("int32").asInt32());
        assertSame(bsonInt64, root.get("int64").asInt64());
        assertSame(bsonDecimal128, root.get("decimal128").asDecimal128());
        assertSame(bsonBoolean, root.get("boolean").asBoolean());
        assertSame(bsonDateTime, root.get("date").asDateTime());
        assertSame(bsonDouble, root.get("double").asDouble());
        assertSame(bsonString, root.get("string").asString());
        assertSame(objectId, root.get("objectId").asObjectId());
        assertSame(timestamp, root.get("timestamp").asTimestamp());
        assertSame(binary, root.get("binary").asBinary());
        assertSame(bsonArray, root.get("array").asArray());
        assertSame(bsonDocument, root.get("document").asDocument());

        assertTrue(root.isInt32("int32"));
        assertTrue(root.isNumber("int32"));
        assertTrue(root.isInt64("int64"));
        assertTrue(root.isDecimal128("decimal128"));
        assertTrue(root.isNumber("int64"));
        assertTrue(root.isBoolean("boolean"));
        assertTrue(root.isDateTime("date"));
        assertTrue(root.isDouble("double"));
        assertTrue(root.isNumber("double"));
        assertTrue(root.isString("string"));
        assertTrue(root.isObjectId("objectId"));
        assertTrue(root.isTimestamp("timestamp"));
        assertTrue(root.isBinary("binary"));
        assertTrue(root.isArray("array"));
        assertTrue(root.isDocument("document"));
    }

    @Test
    @DisplayName("is<type> methods should return false for missing keys")
    void isTypeMethodsShouldReturnFalseForMissingKeys() {
        BsonDocument root = new BsonDocument();

        assertFalse(root.isNull("null"));
        assertFalse(root.isNumber("number"));
        assertFalse(root.isInt32("int32"));
        assertFalse(root.isInt64("int64"));
        assertFalse(root.isDecimal128("decimal128"));
        assertFalse(root.isBoolean("boolean"));
        assertFalse(root.isDateTime("date"));
        assertFalse(root.isDouble("double"));
        assertFalse(root.isString("string"));
        assertFalse(root.isObjectId("objectId"));
        assertFalse(root.isTimestamp("timestamp"));
        assertFalse(root.isBinary("binary"));
        assertFalse(root.isArray("array"));
        assertFalse(root.isDocument("document"));
    }

    @Test
    @DisplayName("get methods should return default values for missing keys")
    void getMethodsShouldReturnDefaultValuesForMissingKeys() {
        BsonNull bsonNull = new BsonNull();
        BsonInt32 bsonInt32 = new BsonInt32(42);
        BsonInt64 bsonInt64 = new BsonInt64(52L);
        BsonDecimal128 bsonDecimal128 = new BsonDecimal128(Decimal128.parse("1.0"));
        BsonBoolean bsonBoolean = new BsonBoolean(true);
        BsonDateTime bsonDateTime = new BsonDateTime(new Date().getTime());
        BsonDouble bsonDouble = new BsonDouble(62.0);
        BsonString bsonString = new BsonString("the fox ...");
        BsonObjectId objectId = new BsonObjectId(new ObjectId());
        BsonRegularExpression regularExpression = new BsonRegularExpression("^test.*regex.*xyz$", "i");
        BsonTimestamp timestamp = new BsonTimestamp(0x12345678, 5);
        BsonBinary binary = new BsonBinary((byte) 80, new byte[]{5, 4, 3, 2, 1});
        BsonArray bsonArray = new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt64(2L), new BsonBoolean(true),
                new BsonDecimal128(Decimal128.parse("4.0")),
                new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3))),
                new BsonDocument("a", new BsonInt64(2L))));
        BsonDocument bsonDocument = new BsonDocument("a", new BsonInt32(1));
        BsonDocument root = new BsonDocument();

        assertSame(bsonNull, root.get("m", bsonNull));
        assertSame(bsonArray, root.getArray("m", bsonArray));
        assertSame(bsonBoolean, root.getBoolean("m", bsonBoolean));
        assertSame(bsonDateTime, root.getDateTime("m", bsonDateTime));
        assertSame(bsonDocument, root.getDocument("m", bsonDocument));
        assertSame(bsonDouble, root.getDouble("m", bsonDouble));
        assertSame(bsonInt32, root.getInt32("m", bsonInt32));
        assertSame(bsonInt64, root.getInt64("m", bsonInt64));
        assertSame(bsonDecimal128, root.getDecimal128("m", bsonDecimal128));
        assertSame(bsonString, root.getString("m", bsonString));
        assertSame(objectId, root.getObjectId("m", objectId));
        assertSame(bsonString, root.getString("m", bsonString));
        assertSame(timestamp, root.getTimestamp("m", timestamp));
        assertSame(bsonInt32, root.getNumber("m", bsonInt32));
        assertSame(regularExpression, root.getRegularExpression("m", regularExpression));
        assertSame(binary, root.getBinary("m", binary));
    }

    @Test
    @DisplayName("clone should make a deep copy of all mutable BsonValue types")
    void cloneShouldMakeDeepCopy() {
        BsonDocument document = new BsonDocument("d", new BsonDocument().append("i2", new BsonInt32(1)))
                .append("i", new BsonInt32(2))
                .append("a", new BsonArray(Arrays.asList(new BsonInt32(3),
                        new BsonArray(Arrays.asList(new BsonInt32(11))),
                        new BsonDocument("i3", new BsonInt32(6)),
                        new BsonBinary(new byte[]{1, 2, 3}),
                        new BsonJavaScriptWithScope("code", new BsonDocument("a", new BsonInt32(4))))))
                .append("b", new BsonBinary(new byte[]{1, 2, 3}))
                .append("js", new BsonJavaScriptWithScope("code", new BsonDocument("a", new BsonInt32(4))));

        BsonDocument clone = document.clone();

        assertEquals(document, clone);
        assertNotSame(document, clone);
        assertSame(clone.get("i"), document.get("i"));
        assertNotSame(clone.get("d"), document.get("d"));
        assertNotSame(clone.get("a"), document.get("a"));
        assertNotSame(clone.get("b"), document.get("b"));
        assertNotSame(clone.get("b").asBinary().getData(), document.get("b").asBinary().getData());
        assertNotSame(clone.get("js").asJavaScriptWithScope().getScope(),
                document.get("js").asJavaScriptWithScope().getScope());

        assertSame(clone.get("a").asArray().get(0), document.get("a").asArray().get(0));
        assertNotSame(clone.get("a").asArray().get(1), document.get("a").asArray().get(1));
        assertNotSame(clone.get("a").asArray().get(2), document.get("a").asArray().get(2));
        assertNotSame(clone.get("a").asArray().get(3), document.get("a").asArray().get(3));
        assertNotSame(clone.get("a").asArray().get(3).asBinary().getData(),
                document.get("a").asArray().get(3).asBinary().getData());
        assertNotSame(clone.get("a").asArray().get(4), document.get("a").asArray().get(4));
        assertNotSame(clone.get("a").asArray().get(4).asJavaScriptWithScope().getScope(),
                document.get("a").asArray().get(4).asJavaScriptWithScope().getScope());
    }

    @Test
    @DisplayName("get methods should throw if key is absent")
    void getMethodsShouldThrowIfKeyIsAbsent() {
        BsonDocument root = new BsonDocument();

        assertThrows(BsonInvalidOperationException.class, () -> root.getInt32("int32"));
        assertThrows(BsonInvalidOperationException.class, () -> root.getInt64("int64"));
        assertThrows(BsonInvalidOperationException.class, () -> root.getDecimal128("decimal128"));
        assertThrows(BsonInvalidOperationException.class, () -> root.getBoolean("boolean"));
        assertThrows(BsonInvalidOperationException.class, () -> root.getDateTime("date"));
        assertThrows(BsonInvalidOperationException.class, () -> root.getDouble("double"));
        assertThrows(BsonInvalidOperationException.class, () -> root.getString("string"));
        assertThrows(BsonInvalidOperationException.class, () -> root.getObjectId("objectId"));
        assertThrows(BsonInvalidOperationException.class, () -> root.getRegularExpression("regex"));
        assertThrows(BsonInvalidOperationException.class, () -> root.getBinary("binary"));
        assertThrows(BsonInvalidOperationException.class, () -> root.getTimestamp("timestamp"));
        assertThrows(BsonInvalidOperationException.class, () -> root.getArray("array"));
        assertThrows(BsonInvalidOperationException.class, () -> root.getDocument("document"));
        assertThrows(BsonInvalidOperationException.class, () -> root.getNumber("int32"));
    }

    @Test
    @DisplayName("should get first key")
    void shouldGetFirstKey() {
        BsonDocument document = new BsonDocument("i", new BsonInt32(2));
        assertEquals("i", document.getFirstKey());
    }

    @Test
    @DisplayName("getFirstKey should throw NoSuchElementException if the document is empty")
    void getFirstKeyShouldThrowForEmptyDocument() {
        BsonDocument document = new BsonDocument();
        assertThrows(NoSuchElementException.class, document::getFirstKey);
    }

    @Test
    @DisplayName("should create BsonReader")
    void shouldCreateBsonReader() {
        BsonDocument document = documentWithValuesOfEveryType();
        BsonReader reader = document.asBsonReader();
        try {
            assertEquals(document, new BsonDocumentCodec().decode(reader, DecoderContext.builder().build()));
        } finally {
            reader.close();
        }
    }

    @Test
    @DisplayName("should serialize and deserialize")
    void shouldSerializeAndDeserialize() throws Exception {
        BsonDocument document = new BsonDocument("d", new BsonDocument().append("i2", new BsonInt32(1)))
                .append("i", new BsonInt32(2))
                .append("d", new BsonDecimal128(Decimal128.parse("1.0")))
                .append("a", new BsonArray(Arrays.asList(new BsonInt32(3),
                        new BsonArray(Arrays.asList(new BsonInt32(11))),
                        new BsonDocument("i3", new BsonInt32(6)),
                        new BsonBinary(new byte[]{1, 2, 3}),
                        new BsonJavaScriptWithScope("code", new BsonDocument("a", new BsonInt32(4))))))
                .append("b", new BsonBinary(new byte[]{1, 2, 3}))
                .append("js", new BsonJavaScriptWithScope("code", new BsonDocument("a", new BsonInt32(4))));

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(document);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object deserializedDocument = ois.readObject();

        assertEquals(document, deserializedDocument);
    }
}
