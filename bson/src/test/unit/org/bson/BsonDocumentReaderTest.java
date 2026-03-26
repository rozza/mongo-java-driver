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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BsonDocumentReaderTest {

    private BsonDocument nullDoc;

    @BeforeEach
    void setUp() {
        nullDoc = new BsonDocument(Arrays.asList(
                new BsonElement("null", new BsonNull())
        ));
    }

    @Test
    @DisplayName("should read all types")
    void shouldReadAllTypes() {
        BsonDocument doc = new BsonDocument(Arrays.asList(
                new BsonElement("null", new BsonNull()),
                new BsonElement("int32", new BsonInt32(42)),
                new BsonElement("int64", new BsonInt64(52L)),
                new BsonElement("decimal128", new BsonDecimal128(Decimal128.parse("1.0"))),
                new BsonElement("boolean", new BsonBoolean(true)),
                new BsonElement("date", new BsonDateTime(new Date().getTime())),
                new BsonElement("double", new BsonDouble(62.0)),
                new BsonElement("string", new BsonString("the fox ...")),
                new BsonElement("minKey", new BsonMinKey()),
                new BsonElement("maxKey", new BsonMaxKey()),
                new BsonElement("dbPointer", new BsonDbPointer("test.test", new ObjectId())),
                new BsonElement("code", new BsonJavaScript("int i = 0;")),
                new BsonElement("codeWithScope", new BsonJavaScriptWithScope("x", new BsonDocument("x", new BsonInt32(1)))),
                new BsonElement("objectId", new BsonObjectId(new ObjectId())),
                new BsonElement("regex", new BsonRegularExpression("^test.*regex.*xyz$", "i")),
                new BsonElement("symbol", new BsonSymbol("ruby stuff")),
                new BsonElement("timestamp", new BsonTimestamp(0x12345678, 5)),
                new BsonElement("undefined", new BsonUndefined()),
                new BsonElement("binary", new BsonBinary((byte) 80, new byte[]{5, 4, 3, 2, 1})),
                new BsonElement("array", new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt64(2L), new BsonBoolean(true),
                        new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3))),
                        new BsonDocument("a", new BsonInt64(2L))))),
                new BsonElement("document", new BsonDocument("a", new BsonInt32(1)))
        ));

        BsonDocument decodedDoc = new BsonDocumentCodec().decode(new BsonDocumentReader(doc), DecoderContext.builder().build());
        assertEquals(doc, decodedDoc);
    }

    @Test
    @DisplayName("should fail, ReadBSONType can only be called when State is TYPE, not VALUE")
    void shouldFailReadBsonTypeWhenStateIsValue() {
        BsonDocumentReader reader = new BsonDocumentReader(nullDoc);

        assertThrows(BsonInvalidOperationException.class, () -> {
            reader.readStartDocument();
            reader.readBsonType();
            reader.readName();
            reader.readBsonType();
        });
    }

    @Test
    @DisplayName("should fail, ReadBSONType can only be called when State is TYPE, not NAME")
    void shouldFailReadBsonTypeWhenStateIsName() {
        BsonDocumentReader reader = new BsonDocumentReader(nullDoc);

        assertThrows(BsonInvalidOperationException.class, () -> {
            reader.readStartDocument();
            reader.readBsonType();
            reader.readBsonType();
        });
    }
}
