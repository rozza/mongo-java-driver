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

package org.bson.codecs;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonDouble;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.ByteBufNIO;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BsonDocumentCodecTest {

    @Test
    void shouldEncodeAndDecodeAllDefaultTypes() {
        BsonDocument doc = new BsonDocument(asList(
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
                new BsonElement("javaScript", new BsonJavaScript("int i = 0;")),
                new BsonElement("objectId", new BsonObjectId(new ObjectId())),
                new BsonElement("codeWithScope", new BsonJavaScriptWithScope("int x = y",
                        new BsonDocument("y", new BsonInt32(1)))),
                new BsonElement("regex", new BsonRegularExpression("^test.*regex.*xyz$", "i")),
                new BsonElement("symbol", new BsonSymbol("ruby stuff")),
                new BsonElement("timestamp", new BsonTimestamp(0x12345678, 5)),
                new BsonElement("undefined", new BsonUndefined()),
                new BsonElement("binary", new BsonBinary((byte) 80, new byte[]{5, 4, 3, 2, 1})),
                new BsonElement("array", new BsonArray(asList(new BsonInt32(1), new BsonInt64(2L),
                        new BsonBoolean(true),
                        new BsonArray(asList(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3))),
                        new BsonDocument("a", new BsonInt64(2L))))),
                new BsonElement("document", new BsonDocument("a", new BsonInt32(1)))
        ));

        BsonBinaryWriter writer = new BsonBinaryWriter(new BasicOutputBuffer());
        new BsonDocumentCodec().encode(writer, doc, EncoderContext.builder().build());
        BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(
                new ByteBufNIO(ByteBuffer.wrap(((BasicOutputBuffer) writer.getBsonOutput()).toByteArray()))));
        BsonDocument decodedDoc = new BsonDocumentCodec().decode(reader, DecoderContext.builder().build());

        assertEquals(doc.get("null"), decodedDoc.get("null"));
        assertEquals(doc.get("int32"), decodedDoc.get("int32"));
        assertEquals(doc.get("int64"), decodedDoc.get("int64"));
        assertEquals(doc.get("decimal128"), decodedDoc.get("decimal128"));
        assertEquals(doc.get("boolean"), decodedDoc.get("boolean"));
        assertEquals(doc.get("date"), decodedDoc.get("date"));
        assertEquals(doc.get("double"), decodedDoc.get("double"));
        assertEquals(doc.get("minKey"), decodedDoc.get("minKey"));
        assertEquals(doc.get("maxKey"), decodedDoc.get("maxKey"));
        assertEquals(doc.get("javaScript"), decodedDoc.get("javaScript"));
        assertEquals(doc.get("codeWithScope"), decodedDoc.get("codeWithScope"));
        assertEquals(doc.get("objectId"), decodedDoc.get("objectId"));
        assertEquals(doc.get("regex"), decodedDoc.get("regex"));
        assertEquals(doc.get("string"), decodedDoc.get("string"));
        assertEquals(doc.get("symbol"), decodedDoc.get("symbol"));
        assertEquals(doc.get("timestamp"), decodedDoc.get("timestamp"));
        assertEquals(doc.get("undefined"), decodedDoc.get("undefined"));
        assertEquals(doc.get("binary"), decodedDoc.get("binary"));
        assertEquals(doc.get("array"), decodedDoc.get("array"));
        assertEquals(doc.get("document"), decodedDoc.get("document"));
    }

    @Test
    void shouldRespectEncodeIdFirstPropertyInEncoderContext() {
        BsonDocument doc = new BsonDocument(asList(
                new BsonElement("x", new BsonInt32(2)),
                new BsonElement("_id", new BsonInt32(2)),
                new BsonElement("nested", new BsonDocument(asList(
                        new BsonElement("x", new BsonInt32(2)),
                        new BsonElement("_id", new BsonInt32(2))))),
                new BsonElement("array", new BsonArray(asList(new BsonDocument(asList(
                        new BsonElement("x", new BsonInt32(2)),
                        new BsonElement("_id", new BsonInt32(2)))))))
        ));

        BsonDocument encodedDocument = new BsonDocument();
        new BsonDocumentCodec().encode(new BsonDocumentWriter(encodedDocument), doc,
                EncoderContext.builder().isEncodingCollectibleDocument(true).build());

        assertEquals(asList("_id", "x", "nested", "array"), new ArrayList<>(encodedDocument.keySet()));
        assertEquals(asList("x", "_id"), new ArrayList<>(encodedDocument.getDocument("nested").keySet()));
        assertEquals(asList("x", "_id"),
                new ArrayList<>(encodedDocument.getArray("array").get(0).asDocument().keySet()));

        encodedDocument.clear();
        new BsonDocumentCodec().encode(new BsonDocumentWriter(encodedDocument), doc,
                EncoderContext.builder().isEncodingCollectibleDocument(false).build());

        assertEquals(asList("x", "_id", "nested", "array"), new ArrayList<>(encodedDocument.keySet()));
        assertEquals(asList("x", "_id"), new ArrayList<>(encodedDocument.getDocument("nested").keySet()));
        assertEquals(asList("x", "_id"),
                new ArrayList<>(encodedDocument.getArray("array").get(0).asDocument().keySet()));
    }

    @Test
    void shouldEncodeNestedRawDocuments() {
        BsonDocument doc = new BsonDocument("a", BsonBoolean.TRUE);
        RawBsonDocument rawDoc = new RawBsonDocument(doc, new BsonDocumentCodec());
        BsonDocument docWithNestedRawDoc = new BsonDocument("a", rawDoc)
                .append("b", new BsonArray(asList(rawDoc)));

        BsonDocument encodedDocument = new BsonDocument();
        new BsonDocumentCodec().encode(new BsonDocumentWriter(encodedDocument), docWithNestedRawDoc,
                EncoderContext.builder().isEncodingCollectibleDocument(true).build());

        assertEquals(docWithNestedRawDoc, encodedDocument);
    }

    @Test
    void shouldDetermineIfDocumentHasAnId() {
        assertFalse(new BsonDocumentCodec().documentHasId(new BsonDocument()));
        assertTrue(new BsonDocumentCodec().documentHasId(new BsonDocument("_id", new BsonInt32(1))));
    }

    @Test
    void shouldGetDocumentId() {
        assertNull(new BsonDocumentCodec().getDocumentId(new BsonDocument()));
        assertEquals(new BsonInt32(1),
                new BsonDocumentCodec().getDocumentId(new BsonDocument("_id", new BsonInt32(1))));
    }

    @Test
    void shouldGenerateDocumentIdIfAbsent() {
        BsonDocument document = new BsonDocument();
        document = new BsonDocumentCodec().generateIdIfAbsentFromDocument(document);
        assertInstanceOf(BsonObjectId.class, document.get("_id"));
    }

    @Test
    void shouldNotGenerateDocumentIdIfPresent() {
        BsonDocument document = new BsonDocument("_id", new BsonInt32(1));
        document = new BsonDocumentCodec().generateIdIfAbsentFromDocument(document);
        assertEquals(new BsonInt32(1), document.get("_id"));
    }
}
