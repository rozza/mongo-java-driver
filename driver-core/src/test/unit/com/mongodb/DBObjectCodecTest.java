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

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.BsonInt32;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.UuidRepresentation;
import org.bson.codecs.BinaryCodec;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.UuidCodec;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.CodeWScope;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;

import static org.bson.UuidRepresentation.C_SHARP_LEGACY;
import static org.bson.UuidRepresentation.JAVA_LEGACY;
import static org.bson.UuidRepresentation.PYTHON_LEGACY;
import static org.bson.UuidRepresentation.STANDARD;
import static org.bson.UuidRepresentation.UNSPECIFIED;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class DBObjectCodecTest {

    private final BsonDocument bsonDoc = new BsonDocument();
    private final CodecRegistry codecRegistry = fromRegistries(fromCodecs(new UuidCodec(JAVA_LEGACY)),
            fromProviders(Arrays.asList(new ValueCodecProvider(), new DBObjectCodecProvider(),
                    new BsonValueCodecProvider())));
    private final DBObjectCodec dbObjectCodec = (DBObjectCodec) new DBObjectCodec(codecRegistry).withUuidRepresentation(JAVA_LEGACY);

    @Test
    @DisplayName("default registry should include necessary providers")
    void defaultRegistryShouldIncludeProviders() {
        CodecRegistry registry = DBObjectCodec.getDefaultRegistry();

        assertNotNull(registry.get(Integer.class));
        assertNotNull(registry.get(BsonInt32.class));
        assertNotNull(registry.get(BSONTimestamp.class));
        assertNotNull(registry.get(BasicDBObject.class));
    }

    @Test
    @DisplayName("should encode with default registry")
    void shouldEncodeWithDefaultRegistry() {
        BsonDocument document = new BsonDocument();
        BasicDBObject dBObject = new BasicDBObject("a", 0).append("b", new BsonInt32(1)).append("c", new BSONTimestamp());

        new DBObjectCodec().encode(new BsonDocumentWriter(document), dBObject, EncoderContext.builder().build());

        assertEquals(new BsonDocument("a", new BsonInt32(0)).append("b", new BsonInt32(1)).append("c", new BsonTimestamp()),
                document);
    }

    @Test
    @DisplayName("should encode and decode UUID as UUID")
    void shouldEncodeAndDecodeUUID() {
        UUID uuid = UUID.fromString("01020304-0506-0708-090a-0b0c0d0e0f10");
        BasicDBObject doc = new BasicDBObject("uuid", uuid);

        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build());

        assertEquals(new BsonBinary(BsonBinarySubType.UUID_LEGACY,
                new byte[]{8, 7, 6, 5, 4, 3, 2, 1, 16, 15, 14, 13, 12, 11, 10, 9}), bsonDoc.getBinary("uuid"));

        DBObject decodedUuid = dbObjectCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build());
        assertEquals(uuid, decodedUuid.get("uuid"));
    }

    @ParameterizedTest
    @MethodSource("binarySubtypeNot16BytesData")
    @DisplayName("should decode binary subtypes for UUID that are not 16 bytes into Binary")
    void shouldDecodeBinarySubtypesNot16Bytes(final Object value, final byte[] bytes) {
        BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bytes));
        DBObject document = new DBObjectCodec().decode(reader, DecoderContext.builder().build());
        assertEquals(value, document.get("f"));
    }

    static Stream<Object[]> binarySubtypeNot16BytesData() {
        return Stream.of(
                new Object[]{new Binary((byte) 0x03, new byte[]{115, 116, 11}),
                        new byte[]{16, 0, 0, 0, 5, 102, 0, 3, 0, 0, 0, 3, 115, 116, 11, 0}},
                new Object[]{new Binary((byte) 0x04, new byte[]{115, 116, 11}),
                        new byte[]{16, 0, 0, 0, 5, 102, 0, 3, 0, 0, 0, 4, 115, 116, 11, 0}}
        );
    }

    @ParameterizedTest
    @MethodSource("binarySubtype3UuidData")
    @DisplayName("should decode binary subtype 3 for UUID")
    void shouldDecodeBinarySubtype3ForUuid(final UuidRepresentation representation, final Object value, final byte[] bytes) {
        BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bytes));
        DBObject document = new DBObjectCodec(fromCodecs(new UuidCodec(representation), new BinaryCodec()))
                .withUuidRepresentation(representation)
                .decode(reader, DecoderContext.builder().build());
        assertEquals(value, document.get("f"));
    }

    static Stream<Object[]> binarySubtype3UuidData() {
        byte[] bytes = new byte[]{29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                13, 14, 15, 16, 0};
        byte[] binaryData = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
        return Stream.of(
                new Object[]{JAVA_LEGACY, UUID.fromString("08070605-0403-0201-100f-0e0d0c0b0a09"), bytes},
                new Object[]{C_SHARP_LEGACY, UUID.fromString("04030201-0605-0807-090a-0b0c0d0e0f10"), bytes},
                new Object[]{PYTHON_LEGACY, UUID.fromString("01020304-0506-0708-090a-0b0c0d0e0f10"), bytes},
                new Object[]{STANDARD, new Binary((byte) 3, binaryData), bytes},
                new Object[]{UNSPECIFIED, new Binary((byte) 3, binaryData), bytes}
        );
    }

    @Test
    @DisplayName("should encode and decode UUID as UUID with alternate UUID Codec")
    void shouldEncodeAndDecodeUUIDWithAlternateCodec() {
        DBObjectCodec codecWithAlternateUUIDCodec = (DBObjectCodec) new DBObjectCodec(
                fromRegistries(fromCodecs(new UuidCodec(STANDARD)), codecRegistry))
                .withUuidRepresentation(STANDARD);
        UUID uuid = UUID.fromString("01020304-0506-0708-090a-0b0c0d0e0f10");
        BasicDBObject doc = new BasicDBObject("uuid", uuid);

        codecWithAlternateUUIDCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build());

        assertEquals(new BsonBinary(BsonBinarySubType.UUID_STANDARD,
                new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}), bsonDoc.getBinary("uuid"));

        DBObject decodedDoc = codecWithAlternateUUIDCodec.decode(new BsonDocumentReader(bsonDoc),
                DecoderContext.builder().build());
        assertEquals(uuid, decodedDoc.get("uuid"));
    }

    @ParameterizedTest
    @MethodSource("binarySubtype4UuidData")
    @DisplayName("should decode binary subtype 4 for UUID")
    void shouldDecodeBinarySubtype4ForUuid(final UuidRepresentation representation, final Object value, final byte[] bytes) {
        BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bytes));
        DBObject document = new DBObjectCodec().withUuidRepresentation(representation)
                .decode(reader, DecoderContext.builder().build());
        assertEquals(value, document.get("f"));
    }

    static Stream<Object[]> binarySubtype4UuidData() {
        byte[] bytes = new byte[]{29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                13, 14, 15, 16, 0};
        byte[] binaryData = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
        return Stream.of(
                new Object[]{STANDARD, UUID.fromString("01020304-0506-0708-090a-0b0c0d0e0f10"), bytes},
                new Object[]{JAVA_LEGACY, new Binary((byte) 4, binaryData), bytes},
                new Object[]{C_SHARP_LEGACY, new Binary((byte) 4, binaryData), bytes},
                new Object[]{PYTHON_LEGACY, new Binary((byte) 4, binaryData), bytes},
                new Object[]{UNSPECIFIED, new Binary((byte) 4, binaryData), bytes}
        );
    }

    @Test
    @DisplayName("should encode and decode byte array value as binary")
    void shouldEncodeAndDecodeByteArray() {
        byte[] array = new byte[]{0, 1, 2, 4, 4};
        BasicDBObject doc = new BasicDBObject("byteArray", array);

        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build());
        assertEquals(new BsonBinary(array), bsonDoc.getBinary("byteArray"));

        DBObject decodedDoc = dbObjectCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build());
        assertEquals(array, (byte[]) decodedDoc.get("byteArray"));
    }

    @Test
    @DisplayName("should encode and decode Binary value as binary")
    void shouldEncodeAndDecodeBinaryValue() {
        byte subType = (byte) 42;
        byte[] array = new byte[]{0, 1, 2, 4, 4};
        BasicDBObject doc = new BasicDBObject("byteArray", new Binary(subType, array));

        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build());
        assertEquals(new BsonBinary(subType, array), bsonDoc.getBinary("byteArray"));

        DBObject decodedDoc = dbObjectCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build());
        assertEquals(new Binary(subType, array), decodedDoc.get("byteArray"));
    }

    @Test
    @DisplayName("should encode Symbol to BsonSymbol and decode BsonSymbol to String")
    void shouldEncodeSymbol() {
        Symbol symbol = new Symbol("symbol");
        BasicDBObject doc = new BasicDBObject("symbol", symbol);

        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build());
        assertEquals(new BsonSymbol("symbol"), bsonDoc.get("symbol"));

        DBObject decodedSymbol = dbObjectCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build());
        assertEquals(symbol.toString(), decodedSymbol.get("symbol"));
    }

    @Test
    @DisplayName("should encode java.sql.Date as date")
    void shouldEncodeSqlDate() {
        java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
        BasicDBObject doc = new BasicDBObject("d", sqlDate);

        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build());
        DBObject decodedDoc = dbObjectCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build());

        assertEquals(sqlDate.getTime(), ((java.util.Date) decodedDoc.get("d")).getTime());
    }

    @Test
    @DisplayName("should encode java.sql.Timestamp as date")
    void shouldEncodeSqlTimestamp() {
        Timestamp sqlTimestamp = new Timestamp(System.currentTimeMillis());
        BasicDBObject doc = new BasicDBObject("d", sqlTimestamp);

        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build());
        DBObject decodedDoc = dbObjectCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build());

        assertEquals(sqlTimestamp.getTime(), ((java.util.Date) decodedDoc.get("d")).getTime());
    }

    @Test
    @DisplayName("should encode collectible document with _id")
    void shouldEncodeCollectibleDocumentWithId() {
        BasicDBObject doc = new BasicDBObject("y", 1).append("_id", new BasicDBObject("x", 1));

        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc,
                EncoderContext.builder().isEncodingCollectibleDocument(true).build());

        assertEquals(new BsonDocument("_id", new BsonDocument("x", new BsonInt32(1))).append("y", new BsonInt32(1)),
                bsonDoc);
    }

    @Test
    @DisplayName("should encode collectible document without _id")
    void shouldEncodeCollectibleDocumentWithoutId() {
        BasicDBObject doc = new BasicDBObject("y", 1);

        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc,
                EncoderContext.builder().isEncodingCollectibleDocument(true).build());

        assertEquals(new BsonDocument("y", new BsonInt32(1)), bsonDoc);
    }

    @Test
    @DisplayName("should encode all types")
    void shouldEncodeAllTypes() {
        ObjectId id = new ObjectId();
        DBRef dbRef = new DBRef("c", 1);
        BasicDBObject mapDoc = new BasicDBObject("f", 1);
        BasicDBObject doc = new BasicDBObject("_id", id)
                .append("n", null)
                .append("r", dbRef)
                .append("m", mapDoc)
                .append("i", Arrays.asList(1, 2))
                .append("c", new CodeWScope("c", new BasicDBObject("f", 1)))
                .append("b", new byte[0])
                .append("a", new Object[]{1, 2})
                .append("s", new Symbol("s"));

        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build());

        assertEquals(new BsonDocument("_id", new BsonObjectId(id))
                        .append("n", new BsonNull())
                        .append("r", new BsonDocument("$ref", new BsonString("c")).append("$id", new BsonInt32(1)))
                        .append("m", new BsonDocument("f", new BsonInt32(1)))
                        .append("i", new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt32(2))))
                        .append("c", new BsonJavaScriptWithScope("c", new BsonDocument("f", new BsonInt32(1))))
                        .append("b", new BsonBinary(new byte[0]))
                        .append("a", new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt32(2))))
                        .append("s", new BsonSymbol("s")),
                bsonDoc);
    }
}
