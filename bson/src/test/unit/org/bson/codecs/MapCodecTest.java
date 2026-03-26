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
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDateTime;
import org.bson.BsonDbPointer;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.BsonInt32;
import org.bson.BsonReader;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.BsonWriter;
import org.bson.ByteBufNIO;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.jsr310.Jsr310CodecProvider;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWithScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.ParameterizedType;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.bson.UuidRepresentation.C_SHARP_LEGACY;
import static org.bson.UuidRepresentation.JAVA_LEGACY;
import static org.bson.UuidRepresentation.PYTHON_LEGACY;
import static org.bson.UuidRepresentation.STANDARD;
import static org.bson.UuidRepresentation.UNSPECIFIED;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MapCodecTest {

    private static final CodecRegistry REGISTRY = fromRegistries(fromCodecs(new UuidCodec(JAVA_LEGACY)),
            fromProviders(asList(new ValueCodecProvider(), new BsonValueCodecProvider(),
                    new DocumentCodecProvider(), new CollectionCodecProvider(), new MapCodecProvider())));

    @Test
    void shouldEncodeAndDecodeAllDefaultTypesWithBsonDocumentWriter() {
        Map<String, Object> originalDocument = new LinkedHashMap<>();
        populateTestDocument(originalDocument);

        BsonDocument bsonDoc = new BsonDocument();
        BsonDocumentWriter writer = new BsonDocumentWriter(bsonDoc);
        new MapCodec(REGISTRY, new BsonTypeClassMap(), null, Map.class)
                .encode(writer, originalDocument, EncoderContext.builder().build());
        BsonDocumentReader reader = new BsonDocumentReader(bsonDoc);
        Map<String, Object> decodedDoc = new MapCodec(REGISTRY, new BsonTypeClassMap(), null, Map.class)
                .decode(reader, DecoderContext.builder().build());

        assertMapFields(originalDocument, decodedDoc);
    }

    @Test
    void shouldEncodeAndDecodeAllDefaultTypesWithBsonBinaryWriter() {
        Map<String, Object> originalDocument = new LinkedHashMap<>();
        populateTestDocument(originalDocument);

        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonBinaryWriter binaryWriter = new BsonBinaryWriter(outputBuffer);
        new MapCodec(REGISTRY, new BsonTypeClassMap(), null, Map.class)
                .encode(binaryWriter, originalDocument, EncoderContext.builder().build());
        BsonBinaryReader binaryReader = new BsonBinaryReader(new ByteBufferBsonInput(
                new ByteBufNIO(ByteBuffer.wrap(outputBuffer.toByteArray()))));
        Map<String, Object> decodedDoc = new MapCodec(REGISTRY, new BsonTypeClassMap(), null, Map.class)
                .decode(binaryReader, DecoderContext.builder().build());

        assertMapFields(originalDocument, decodedDoc);
    }

    private void populateTestDocument(Map<String, Object> doc) {
        doc.put("null", null);
        doc.put("int32", 42);
        doc.put("int64", 52L);
        doc.put("booleanTrue", true);
        doc.put("booleanFalse", false);
        doc.put("date", new Date());
        doc.put("dbPointer", new BsonDbPointer("foo.bar", new ObjectId()));
        doc.put("double", 62.0);
        doc.put("minKey", new MinKey());
        doc.put("maxKey", new MaxKey());
        doc.put("code", new Code("int i = 0;"));
        doc.put("codeWithScope", new CodeWithScope("int x = y", new Document("y", 1)));
        doc.put("objectId", new ObjectId());
        doc.put("regex", new BsonRegularExpression("^test.*regex.*xyz$", "i"));
        doc.put("string", "the fox ...");
        doc.put("symbol", new Symbol("ruby stuff"));
        doc.put("timestamp", new BsonTimestamp(0x12345678, 5));
        doc.put("undefined", new BsonUndefined());
        doc.put("binary", new Binary((byte) 0x80, new byte[]{5, 4, 3, 2, 1}));
        doc.put("array", asList(1, 1L, true, asList(1, 2, 3), new Document("a", 1), null));
        doc.put("document", new Document("a", 2));
        Map<String, Object> innerMap = new LinkedHashMap<>();
        innerMap.put("a", 1);
        innerMap.put("b", 2);
        doc.put("map", innerMap);
        doc.put("atomicLong", new AtomicLong(1));
        doc.put("atomicInteger", new AtomicInteger(1));
        doc.put("atomicBoolean", new AtomicBoolean(true));
    }

    private void assertMapFields(Map<String, Object> originalDocument, Map<String, Object> decodedDoc) {
        assertEquals(originalDocument.get("null"), decodedDoc.get("null"));
        assertEquals(originalDocument.get("int32"), decodedDoc.get("int32"));
        assertEquals(originalDocument.get("int64"), decodedDoc.get("int64"));
        assertEquals(originalDocument.get("booleanTrue"), decodedDoc.get("booleanTrue"));
        assertEquals(originalDocument.get("booleanFalse"), decodedDoc.get("booleanFalse"));
        assertEquals(originalDocument.get("date"), decodedDoc.get("date"));
        assertEquals(originalDocument.get("dbPointer"), decodedDoc.get("dbPointer"));
        assertEquals(originalDocument.get("double"), decodedDoc.get("double"));
        assertEquals(originalDocument.get("minKey"), decodedDoc.get("minKey"));
        assertEquals(originalDocument.get("maxKey"), decodedDoc.get("maxKey"));
        assertEquals(originalDocument.get("code"), decodedDoc.get("code"));
        assertEquals(originalDocument.get("codeWithScope"), decodedDoc.get("codeWithScope"));
        assertEquals(originalDocument.get("objectId"), decodedDoc.get("objectId"));
        assertEquals(originalDocument.get("regex"), decodedDoc.get("regex"));
        assertEquals(originalDocument.get("string"), decodedDoc.get("string"));
        assertEquals(originalDocument.get("symbol"), decodedDoc.get("symbol"));
        assertEquals(originalDocument.get("timestamp"), decodedDoc.get("timestamp"));
        assertEquals(originalDocument.get("undefined"), decodedDoc.get("undefined"));
        assertEquals(originalDocument.get("binary"), decodedDoc.get("binary"));
        assertEquals(originalDocument.get("array"), decodedDoc.get("array"));
        assertEquals(originalDocument.get("document"), decodedDoc.get("document"));
        assertEquals(originalDocument.get("map"), decodedDoc.get("map"));
        assertEquals(((AtomicLong) originalDocument.get("atomicLong")).get(), decodedDoc.get("atomicLong"));
        assertEquals(((AtomicInteger) originalDocument.get("atomicInteger")).get(), decodedDoc.get("atomicInteger"));
        assertEquals(((AtomicBoolean) originalDocument.get("atomicBoolean")).get(), decodedDoc.get("atomicBoolean"));
    }

    static Stream<Arguments> binarySubtypesNot16Bytes() {
        return Stream.of(
                Arguments.of(new Binary((byte) 0x03, new byte[]{115, 116, 11}),
                        new byte[]{16, 0, 0, 0, 5, 102, 0, 3, 0, 0, 0, 3, 115, 116, 11, 0}),
                Arguments.of(new Binary((byte) 0x04, new byte[]{115, 116, 11}),
                        new byte[]{16, 0, 0, 0, 5, 102, 0, 3, 0, 0, 0, 4, 115, 116, 11, 0})
        );
    }

    @ParameterizedTest
    @MethodSource("binarySubtypesNot16Bytes")
    void shouldDecodeBinarySubtypesForUuidThatAreNot16BytesIntoBinary(Binary value, byte[] bytes) {
        BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bytes));
        Document document = new DocumentCodec().decode(reader, DecoderContext.builder().build());
        assertEquals(value, document.get("f"));
    }

    static Stream<Arguments> binarySubtype3ForUuid() {
        return Stream.of(
                Arguments.of(JAVA_LEGACY,
                        java.util.UUID.fromString("08070605-0403-0201-100f-0e0d0c0b0a09"),
                        new byte[]{29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0}),
                Arguments.of(C_SHARP_LEGACY,
                        java.util.UUID.fromString("04030201-0605-0807-090a-0b0c0d0e0f10"),
                        new byte[]{29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0}),
                Arguments.of(PYTHON_LEGACY,
                        java.util.UUID.fromString("01020304-0506-0708-090a-0b0c0d0e0f10"),
                        new byte[]{29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0}),
                Arguments.of(STANDARD,
                        new Binary((byte) 3, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
                        new byte[]{29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0}),
                Arguments.of(UNSPECIFIED,
                        new Binary((byte) 3, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
                        new byte[]{29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0})
        );
    }

    @ParameterizedTest
    @MethodSource("binarySubtype3ForUuid")
    void shouldDecodeBinarySubtype3ForUuid(UuidRepresentation representation, Object value, byte[] bytes) {
        BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bytes));
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) new MapCodec(fromCodecs(new UuidCodec(representation), new BinaryCodec()),
                new BsonTypeClassMap(), null, Map.class)
                .withUuidRepresentation(representation)
                .decode(reader, DecoderContext.builder().build());
        assertEquals(value, map.get("f"));
    }

    static Stream<Arguments> binarySubtype4ForUuid() {
        return Stream.of(
                Arguments.of(STANDARD,
                        java.util.UUID.fromString("01020304-0506-0708-090a-0b0c0d0e0f10"),
                        new byte[]{29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0}),
                Arguments.of(JAVA_LEGACY,
                        new Binary((byte) 4, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
                        new byte[]{29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0}),
                Arguments.of(C_SHARP_LEGACY,
                        new Binary((byte) 4, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
                        new byte[]{29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0}),
                Arguments.of(PYTHON_LEGACY,
                        new Binary((byte) 4, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
                        new byte[]{29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0}),
                Arguments.of(UNSPECIFIED,
                        new Binary((byte) 4, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
                        new byte[]{29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0})
        );
    }

    @ParameterizedTest
    @MethodSource("binarySubtype4ForUuid")
    void shouldDecodeBinarySubtype4ForUuid(UuidRepresentation representation, Object value, byte[] bytes) {
        BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bytes));
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) new MapCodec(fromCodecs(new UuidCodec(representation), new BinaryCodec()),
                new BsonTypeClassMap(), null, Map.class)
                .withUuidRepresentation(representation)
                .decode(reader, DecoderContext.builder().build());
        assertEquals(value, map.get("f"));
    }

    @Test
    void shouldApplyTransformerToDecodedValues() {
        MapCodec codec = new MapCodec(
                fromProviders(asList(new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider())),
                new BsonTypeClassMap(), value -> 5, Map.class);
        Map<String, Object> doc = codec.decode(
                new BsonDocumentReader(new BsonDocument("_id", new BsonInt32(1))),
                DecoderContext.builder().build());
        assertEquals(5, doc.get("_id"));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    static Stream<Arguments> mapTypes() {
        return Stream.of(
                Arguments.of(Map.class, HashMap.class),
                Arguments.of(NavigableMap.class, TreeMap.class),
                Arguments.of(AbstractMap.class, HashMap.class),
                Arguments.of(HashMap.class, HashMap.class),
                Arguments.of(TreeMap.class, TreeMap.class),
                Arguments.of(WeakHashMap.class, WeakHashMap.class)
        );
    }

    @ParameterizedTest
    @MethodSource("mapTypes")
    @SuppressWarnings({"rawtypes", "unchecked"})
    void shouldDecodeToSpecifiedGenericClass(Class mapType, Class actualType) {
        BsonDocument doc = new BsonDocument("_id", new BsonInt32(1));

        MapCodec codec = new MapCodec(fromProviders(asList(new ValueCodecProvider())),
                new BsonTypeClassMap(), null, mapType);
        Map map = codec.decode(new BsonDocumentReader(doc), DecoderContext.builder().build());

        assertEquals(mapType, codec.getEncoderClass());
        assertEquals(actualType, map.getClass());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void shouldParameterize() throws NoSuchMethodException {
        Codec codec = fromProviders(new Jsr310CodecProvider(), REGISTRY).get(
                Map.class,
                asList(((ParameterizedType) Container.class.getDeclaredMethod("getInstants").getGenericReturnType())
                        .getActualTypeArguments()));

        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        BsonDocumentReader reader = new BsonDocumentReader(writer.getDocument());
        Map<String, List<Instant>> instants = new LinkedHashMap<>();
        instants.put("firstMap", asList(Instant.ofEpochMilli(1), Instant.ofEpochMilli(2)));
        instants.put("secondMap", asList(Instant.ofEpochMilli(3), Instant.ofEpochMilli(4)));

        writer.writeStartDocument();
        writer.writeName("instants");
        codec.encode(writer, instants, EncoderContext.builder().build());
        writer.writeEndDocument();

        assertEquals(new BsonDocument()
                        .append("instants", new BsonDocument()
                                .append("firstMap", new BsonArray(asList(new BsonDateTime(1), new BsonDateTime(2))))
                                .append("secondMap", new BsonArray(asList(new BsonDateTime(3), new BsonDateTime(4))))),
                writer.getDocument());

        reader.readStartDocument();
        reader.readName("instants");
        Object decodedInstants = codec.decode(reader, DecoderContext.builder().build());

        assertEquals(instants, decodedInstants);
    }

    @SuppressWarnings("unused")
    static class Container {
        private final Map<String, List<Instant>> instants = new HashMap<>();

        Map<String, List<Instant>> getInstants() {
            return instants;
        }
    }
}
