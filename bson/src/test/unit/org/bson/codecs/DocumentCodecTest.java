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

import org.bson.BsonBinaryReader;
import org.bson.BsonBinarySubType;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDbPointer;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.BsonWriter;
import org.bson.ByteBufNIO;
import org.bson.Document;
import org.bson.BinaryVector;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.BsonInput;
import org.bson.io.ByteBufferBsonInput;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWithScope;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DocumentCodecTest {
    private BasicOutputBuffer buffer;
    private BsonBinaryWriter writer;

    @BeforeEach
    public void setUp() throws Exception {
        buffer = new BasicOutputBuffer();
        writer = new BsonBinaryWriter(buffer);
    }

    @AfterEach
    public void tearDown() {
        writer.close();
    }

    @Test
    public void testPrimitiveBSONTypeCodecs() throws IOException {
        DocumentCodec documentCodec = new DocumentCodec();
        Document doc = new Document();
        doc.put("oid", new ObjectId());
        doc.put("integer", 1);
        doc.put("long", 2L);
        doc.put("string", "hello");
        doc.put("double", 3.2);
        doc.put("decimal", Decimal128.parse("0.100"));
        doc.put("binary", new Binary(BsonBinarySubType.USER_DEFINED, new byte[]{0, 1, 2, 3}));
        doc.put("date", new Date(1000));
        doc.put("boolean", true);
        doc.put("code", new Code("var i = 0"));
        doc.put("minkey", new MinKey());
        doc.put("maxkey", new MaxKey());
        doc.put("vectorFloat", BinaryVector.floatVector(new float[]{1.1f, 2.2f, 3.3f}));
        doc.put("vectorInt8", BinaryVector.int8Vector(new byte[]{10, 20, 30, 40}));
        doc.put("vectorPackedBit", BinaryVector.packedBitVector(new byte[]{(byte) 0b10101010, (byte) 0b01010101}, (byte) 3));
        //        doc.put("pattern", Pattern.compile("^hello"));  // TODO: Pattern doesn't override equals method!
        doc.put("null", null);

        documentCodec.encode(writer, doc, EncoderContext.builder().build());

        BsonInput bsonInput = createInputBuffer();
        Document decodedDocument = documentCodec.decode(new BsonBinaryReader(bsonInput), DecoderContext.builder().build());
        assertEquals(doc, decodedDocument);
    }

    @Test
    public void testIterableEncoding() throws IOException {
        DocumentCodec documentCodec = new DocumentCodec();
        Document doc = new Document()
                       .append("list", asList(1, 2, 3, 4, 5))
                       .append("set", new HashSet<>(asList(1, 2, 3, 4)));

        documentCodec.encode(writer, doc, EncoderContext.builder().build());

        BsonInput bsonInput = createInputBuffer();
        Document decodedDocument = documentCodec.decode(new BsonBinaryReader(bsonInput), DecoderContext.builder().build());
        assertEquals(new Document()
                     .append("list", asList(1, 2, 3, 4, 5))
                     .append("set", asList(1, 2, 3, 4)), decodedDocument);
    }

    @Test
    public void testCodeWithScopeEncoding() throws IOException {
        DocumentCodec documentCodec = new DocumentCodec();
        Document doc = new Document();
        doc.put("theCode", new CodeWithScope("javaScript code", new Document("fieldNameOfScope", "valueOfScope")));

        documentCodec.encode(writer, doc, EncoderContext.builder().build());

        Document decodedDocument = documentCodec.decode(new BsonBinaryReader(createInputBuffer()), DecoderContext.builder().build());
        assertEquals(doc, decodedDocument);
    }

    @Test
    public void testIterableContainingOtherIterableEncoding() throws IOException {
        DocumentCodec documentCodec = new DocumentCodec();
        Document doc = new Document();
        List<List<Integer>> listOfLists = asList(asList(1), asList(2));
        doc.put("array", listOfLists);

        documentCodec.encode(writer, doc, EncoderContext.builder().build());

        BsonInput bsonInput = createInputBuffer();
        Document decodedDocument = documentCodec.decode(new BsonBinaryReader(bsonInput), DecoderContext.builder().build());
        assertEquals(doc, decodedDocument);
    }

    @Test
    public void testIterableContainingDocumentsEncoding() throws IOException {
        DocumentCodec documentCodec = new DocumentCodec();
        Document doc = new Document();
        List<Document> listOfDocuments = asList(new Document("intVal", 1), new Document("anotherInt", 2));
        doc.put("array", listOfDocuments);

        documentCodec.encode(writer, doc, EncoderContext.builder().build());

        BsonInput bsonInput = createInputBuffer();
        Document decodedDocument = documentCodec.decode(new BsonBinaryReader(bsonInput), DecoderContext.builder().build());
        assertEquals(doc, decodedDocument);
    }

    @Test
    public void testNestedDocumentEncoding() throws IOException {
        DocumentCodec documentCodec = new DocumentCodec();
        Document doc = new Document();
        doc.put("nested", new Document("x", 1));

        documentCodec.encode(writer, doc, EncoderContext.builder().build());

        BsonInput bsonInput = createInputBuffer();
        Document decodedDocument = documentCodec.decode(new BsonBinaryReader(bsonInput), DecoderContext.builder().build());
        assertEquals(doc, decodedDocument);
    }

    @Test
    public void shouldNotGenerateIdIfPresent() {
        DocumentCodec documentCodec = new DocumentCodec();
        Document document = new Document("_id", 1);
        assertTrue(documentCodec.documentHasId(document));
        document = documentCodec.generateIdIfAbsentFromDocument(document);
        assertTrue(documentCodec.documentHasId(document));
        assertEquals(new BsonInt32(1), documentCodec.getDocumentId(document));
    }

    @Test
    public void shouldGenerateIdIfAbsent() {
        DocumentCodec documentCodec = new DocumentCodec();
        Document document = new Document();
        assertFalse(documentCodec.documentHasId(document));
        document = documentCodec.generateIdIfAbsentFromDocument(document);
        assertTrue(documentCodec.documentHasId(document));
        assertEquals(BsonObjectId.class, documentCodec.getDocumentId(document).getClass());
    }

    @Test
    public void shouldEncodeAndDecodeAllDefaultTypesWithBsonDocumentWriter() {
        CodecRegistry registry = fromRegistries(fromCodecs(new UuidCodec(STANDARD)),
                fromProviders(asList(new ValueCodecProvider(), new CollectionCodecProvider(),
                        new BsonValueCodecProvider(), new DocumentCodecProvider(), new MapCodecProvider())));

        Document originalDocument = new Document();
        originalDocument.put("null", null);
        originalDocument.put("int32", 42);
        originalDocument.put("int64", 52L);
        originalDocument.put("booleanTrue", true);
        originalDocument.put("booleanFalse", false);
        originalDocument.put("date", new Date());
        originalDocument.put("dbPointer", new BsonDbPointer("foo.bar", new ObjectId()));
        originalDocument.put("double", 62.0);
        originalDocument.put("minKey", new MinKey());
        originalDocument.put("maxKey", new MaxKey());
        originalDocument.put("code", new Code("int i = 0;"));
        originalDocument.put("codeWithScope", new CodeWithScope("int x = y", new Document("y", 1)));
        originalDocument.put("objectId", new ObjectId());
        originalDocument.put("regex", new BsonRegularExpression("^test.*regex.*xyz$", "i"));
        originalDocument.put("string", "the fox ...");
        originalDocument.put("symbol", new Symbol("ruby stuff"));
        originalDocument.put("timestamp", new BsonTimestamp(0x12345678, 5));
        originalDocument.put("undefined", new BsonUndefined());
        originalDocument.put("binary", new Binary((byte) 0x80, new byte[]{5, 4, 3, 2, 1}));
        originalDocument.put("array", asList(1, 1L, true, asList(1, 2, 3), new Document("a", 1), null));
        originalDocument.put("uuid", new UUID(1L, 2L));
        originalDocument.put("document", new Document("a", 2));
        originalDocument.put("map", new Document("a", 1).append("b", 2));
        originalDocument.put("atomicLong", new AtomicLong(1));
        originalDocument.put("atomicInteger", new AtomicInteger(1));
        originalDocument.put("atomicBoolean", new AtomicBoolean(true));

        BsonDocument bsonDoc = new BsonDocument();
        BsonDocumentWriter docWriter = new BsonDocumentWriter(bsonDoc);
        new DocumentCodec(registry).withUuidRepresentation(STANDARD)
                .encode(docWriter, originalDocument, EncoderContext.builder().build());
        BsonDocumentReader reader = new BsonDocumentReader(bsonDoc);
        Document decodedDoc = new DocumentCodec(registry).withUuidRepresentation(STANDARD)
                .decode(reader, DecoderContext.builder().build());

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
        assertEquals(originalDocument.get("uuid"), decodedDoc.get("uuid"));
        assertEquals(originalDocument.get("array"), decodedDoc.get("array"));
        assertEquals(originalDocument.get("document"), decodedDoc.get("document"));
        assertEquals(originalDocument.get("map"), decodedDoc.get("map"));
        assertEquals(((AtomicLong) originalDocument.get("atomicLong")).get(), decodedDoc.get("atomicLong"));
        assertEquals(((AtomicInteger) originalDocument.get("atomicInteger")).get(), decodedDoc.get("atomicInteger"));
        assertEquals(((AtomicBoolean) originalDocument.get("atomicBoolean")).get(), decodedDoc.get("atomicBoolean"));
    }

    @Test
    public void shouldEncodeAndDecodeAllDefaultTypesWithBsonBinaryWriter() {
        CodecRegistry registry = fromRegistries(fromCodecs(new UuidCodec(STANDARD)),
                fromProviders(asList(new ValueCodecProvider(), new CollectionCodecProvider(),
                        new BsonValueCodecProvider(), new DocumentCodecProvider(), new MapCodecProvider())));

        Document originalDocument = new Document();
        originalDocument.put("null", null);
        originalDocument.put("int32", 42);
        originalDocument.put("int64", 52L);
        originalDocument.put("booleanTrue", true);
        originalDocument.put("booleanFalse", false);
        originalDocument.put("date", new Date());
        originalDocument.put("dbPointer", new BsonDbPointer("foo.bar", new ObjectId()));
        originalDocument.put("double", 62.0);
        originalDocument.put("minKey", new MinKey());
        originalDocument.put("maxKey", new MaxKey());
        originalDocument.put("code", new Code("int i = 0;"));
        originalDocument.put("codeWithScope", new CodeWithScope("int x = y", new Document("y", 1)));
        originalDocument.put("objectId", new ObjectId());
        originalDocument.put("regex", new BsonRegularExpression("^test.*regex.*xyz$", "i"));
        originalDocument.put("string", "the fox ...");
        originalDocument.put("symbol", new Symbol("ruby stuff"));
        originalDocument.put("timestamp", new BsonTimestamp(0x12345678, 5));
        originalDocument.put("undefined", new BsonUndefined());
        originalDocument.put("binary", new Binary((byte) 0x80, new byte[]{5, 4, 3, 2, 1}));
        originalDocument.put("array", asList(1, 1L, true, asList(1, 2, 3), new Document("a", 1), null));
        originalDocument.put("uuid", new UUID(1L, 2L));
        originalDocument.put("document", new Document("a", 2));
        originalDocument.put("map", new Document("a", 1).append("b", 2));
        originalDocument.put("atomicLong", new AtomicLong(1));
        originalDocument.put("atomicInteger", new AtomicInteger(1));
        originalDocument.put("atomicBoolean", new AtomicBoolean(true));

        BasicOutputBuffer binaryBuffer = new BasicOutputBuffer();
        BsonBinaryWriter binaryWriter = new BsonBinaryWriter(binaryBuffer);
        new DocumentCodec(registry).withUuidRepresentation(STANDARD)
                .encode(binaryWriter, originalDocument, EncoderContext.builder().build());
        BsonBinaryReader binaryReader = new BsonBinaryReader(new ByteBufferBsonInput(
                new ByteBufNIO(ByteBuffer.wrap(binaryBuffer.toByteArray()))));
        Document decodedDoc = new DocumentCodec(registry).withUuidRepresentation(STANDARD)
                .decode(binaryReader, DecoderContext.builder().build());

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
        assertEquals(originalDocument.get("uuid"), decodedDoc.get("uuid"));
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
    public void shouldDecodeBinarySubtypesForUuidThatAreNot16BytesIntoBinary(Binary value, byte[] bytes) {
        BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bytes));
        Document document = new DocumentCodec().decode(reader, DecoderContext.builder().build());
        assertEquals(value, document.get("f"));
    }

    static Stream<Arguments> binarySubtype3ForUuid() {
        return Stream.of(
                Arguments.of(JAVA_LEGACY,
                        UUID.fromString("08070605-0403-0201-100f-0e0d0c0b0a09"),
                        new byte[]{29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0}),
                Arguments.of(C_SHARP_LEGACY,
                        UUID.fromString("04030201-0605-0807-090a-0b0c0d0e0f10"),
                        new byte[]{29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0}),
                Arguments.of(PYTHON_LEGACY,
                        UUID.fromString("01020304-0506-0708-090a-0b0c0d0e0f10"),
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
    public void shouldDecodeBinarySubtype3ForUuid(UuidRepresentation representation, Object value, byte[] bytes) {
        BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bytes));
        Document document = new DocumentCodec(fromCodecs(new UuidCodec(representation), new BinaryCodec()))
                .withUuidRepresentation(representation)
                .decode(reader, DecoderContext.builder().build());
        assertEquals(value, document.get("f"));
    }

    static Stream<Arguments> binarySubtype4ForUuid() {
        return Stream.of(
                Arguments.of(STANDARD,
                        UUID.fromString("01020304-0506-0708-090a-0b0c0d0e0f10"),
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
    public void shouldDecodeBinarySubtype4ForUuid(UuidRepresentation representation, Object value, byte[] bytes) {
        BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bytes));
        Document document = new DocumentCodec(fromCodecs(new UuidCodec(representation), new BinaryCodec()))
                .withUuidRepresentation(representation)
                .decode(reader, DecoderContext.builder().build());
        assertEquals(value, document.get("f"));
    }

    @Test
    public void shouldRespectEncodeIdFirstPropertyInEncoderContext() {
        Document originalDocument = new Document("x", 2)
                .append("_id", 2)
                .append("nested", new Document("x", 2).append("_id", 2))
                .append("array", asList(new Document("x", 2).append("_id", 2)));

        BsonDocument encodedDocument = new BsonDocument();
        new DocumentCodec().encode(new BsonDocumentWriter(encodedDocument), originalDocument,
                EncoderContext.builder().isEncodingCollectibleDocument(true).build());

        assertEquals(asList("_id", "x", "nested", "array"), new ArrayList<>(encodedDocument.keySet()));
        assertEquals(asList("x", "_id"), new ArrayList<>(encodedDocument.getDocument("nested").keySet()));
        assertEquals(asList("x", "_id"),
                new ArrayList<>(encodedDocument.getArray("array").get(0).asDocument().keySet()));

        encodedDocument.clear();
        new DocumentCodec().encode(new BsonDocumentWriter(encodedDocument), originalDocument,
                EncoderContext.builder().isEncodingCollectibleDocument(false).build());

        assertEquals(asList("x", "_id", "nested", "array"), new ArrayList<>(encodedDocument.keySet()));
        assertEquals(asList("x", "_id"), new ArrayList<>(encodedDocument.getDocument("nested").keySet()));
        assertEquals(asList("x", "_id"),
                new ArrayList<>(encodedDocument.getArray("array").get(0).asDocument().keySet()));
    }

    @Test
    public void shouldApplyTransformerToDecodedValues() {
        DocumentCodec codec = new DocumentCodec(
                fromProviders(asList(new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider())),
                new BsonTypeClassMap(),
                value -> 5);
        Document doc = codec.decode(new BsonDocumentReader(new BsonDocument("_id", new BsonInt32(1))),
                DecoderContext.builder().build());
        assertEquals(5, doc.get("_id"));
    }

    @Test
    public void shouldDetermineIfIdIsPresent() {
        assertTrue(new DocumentCodec().documentHasId(new Document("_id", 1)));
        assertFalse(new DocumentCodec().documentHasId(new Document()));
    }

    @Test
    public void shouldGetIdIfPresent() {
        assertEquals(new BsonInt32(1), new DocumentCodec().getDocumentId(new Document("_id", 1)));
        assertEquals(new BsonInt32(1), new DocumentCodec().getDocumentId(new Document("_id", new BsonInt32(1))));
    }

    @Test
    public void shouldThrowIfGettingIdWhenAbsent() {
        assertThrows(IllegalStateException.class, () -> new DocumentCodec().getDocumentId(new Document()));
    }

    // TODO: factor into common base class;
    private BsonInput createInputBuffer() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        buffer.pipe(baos);
        return new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(baos.toByteArray())));
    }
}
