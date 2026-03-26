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

import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.Binary;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.bson.BsonDocument.parse;
import static org.bson.UuidRepresentation.C_SHARP_LEGACY;
import static org.bson.UuidRepresentation.JAVA_LEGACY;
import static org.bson.UuidRepresentation.PYTHON_LEGACY;
import static org.bson.UuidRepresentation.STANDARD;
import static org.bson.UuidRepresentation.UNSPECIFIED;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.junit.jupiter.api.Assertions.assertEquals;

class IterableCodecTest {

    private static final CodecRegistry REGISTRY = fromRegistries(fromCodecs(new UuidCodec(JAVA_LEGACY)),
            fromProviders(new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider(),
                    new IterableCodecProvider(), new MapCodecProvider()));

    @Test
    void shouldHaveIterableEncodingClass() {
        IterableCodec codec = new IterableCodec(REGISTRY, new BsonTypeClassMap(), null);
        assertEquals(Iterable.class, codec.getEncoderClass());
    }

    @Test
    void shouldEncodeAnIterableToABsonArray() {
        IterableCodec codec = new IterableCodec(REGISTRY, new BsonTypeClassMap(), null);
        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

        writer.writeStartDocument();
        writer.writeName("array");
        codec.encode(writer, asList(1, 2, 3, null), EncoderContext.builder().build());
        writer.writeEndDocument();

        assertEquals(parse("{array : [1, 2, 3, null]}"), writer.getDocument());
    }

    @Test
    void shouldDecodeABsonArrayToAnIterable() {
        IterableCodec codec = new IterableCodec(REGISTRY, new BsonTypeClassMap(), null);
        BsonDocumentReader reader = new BsonDocumentReader(parse("{array : [1, 2, 3, null]}"));

        reader.readStartDocument();
        reader.readName("array");
        List<?> iterable = (List<?>) codec.decode(reader, DecoderContext.builder().build());
        reader.readEndDocument();

        assertEquals(asList(1, 2, 3, null), iterable);
    }

    @Test
    void shouldDecodeABsonArrayOfArraysToAnIterableOfIterables() {
        IterableCodec codec = new IterableCodec(REGISTRY, new BsonTypeClassMap(), null);
        BsonDocumentReader reader = new BsonDocumentReader(parse("{array : [[1, 2], [3, 4, 5]]}"));

        reader.readStartDocument();
        reader.readName("array");
        List<?> iterable = (List<?>) codec.decode(reader, DecoderContext.builder().build());
        reader.readEndDocument();

        assertEquals(asList(asList(1, 2), asList(3, 4, 5)), iterable);
    }

    @Test
    void shouldUseProvidedTransformer() {
        IterableCodec codec = new IterableCodec(REGISTRY, new BsonTypeClassMap(), from -> from.toString());
        BsonDocumentReader reader = new BsonDocumentReader(parse("{array : [1, 2, 3]}"));

        reader.readStartDocument();
        reader.readName("array");
        List<?> iterable = (List<?>) codec.decode(reader, DecoderContext.builder().build());
        reader.readEndDocument();

        assertEquals(asList("1", "2", "3"), iterable);
    }

    static Stream<Arguments> binarySubtype3ForUuid() {
        return Stream.of(
                Arguments.of(JAVA_LEGACY,
                        asList(UUID.fromString("08070605-0403-0201-100f-0e0d0c0b0a09")),
                        "{\"array\": [{ \"$binary\" : \"AQIDBAUGBwgJCgsMDQ4PEA==\", \"$type\" : \"3\" }]}"),
                Arguments.of(C_SHARP_LEGACY,
                        asList(UUID.fromString("04030201-0605-0807-090a-0b0c0d0e0f10")),
                        "{\"array\": [{ \"$binary\" : \"AQIDBAUGBwgJCgsMDQ4PEA==\", \"$type\" : \"3\" }]}"),
                Arguments.of(PYTHON_LEGACY,
                        asList(UUID.fromString("01020304-0506-0708-090a-0b0c0d0e0f10")),
                        "{\"array\": [{ \"$binary\" : \"AQIDBAUGBwgJCgsMDQ4PEA==\", \"$type\" : \"3\" }]}"),
                Arguments.of(STANDARD,
                        asList(new Binary((byte) 3, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})),
                        "{\"array\": [{ \"$binary\" : \"AQIDBAUGBwgJCgsMDQ4PEA==\", \"$type\" : \"3\" }]}"),
                Arguments.of(UNSPECIFIED,
                        asList(new Binary((byte) 3, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})),
                        "{\"array\": [{ \"$binary\" : \"AQIDBAUGBwgJCgsMDQ4PEA==\", \"$type\" : \"3\" }]}")
        );
    }

    @ParameterizedTest
    @MethodSource("binarySubtype3ForUuid")
    void shouldDecodeBinarySubtype3ForUuid(UuidRepresentation representation, List<?> value, String document) {
        BsonDocumentReader reader = new BsonDocumentReader(parse(document));
        IterableCodec codec = (IterableCodec) new IterableCodec(
                fromCodecs(new UuidCodec(representation), new BinaryCodec()),
                new BsonTypeClassMap(), null)
                .withUuidRepresentation(representation);

        reader.readStartDocument();
        reader.readName("array");
        List<?> iterable = (List<?>) codec.decode(reader, DecoderContext.builder().build());
        reader.readEndDocument();

        assertEquals(value, iterable);
    }

    static Stream<Arguments> binarySubtype4ForUuid() {
        return Stream.of(
                Arguments.of(STANDARD,
                        asList(UUID.fromString("01020304-0506-0708-090a-0b0c0d0e0f10")),
                        "{\"array\": [{ \"$binary\" : \"AQIDBAUGBwgJCgsMDQ4PEA==\", \"$type\" : \"4\" }]}"),
                Arguments.of(JAVA_LEGACY,
                        asList(UUID.fromString("01020304-0506-0708-090a-0b0c0d0e0f10")),
                        "{\"array\": [{ \"$binary\" : \"CAcGBQQDAgEQDw4NDAsKCQ==\", \"$type\" : \"3\" }]}"),
                Arguments.of(C_SHARP_LEGACY,
                        asList(new Binary((byte) 4, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})),
                        "{\"array\": [{ \"$binary\" : \"AQIDBAUGBwgJCgsMDQ4PEA==\", \"$type\" : \"4\" }]}"),
                Arguments.of(PYTHON_LEGACY,
                        asList(new Binary((byte) 4, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})),
                        "{\"array\": [{ \"$binary\" : \"AQIDBAUGBwgJCgsMDQ4PEA==\", \"$type\" : \"4\" }]}"),
                Arguments.of(UNSPECIFIED,
                        asList(new Binary((byte) 4, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})),
                        "{\"array\": [{ \"$binary\" : \"AQIDBAUGBwgJCgsMDQ4PEA==\", \"$type\" : \"4\" }]}")
        );
    }

    @ParameterizedTest
    @MethodSource("binarySubtype4ForUuid")
    void shouldDecodeBinarySubtype4ForUuid(UuidRepresentation representation, List<?> value, String document) {
        BsonDocumentReader reader = new BsonDocumentReader(parse(document));
        IterableCodec codec = (IterableCodec) new IterableCodec(
                fromCodecs(new UuidCodec(representation), new BinaryCodec()),
                new BsonTypeClassMap(), null)
                .withUuidRepresentation(representation);

        reader.readStartDocument();
        reader.readName("array");
        List<?> iterable = (List<?>) codec.decode(reader, DecoderContext.builder().build());
        reader.readEndDocument();

        assertEquals(value, iterable);
    }
}
