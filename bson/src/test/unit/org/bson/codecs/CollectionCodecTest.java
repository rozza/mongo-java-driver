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
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.jsr310.Jsr310CodecProvider;
import org.bson.types.Binary;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.ParameterizedType;
import java.time.Instant;
import java.util.AbstractCollection;
import java.util.Collections;
import java.util.AbstractList;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
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

class CollectionCodecTest {

    private static final CodecRegistry REGISTRY = fromRegistries(fromCodecs(new UuidCodec(JAVA_LEGACY)),
            fromProviders(new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider(),
                    new CollectionCodecProvider(), new MapCodecProvider()));

    static Stream<Arguments> collectionTypes() {
        return Stream.of(
                Arguments.of(Collection.class, ArrayList.class),
                Arguments.of(List.class, ArrayList.class),
                Arguments.of(AbstractList.class, ArrayList.class),
                Arguments.of(AbstractCollection.class, ArrayList.class),
                Arguments.of(ArrayList.class, ArrayList.class),
                Arguments.of(Set.class, HashSet.class),
                Arguments.of(AbstractSet.class, HashSet.class),
                Arguments.of(HashSet.class, HashSet.class),
                Arguments.of(NavigableSet.class, TreeSet.class),
                Arguments.of(SortedSet.class, TreeSet.class),
                Arguments.of(TreeSet.class, TreeSet.class),
                Arguments.of(CopyOnWriteArrayList.class, CopyOnWriteArrayList.class)
        );
    }

    @ParameterizedTest
    @MethodSource("collectionTypes")
    @SuppressWarnings({"rawtypes", "unchecked"})
    void shouldDecodeToSpecifiedGenericClass(Class collectionType, Class decodedType) {
        BsonDocument doc = new BsonDocument("a", new BsonArray());

        CollectionCodec codec = new CollectionCodec(fromProviders(new ValueCodecProvider()),
                new BsonTypeClassMap(), null, collectionType);
        BsonDocumentReader reader = new BsonDocumentReader(doc);
        reader.readStartDocument();
        reader.readName("a");
        Collection<?> collection = (Collection<?>) codec.decode(reader, DecoderContext.builder().build());

        assertEquals(collectionType, codec.getEncoderClass());
        assertEquals(decodedType, collection.getClass());
    }

    @Test
    void shouldEncodeACollectionToABsonArray() {
        @SuppressWarnings("unchecked")
        CollectionCodec codec = new CollectionCodec<>(REGISTRY, new BsonTypeClassMap(), null, Collection.class);
        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

        writer.writeStartDocument();
        writer.writeName("array");
        codec.encode(writer, asList(1, 2, 3, null), EncoderContext.builder().build());
        writer.writeEndDocument();

        assertEquals(parse("{array : [1, 2, 3, null]}"), writer.getDocument());
    }

    @Test
    void shouldDecodeABsonArrayToACollection() {
        @SuppressWarnings("unchecked")
        CollectionCodec codec = new CollectionCodec<>(REGISTRY, new BsonTypeClassMap(), null, Collection.class);
        BsonDocumentReader reader = new BsonDocumentReader(parse("{array : [1, 2, 3, null]}"));

        reader.readStartDocument();
        reader.readName("array");
        Collection<?> collection = (Collection<?>) codec.decode(reader, DecoderContext.builder().build());
        reader.readEndDocument();

        assertEquals(asList(1, 2, 3, null), new ArrayList<>(collection));
    }

    @Test
    void shouldDecodeABsonArrayOfArraysToACollectionOfCollection() {
        @SuppressWarnings("unchecked")
        CollectionCodec codec = new CollectionCodec<>(REGISTRY, new BsonTypeClassMap(), null, Collection.class);
        BsonDocumentReader reader = new BsonDocumentReader(parse("{array : [[1, 2], [3, 4, 5]]}"));

        reader.readStartDocument();
        reader.readName("array");
        Collection<?> collection = (Collection<?>) codec.decode(reader, DecoderContext.builder().build());
        reader.readEndDocument();

        assertEquals(asList(asList(1, 2), asList(3, 4, 5)), new ArrayList<>(collection));
    }

    @Test
    void shouldUseProvidedTransformer() {
        @SuppressWarnings("unchecked")
        CollectionCodec codec = new CollectionCodec<>(REGISTRY, new BsonTypeClassMap(),
                from -> from.toString(), Collection.class);
        BsonDocumentReader reader = new BsonDocumentReader(parse("{array : [1, 2, 3]}"));

        reader.readStartDocument();
        reader.readName("array");
        Collection<?> collection = (Collection<?>) codec.decode(reader, DecoderContext.builder().build());
        reader.readEndDocument();

        assertEquals(asList("1", "2", "3"), new ArrayList<>(collection));
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
    @SuppressWarnings("unchecked")
    void shouldDecodeBinarySubtype3ForUuid(UuidRepresentation representation, List<?> value, String document) {
        BsonDocumentReader reader = new BsonDocumentReader(parse(document));
        CollectionCodec codec = (CollectionCodec) new CollectionCodec<>(
                fromCodecs(new UuidCodec(representation), new BinaryCodec()),
                new BsonTypeClassMap(), null, Collection.class)
                .withUuidRepresentation(representation);

        reader.readStartDocument();
        reader.readName("array");
        Collection<?> collection = (Collection<?>) codec.decode(reader, DecoderContext.builder().build());
        reader.readEndDocument();

        assertEquals(value, new ArrayList<>(collection));
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
    @SuppressWarnings("unchecked")
    void shouldDecodeBinarySubtype4ForUuid(UuidRepresentation representation, List<?> value, String document) {
        BsonDocumentReader reader = new BsonDocumentReader(parse(document));
        CollectionCodec codec = (CollectionCodec) new CollectionCodec<>(
                fromCodecs(new UuidCodec(representation), new BinaryCodec()),
                new BsonTypeClassMap(), null, Collection.class)
                .withUuidRepresentation(representation);

        reader.readStartDocument();
        reader.readName("array");
        Collection<?> collection = (Collection<?>) codec.decode(reader, DecoderContext.builder().build());
        reader.readEndDocument();

        assertEquals(value, new ArrayList<>(collection));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void shouldParameterize() throws NoSuchMethodException {
        Codec codec = fromProviders(new Jsr310CodecProvider(), REGISTRY).get(
                Collection.class,
                asList(((ParameterizedType) Container.class.getDeclaredMethod("getInstants").getGenericReturnType())
                        .getActualTypeArguments()));
        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        BsonDocumentReader reader = new BsonDocumentReader(writer.getDocument());
        List<Map<String, List<Instant>>> instants = asList(
                Collections.singletonMap("firstMap", asList(Instant.ofEpochMilli(1), Instant.ofEpochMilli(2))),
                Collections.singletonMap("secondMap", asList(Instant.ofEpochMilli(3), Instant.ofEpochMilli(4))));

        writer.writeStartDocument();
        writer.writeName("instants");
        codec.encode(writer, instants, EncoderContext.builder().build());
        writer.writeEndDocument();

        assertEquals(new BsonDocument()
                        .append("instants", new BsonArray(asList(
                                new BsonDocument("firstMap", new BsonArray(asList(new BsonDateTime(1), new BsonDateTime(2)))),
                                new BsonDocument("secondMap", new BsonArray(asList(new BsonDateTime(3), new BsonDateTime(4))))))),
                writer.getDocument());

        reader.readStartDocument();
        reader.readName("instants");
        Object decodedInstants = codec.decode(reader, DecoderContext.builder().build());

        assertEquals(instants, decodedInstants);
    }

    @SuppressWarnings("unused")
    static class Container {
        private final List<Map<String, List<Instant>>> instants = new ArrayList<>();

        List<Map<String, List<Instant>>> getInstants() {
            return instants;
        }
    }
}
