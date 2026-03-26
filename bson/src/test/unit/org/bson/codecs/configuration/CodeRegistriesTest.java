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

package org.bson.codecs.configuration;

import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.BsonInt32;
import org.bson.codecs.BsonInt32Codec;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.CollectionCodecProvider;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.IntegerCodec;
import org.bson.codecs.LongCodec;
import org.bson.codecs.MapCodecProvider;
import org.bson.codecs.UuidCodec;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.jsr310.Jsr310CodecProvider;
import org.bson.internal.ProvidersCodecRegistry;
import org.junit.jupiter.api.Test;

import java.lang.reflect.ParameterizedType;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.bson.UuidRepresentation.STANDARD;
import static org.bson.UuidRepresentation.UNSPECIFIED;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.bson.codecs.configuration.CodecRegistries.withUuidRepresentation;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

class CodeRegistriesTest {

    @Test
    void fromCodecShouldReturnProvidersCodecRegistry() {
        CodecRegistry registry = fromCodecs(new UuidCodec(), new LongCodec());

        assertInstanceOf(ProvidersCodecRegistry.class, registry);
        assertInstanceOf(UuidCodec.class, registry.get(UUID.class));
        assertInstanceOf(LongCodec.class, registry.get(Long.class));
    }

    @Test
    void fromProviderShouldReturnProvidersCodecRegistry() {
        CodecRegistry registry = fromProviders(new BsonValueCodecProvider());

        assertInstanceOf(ProvidersCodecRegistry.class, registry);
        assertInstanceOf(BsonInt32Codec.class, registry.get(BsonInt32.class));
    }

    @Test
    void fromProvidersShouldReturnProvidersCodecRegistry() {
        CodecRegistry providers = fromProviders(asList(new BsonValueCodecProvider(), new ValueCodecProvider()));

        assertInstanceOf(ProvidersCodecRegistry.class, providers);
        assertInstanceOf(BsonInt32Codec.class, providers.get(BsonInt32.class));
        assertInstanceOf(IntegerCodec.class, providers.get(Integer.class));
    }

    @Test
    void fromRegistriesShouldReturnProvidersCodecRegistry() {
        UuidCodec uuidCodec = new UuidCodec();
        CodecRegistry registry = fromRegistries(fromCodecs(uuidCodec), fromProviders(new ValueCodecProvider()));

        assertInstanceOf(ProvidersCodecRegistry.class, registry);
        assertSame(uuidCodec, registry.get(UUID.class));
        assertInstanceOf(IntegerCodec.class, registry.get(Integer.class));
    }

    @Test
    void withUuidRepresentationShouldApplyUuidRepresentation() {
        CodecRegistry registry = fromProviders(new ValueCodecProvider());
        CodecRegistry registryWithStandard = withUuidRepresentation(registry, STANDARD);

        UuidCodec uuidCodec = (UuidCodec) registry.get(UUID.class);
        assertEquals(UNSPECIFIED, uuidCodec.getUuidRepresentation());

        uuidCodec = (UuidCodec) registryWithStandard.get(UUID.class);
        assertEquals(STANDARD, uuidCodec.getUuidRepresentation());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void withUuidRepresentationShouldNotBreakParameterization() throws NoSuchMethodException {
        CodecRegistry registry = fromProviders(
                new Jsr310CodecProvider(),
                new ValueCodecProvider(),
                withUuidRepresentation(fromProviders(new CollectionCodecProvider()), STANDARD),
                withUuidRepresentation(fromProviders(new MapCodecProvider()), STANDARD)
        );
        Codec codec = registry.get(Collection.class, asList(
                ((ParameterizedType) CodeRegistriesTest.class.getMethod("parameterizedTypeProvider")
                        .getGenericReturnType()).getActualTypeArguments()));
        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        BsonDocumentReader reader = new BsonDocumentReader(writer.getDocument());
        List<Map<String, List<Instant>>> value = asList(
                Collections.singletonMap("firstMap", asList(Instant.ofEpochMilli(1), Instant.ofEpochMilli(2))),
                Collections.singletonMap("secondMap", asList(Instant.ofEpochMilli(3), Instant.ofEpochMilli(4))));

        writer.writeStartDocument();
        writer.writeName("value");
        codec.encode(writer, value, EncoderContext.builder().build());
        writer.writeEndDocument();

        assertEquals(new BsonDocument()
                        .append("value", new BsonArray(asList(
                                new BsonDocument("firstMap", new BsonArray(asList(new BsonDateTime(1), new BsonDateTime(2)))),
                                new BsonDocument("secondMap", new BsonArray(asList(new BsonDateTime(3), new BsonDateTime(4))))))),
                writer.getDocument());

        reader.readStartDocument();
        reader.readName("value");
        Object decodedValue = codec.decode(reader, DecoderContext.builder().build());

        assertEquals(value, decodedValue);
    }

    @SuppressWarnings("unused")
    public List<Map<String, List<Instant>>> parameterizedTypeProvider() {
        return new ArrayList<>();
    }
}
