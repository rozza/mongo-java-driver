/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.bson.codecs;

import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.BsonReader;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BsonCodecTest {

    private static final CodecRegistry REGISTRY = CodecRegistries.fromCodecs(new BsonDocumentCodec());

    @Test
    void shouldEncodeBson() {
        BsonCodec codec = new BsonCodec(REGISTRY);
        Bson customBson = new Bson() {
            @Override
            public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> clazz, final CodecRegistry codecRegistry) {
                return BsonDocument.parse("{a: 1, b: 2}");
            }
        };

        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        writer.writeStartDocument();
        writer.writeName("customBson");
        codec.encode(writer, customBson, EncoderContext.builder().build());
        writer.writeEndDocument();

        assertEquals(BsonDocument.parse("{a: 1, b:2}"), writer.getDocument().get("customBson"));
    }

    @Test
    void shouldThrowCodecConfigurationExceptionIfCannotEncodeBson() {
        BsonCodec codec = new BsonCodec(REGISTRY);
        Bson exceptionBson = new Bson() {
            @Override
            public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> clazz, final CodecRegistry codecRegistry) {
                throw new RuntimeException("Cannot encode");
            }
        };

        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        writer.writeStartDocument();
        writer.writeName("customBson");

        assertThrows(CodecConfigurationException.class, () ->
                codec.encode(writer, exceptionBson, EncoderContext.builder().build()));
    }

    @Test
    void shouldThrowUnsupportedOperationExceptionIfDecodeIsCalled() {
        BsonReader reader = new BsonDocumentReader(new BsonDocument());
        assertThrows(UnsupportedOperationException.class, () ->
                new BsonCodec(REGISTRY).decode(reader, DecoderContext.builder().build()));
    }
}
