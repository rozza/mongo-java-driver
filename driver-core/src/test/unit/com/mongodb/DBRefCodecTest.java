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

import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class DBRefCodecTest {

    @Test
    @DisplayName("provider should return codec for DBRef class")
    void providerShouldReturnCodecForDBRef() {
        assertInstanceOf(DBRefCodec.class, new DBRefCodecProvider().get(DBRef.class, mock(CodecRegistry.class)));
    }

    @Test
    @DisplayName("provider should return null for non-DBRef class")
    void providerShouldReturnNullForNonDBRef() {
        assertNull(new DBRefCodecProvider().get(Integer.class, mock(CodecRegistry.class)));
    }

    @Test
    @DisplayName("provider should be equal to another of the same class")
    void providerShouldBeEqual() {
        assertEquals(new DBRefCodecProvider(), new DBRefCodecProvider());
    }

    @Test
    @DisplayName("provider should be not equal to any thing else")
    void providerShouldNotBeEqualToOther() {
        assertNotEquals(new DBRefCodecProvider(), new ValueCodecProvider());
    }

    @Test
    @DisplayName("codec should encode DBRef")
    void codecShouldEncodeDBRef() {
        DBRef ref = new DBRef("foo", 1);
        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

        writer.writeStartDocument();
        writer.writeName("ref");
        new DBRefCodec(fromProviders(Collections.singletonList(new ValueCodecProvider())))
                .encode(writer, ref, EncoderContext.builder().build());
        writer.writeEndDocument();

        assertEquals(
                new BsonDocument("ref", new BsonDocument("$ref", new BsonString("foo")).append("$id", new BsonInt32(1))),
                writer.getDocument());
    }

    @Test
    @DisplayName("codec should encode DBRef with database name")
    void codecShouldEncodeDBRefWithDatabaseName() {
        DBRef ref = new DBRef("mydb", "foo", 1);
        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

        writer.writeStartDocument();
        writer.writeName("ref");
        new DBRefCodec(fromProviders(Collections.singletonList(new ValueCodecProvider())))
                .encode(writer, ref, EncoderContext.builder().build());
        writer.writeEndDocument();

        assertEquals(
                new BsonDocument("ref",
                        new BsonDocument("$ref", new BsonString("foo"))
                                .append("$id", new BsonInt32(1))
                                .append("$db", new BsonString("mydb"))),
                writer.getDocument());
    }

    @Test
    @DisplayName("codec should throw UnsupportedOperationException on decode")
    void codecShouldThrowOnDecode() {
        assertThrows(UnsupportedOperationException.class, () ->
                new DBRefCodec(fromProviders(Collections.singletonList(new ValueCodecProvider())))
                        .decode(new BsonDocumentReader(new BsonDocument()), DecoderContext.builder().build()));
    }
}
