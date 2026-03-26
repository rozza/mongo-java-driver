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
import org.bson.BsonInvalidOperationException;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CharacterCodecTest {

    private final CharacterCodec codec = new CharacterCodec();

    @Test
    void shouldGetEncoderClass() {
        assertEquals(Character.class, codec.getEncoderClass());
    }

    @Test
    void whenEncodingACharacterShouldThrowIfItIsNull() {
        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        assertThrows(IllegalArgumentException.class, () ->
                codec.encode(writer, null, EncoderContext.builder().build()));
    }

    @Test
    void shouldEncodeACharacter() {
        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        writer.writeStartDocument();
        writer.writeName("str");
        codec.encode(writer, 'c', EncoderContext.builder().build());
        writer.writeEndDocument();

        assertEquals(new BsonDocument("str", new BsonString("c")), writer.getDocument());
    }

    @Test
    void shouldDecodeACharacter() {
        BsonDocumentReader reader = new BsonDocumentReader(new BsonDocument("str", new BsonString("c")));
        reader.readStartDocument();
        reader.readName();
        Character character = codec.decode(reader, DecoderContext.builder().build());

        assertEquals('c', character);
    }

    @Test
    void whenDecodingAStringWhoseLengthIsNot1ShouldThrowBsonInvalidOperationException() {
        BsonDocumentReader reader = new BsonDocumentReader(new BsonDocument("str", new BsonString("cc")));
        reader.readStartDocument();
        reader.readName();

        assertThrows(BsonInvalidOperationException.class, () ->
                codec.decode(reader, DecoderContext.builder().build()));
    }
}
