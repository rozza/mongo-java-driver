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
import org.bson.BsonUndefined;
import org.junit.jupiter.api.Test;

import static org.bson.codecs.CodecTestUtil.prepareReaderWithObjectToBeDecoded;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class UndefinedCodecTest {

    private final BsonUndefinedCodec codec = new BsonUndefinedCodec();

    @Test
    void shouldReturnUndefinedClass() {
        assertEquals(BsonUndefined.class, codec.getEncoderClass());
    }

    @Test
    void shouldDecodeUndefinedTypeFromBsonReader() {
        // Use a round-trip approach: encode an undefined value, then decode it
        BsonDocument document = new BsonDocument();
        BsonDocumentWriter writer = new BsonDocumentWriter(document);
        writer.writeStartDocument();
        writer.writeName("val");
        codec.encode(writer, new BsonUndefined(), EncoderContext.builder().build());
        writer.writeEndDocument();

        BsonDocumentReader reader = new BsonDocumentReader(document);
        reader.readStartDocument();
        reader.readName("val");
        BsonUndefined result = codec.decode(reader, DecoderContext.builder().build());

        assertNotNull(result);
        assertEquals(BsonUndefined.class, result.getClass());
    }

    @Test
    void shouldEncodeUndefinedTypeToBsonWriter() {
        BsonDocument document = new BsonDocument();
        BsonDocumentWriter writer = new BsonDocumentWriter(document);
        writer.writeStartDocument();
        writer.writeName("val");
        codec.encode(writer, new BsonUndefined(), EncoderContext.builder().build());
        writer.writeEndDocument();

        // Verify by decoding back
        BsonDocumentReader reader = new BsonDocumentReader(document);
        reader.readStartDocument();
        reader.readName("val");
        BsonUndefined result = codec.decode(reader, DecoderContext.builder().build());
        assertNotNull(result);
    }
}
