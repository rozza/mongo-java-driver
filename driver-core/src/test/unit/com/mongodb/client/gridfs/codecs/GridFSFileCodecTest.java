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

package com.mongodb.client.gridfs.codecs;

import com.mongodb.client.gridfs.model.GridFSFile;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.ByteBufNIO;
import org.bson.Document;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;
import org.bson.io.OutputBuffer;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.junit.jupiter.api.Assertions.assertEquals;

class GridFSFileCodecTest {

    private static final CodecRegistry REGISTRY = fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider(),
            new DocumentCodecProvider());
    private static final CodecRegistry BSON_TYPES_REGISTRY;
    static {
        Map<BsonType, Class<?>> replacements = new HashMap<>();
        replacements.put(BsonType.STRING, BsonString.class);
        BSON_TYPES_REGISTRY = fromRegistries(
                fromProviders(new DocumentCodecProvider(new BsonTypeClassMap(replacements))), REGISTRY);
    }
    private static final GridFSFileCodec CODEC = new GridFSFileCodec(REGISTRY);
    private static final BsonObjectId ID = new BsonObjectId(new ObjectId());
    private static final String FILENAME = "filename";
    private static final long LENGTH = 100L;
    private static final int CHUNKSIZE = 255;
    private static final Date UPLOADDATE = new Date();
    private static final Document METADATA = new Document("field", "value");

    @Test
    void shouldEncodeAndDecodeWithoutMetadata() {
        GridFSFile original = new GridFSFile(ID, FILENAME, LENGTH, CHUNKSIZE, UPLOADDATE, null);
        assertEquals(original, roundTripped(original));
    }

    @Test
    void shouldEncodeAndDecodeWithMetadata() {
        GridFSFile original = new GridFSFile(ID, FILENAME, LENGTH, CHUNKSIZE, UPLOADDATE, METADATA);
        assertEquals(original, roundTripped(original));
    }

    @Test
    void shouldUseUsersCodecForMetadata() {
        Document doc = new Document("_id", ID)
                .append("filename", FILENAME)
                .append("length", LENGTH)
                .append("chunkSize", CHUNKSIZE)
                .append("uploadDate", UPLOADDATE)
                .append("metadata", METADATA);

        GridFSFile gridFSFile = toGridFSFile(doc, BSON_TYPES_REGISTRY);
        assertEquals(new BsonString("value"), gridFSFile.getMetadata().get("field"));
    }

    private GridFSFile roundTripped(final GridFSFile gridFSFile) {
        BsonBinaryWriter writer = new BsonBinaryWriter(new BasicOutputBuffer());
        CODEC.encode(writer, gridFSFile, EncoderContext.builder().build());
        return decode(writer, CODEC);
    }

    private GridFSFile toGridFSFile(final Document document, final CodecRegistry registry) {
        BsonBinaryWriter writer = new BsonBinaryWriter(new BasicOutputBuffer());
        registry.get(Document.class).encode(writer, document, EncoderContext.builder().build());
        return decode(writer, new GridFSFileCodec(registry));
    }

    private <T> T decode(final BsonBinaryWriter writer, final Codec<T> codec) {
        BsonBinaryReader reader = new BsonBinaryReader(
                new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(((OutputBuffer) writer.getBsonOutput()).toByteArray()))));
        return codec.decode(reader, DecoderContext.builder().build());
    }
}
