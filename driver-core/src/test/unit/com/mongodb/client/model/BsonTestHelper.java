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

package com.mongodb.client.model;

import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.ByteBufNIO;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;
import org.bson.io.OutputBuffer;

import java.nio.ByteBuffer;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;

final class BsonTestHelper {
    private static final CodecRegistry CODEC_REGISTRY = getDefaultCodecRegistry();

    @SuppressWarnings("unchecked")
    static BsonDocument toBson(final Bson bson, final boolean direct) {
        if (direct) {
            return bson.toBsonDocument(BsonDocument.class, CODEC_REGISTRY);
        } else {
            Codec<Bson> codec = (Codec<Bson>) CODEC_REGISTRY.get(bson.getClass());
            OutputBuffer buffer = new BasicOutputBuffer();
            codec.encode(new BsonBinaryWriter(buffer), bson, EncoderContext.builder().build());
            return new BsonDocumentCodec().decode(new BsonBinaryReader(new ByteBufferBsonInput(
                    new ByteBufNIO(ByteBuffer.wrap(buffer.toByteArray())))), DecoderContext.builder().build());
        }
    }

    private BsonTestHelper() {
    }

}
