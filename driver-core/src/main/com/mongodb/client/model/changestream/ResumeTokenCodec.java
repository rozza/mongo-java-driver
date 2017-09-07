/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model.changestream;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.RawBsonDocument;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.RawBsonDocumentCodec;

final class ResumeTokenCodec implements Codec<ResumeToken> {

    private static final RawBsonDocumentCodec RAW_BSON_DOCUMENT_CODEC = new RawBsonDocumentCodec();

    @Override
    public ResumeToken decode(final BsonReader reader, final DecoderContext decoderContext) {
        return new ResumeToken(RAW_BSON_DOCUMENT_CODEC.decode(reader, decoderContext));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void encode(final BsonWriter writer, final ResumeToken value, final EncoderContext encoderContext) {
        RAW_BSON_DOCUMENT_CODEC.encode(writer, (RawBsonDocument) value.getResumeToken(), encoderContext);
    }

    @Override
    public Class<ResumeToken> getEncoderClass() {
        return ResumeToken.class;
    }
}
