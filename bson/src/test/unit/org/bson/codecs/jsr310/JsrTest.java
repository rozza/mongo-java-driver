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

package org.bson.codecs.jsr310;

import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.BsonReader;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

@SuppressWarnings("unchecked")
abstract class JsrTest {

    abstract Codec<?> getCodec();

    @SuppressWarnings("rawtypes")
    BsonDocumentWriter encode(final Object jsrDateTime) {
        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        writer.writeStartDocument();
        writer.writeName("key");
        ((Codec) getCodec()).encode(writer, jsrDateTime, EncoderContext.builder().build());
        writer.writeEndDocument();
        return writer;
    }

    @SuppressWarnings("rawtypes")
    Object decode(final BsonDocumentWriter writer) {
        return decode(writer.getDocument());
    }

    Object decode(final BsonDocument document) {
        BsonReader bsonReader = new BsonDocumentReader(document);
        bsonReader.readStartDocument();
        bsonReader.readName();
        return getCodec().decode(bsonReader, DecoderContext.builder().build());
    }
}
