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

import com.mongodb.MongoNamespace;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

final class MongoNamespaceCodec implements Codec<MongoNamespace> {

    private static final String DATABASE_FIELD = "db";
    private static final String COLLECTION_FIELD = "coll";

    @Override
    public MongoNamespace decode(final BsonReader reader, final DecoderContext decoderContext) {
        reader.readStartDocument();
        String db = reader.readString(DATABASE_FIELD);
        String col = reader.readString(COLLECTION_FIELD);
        reader.readEndDocument();
        return new MongoNamespace(db, col);
    }

    @Override
    public void encode(final BsonWriter writer, final MongoNamespace value, final EncoderContext encoderContext) {
        writer.writeStartDocument();
        writer.writeString(DATABASE_FIELD, value.getDatabaseName());
        writer.writeString(COLLECTION_FIELD, value.getCollectionName());
        writer.writeEndDocument();
    }

    @Override
    public Class<MongoNamespace> getEncoderClass() {
        return MongoNamespace.class;
    }
}
