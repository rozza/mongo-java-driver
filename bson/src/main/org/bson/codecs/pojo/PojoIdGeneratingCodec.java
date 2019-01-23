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

package org.bson.codecs.pojo;

import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonObjectId;
import org.bson.BsonReader;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.ObjectId;

import static org.bson.assertions.Assertions.isTrueArgument;
import static org.bson.assertions.Assertions.notNull;


class PojoIdGeneratingCodec<T, I> extends PojoCodec<T> {
    private final PojoCodec<T> pojoCodec;
    private final PropertyModel<I> idPropertyModel;
    private final IdGenerator<I> idGenerator;
    private final CodecRegistry codecRegistry;

    PojoIdGeneratingCodec(final PojoCodec<T> pojoCodec, final IdPropertyModelHolder<I> idPropertyModelHolder,
                          final CodecRegistry codecRegistry) {
        isTrueArgument("isCollectible", notNull("idPropertyModelHolder", idPropertyModelHolder).isCollectible());
        this.pojoCodec = pojoCodec;
        this.idPropertyModel = idPropertyModelHolder.getPropertyModel();
        this.idGenerator = idPropertyModelHolder.getIdGenerator();
        this.codecRegistry = codecRegistry;
    }

    @Override
    public T decode(final BsonReader reader, final DecoderContext decoderContext) {
        return pojoCodec.decode(reader, decoderContext);
    }

    @Override
    public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
        pojoCodec.encode(generateIdAndMutatePojoOrWrapBsonWriter(writer, value), value, encoderContext);
    }

    @Override
    public Class<T> getEncoderClass() {
        return pojoCodec.getEncoderClass();
    }

    @Override
    ClassModel<T> getClassModel() {
        return pojoCodec.getClassModel();
    }

    private boolean isPojoMissingId(final T pojo) {
        try {
            return idPropertyModel.getPropertyAccessor().get(pojo) == null;
        } catch (Exception e) {
            return true;
        }
    }

    private I generateAndTryToAddId(final T pojo) {
        I id = idGenerator.generate();
        try {
            idPropertyModel.getPropertyAccessor().set(pojo, id);
        } catch (Exception e) {
            // ignore
        }
        return id;
    }

    private BsonWriter generateIdAndMutatePojoOrWrapBsonWriter(final BsonWriter writer, final T pojo) {
        if (isPojoMissingId(pojo)) {
            I id = generateAndTryToAddId(pojo);
            if (isPojoMissingId(pojo)) {
                return new PojoIdExtendingBsonWriter(getBsonValueId(id), writer);
            }
        }
        return writer;
    }

    private BsonValue getBsonValueId(final I id) {
        if (id == null) {
            return null;
        } if (id instanceof ObjectId) {
            return new BsonObjectId((ObjectId) id);
        } else if (id instanceof BsonValue) {
            return (BsonValue) id;
        }
        Codec<I> idCodec = codecRegistry.get(idPropertyModel.getTypeData().getType());
        BsonDocument idHoldingDocument = new BsonDocument();
        BsonWriter writer = new BsonDocumentWriter(idHoldingDocument);
        writer.writeStartDocument();
        writer.writeName(idPropertyModel.getWriteName());
        EncoderContext.builder().build().encodeWithChildContext(idCodec, writer, id);
        writer.writeEndDocument();
        return idHoldingDocument.get(idPropertyModel.getWriteName());
    }
}
