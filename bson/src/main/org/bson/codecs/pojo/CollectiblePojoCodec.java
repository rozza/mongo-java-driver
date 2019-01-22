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

import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonElement;
import org.bson.BsonReader;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.ByteBufNIO;
import org.bson.codecs.Codec;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;

import java.nio.ByteBuffer;

import static java.util.Collections.singletonList;
import static org.bson.assertions.Assertions.isTrueArgument;
import static org.bson.assertions.Assertions.notNull;

final class CollectiblePojoCodec<T, I> extends PojoCodec<T> implements CollectibleCodec<T> {
    private final PojoCodec<T> pojoCodec;
    private final IdPropertyModelHolder<I> idPropertyModelHolder;
    private final CodecRegistry codecRegistry;

    CollectiblePojoCodec(final PojoCodec<T> pojoCodec, final IdPropertyModelHolder<I> idPropertyModelHolder,
                         final CodecRegistry codecRegistry) {
        isTrueArgument("isCollectible", notNull("idPropertyModelHolder", idPropertyModelHolder).isCollectible());
        this.pojoCodec = pojoCodec;
        this.idPropertyModelHolder = idPropertyModelHolder;
        this.codecRegistry = codecRegistry;
    }

    @Override
    public T generateIdIfAbsentFromDocument(final T pojo) {
        if (!documentHasId(pojo)) {
            I id = idPropertyModelHolder.getIdGenerator().generate();
            try {
                idPropertyModelHolder.getPropertyModel().getPropertyAccessor().set(pojo, id);
            } catch (Exception e) {
                // ignore
            }
            if (documentHasId(pojo)) {
                return pojo;
            } else {
                BasicOutputBuffer pipedBuffer = new BasicOutputBuffer();
                BsonBinaryWriter pipedWriter = new BsonBinaryWriter(pipedBuffer);
                pojoCodec.encode(pipedWriter, pojo, EncoderContext.builder().build());

                BasicOutputBuffer newBuffer = new BasicOutputBuffer();
                BsonBinaryWriter newWriter = new BsonBinaryWriter(newBuffer);

                BsonBinaryReader pipedReader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(
                        ByteBuffer.wrap(pipedBuffer.toByteArray()))));
                newWriter.pipe(pipedReader, singletonList(new BsonElement(idPropertyModelHolder.getPropertyModel().getWriteName(),
                        getBsonValueId(id))));

                BsonBinaryReader newReader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(
                        ByteBuffer.wrap(newBuffer.toByteArray()))));
                return pojoCodec.decode(newReader, DecoderContext.builder().build());
            }
        }
        return pojo;
    }

    @Override
    public boolean documentHasId(final T pojo) {
        try {
            return idPropertyModelHolder.getPropertyModel().getPropertyAccessor().get(pojo) != null;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public BsonValue getDocumentId(final T pojo) {
        return getBsonValueId(idPropertyModelHolder.getPropertyModel().getPropertyAccessor().get(pojo));
    }

    private BsonValue getBsonValueId(final I id) {
        if (id == null) {
            return null;
        } else if (id instanceof BsonValue) {
            return (BsonValue) id;
        }

        Codec<I> idCodec = codecRegistry.get(idPropertyModelHolder.getPropertyModel().getTypeData().getType());
        BsonDocument idHoldingDocument = new BsonDocument();
        BsonWriter writer = new BsonDocumentWriter(idHoldingDocument);
        writer.writeStartDocument();
        writer.writeName(idPropertyModelHolder.getPropertyModel().getWriteName());
        EncoderContext.builder().build().encodeWithChildContext(idCodec, writer, id);
        writer.writeEndDocument();
        return idHoldingDocument.get(idPropertyModelHolder.getPropertyModel().getWriteName());
    }

    @Override
    ClassModel<T> getClassModel() {
        return pojoCodec.getClassModel();
    }

    @Override
    public T decode(final BsonReader reader, final DecoderContext decoderContext) {
        return pojoCodec.decode(reader, decoderContext);
    }

    @Override
    public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
        pojoCodec.encode(writer, value, encoderContext);
    }

    @Override
    public Class<T> getEncoderClass() {
        return pojoCodec.getEncoderClass();
    }
}
