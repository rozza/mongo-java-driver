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
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.ClassModelBuilder;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.codecs.pojo.PropertyModelBuilder;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@SuppressWarnings({"unchecked", "rawtypes"})
final class ChangeStreamOutputCodec<TResult> implements Codec<ChangeStreamOutput<TResult>> {

    private static final ResumeTokenCodec RESUME_TOKEN_CODEC = new ResumeTokenCodec();
    private static final MongoNamespaceCodec MONGO_NAMESPACE_CODEC = new MongoNamespaceCodec();
    private static final OperationTypeCodec OPERATION_TYPE_CODEC = new OperationTypeCodec();

    private final Codec<ChangeStreamOutput<TResult>> codec;

    ChangeStreamOutputCodec(final Class<TResult> fullDocumentClass, final CodecRegistry codecRegistry) {

        ClassModelBuilder<ChangeStreamOutput> classModelBuilder = ClassModel.builder(ChangeStreamOutput.class);
        ((PropertyModelBuilder<TResult>) classModelBuilder.getProperty("fullDocument")).codec(codecRegistry.get(fullDocumentClass));
        ((PropertyModelBuilder<ResumeToken>) classModelBuilder.getProperty("resumeToken")).codec(RESUME_TOKEN_CODEC);
        ((PropertyModelBuilder<MongoNamespace>) classModelBuilder.getProperty("namespace")).codec(MONGO_NAMESPACE_CODEC);
        ((PropertyModelBuilder<OperationType>) classModelBuilder.getProperty("operationType")).codec(OPERATION_TYPE_CODEC);
        ClassModel<ChangeStreamOutput> changeStreamOutputClassModel = classModelBuilder.build();

        PojoCodecProvider provider = PojoCodecProvider.builder()
                .register(changeStreamOutputClassModel)
                .register(OperationType.class)
                .register(UpdateDescription.class)
                .build();

        CodecRegistry registry = fromRegistries(codecRegistry, fromProviders(provider));
        this.codec = (Codec<ChangeStreamOutput<TResult>>) (Codec<? extends ChangeStreamOutput>) registry.get(ChangeStreamOutput.class);
    }

    @Override
    public ChangeStreamOutput<TResult> decode(final BsonReader reader, final DecoderContext decoderContext) {
        return codec.decode(reader, decoderContext);
    }

    @Override
    public void encode(final BsonWriter writer, final ChangeStreamOutput<TResult> value, final EncoderContext encoderContext) {
        codec.encode(writer, value, encoderContext);
    }

    @Override
    public Class<ChangeStreamOutput<TResult>> getEncoderClass() {
        return (Class<ChangeStreamOutput<TResult>>) (Class<? extends ChangeStreamOutput>) ChangeStreamOutput.class;
    }
}
