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

package com.mongodb.connection;

import com.mongodb.client.model.SplittablePayload;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonValue;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.List;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

final class BsonWriterHelper {
    private static final CodecRegistry REGISTRY = fromProviders(new BsonValueCodecProvider());
    private static final EncoderContext ENCODER_CONTEXT = EncoderContext.builder().build();

    private final BsonBinaryWriter writer;
    private final int maxBatchLength;

    BsonWriterHelper(final BsonBinaryWriter writer) {
        this(writer, Integer.MAX_VALUE);
    }

    BsonWriterHelper(final BsonBinaryWriter writer, final int maxBatchLength) {
        this.writer = writer;
        this.maxBatchLength = maxBatchLength;
    }

    void writePayloadArray(final SplittablePayload payload, final int commandStartPosition) {
        writer.writeStartArray(payload.getPayloadName());
        writePayload(payload, commandStartPosition);
        writer.writeEndArray();
    }

    void writePayload(final SplittablePayload payload) {
        for (int i = 0; i < payload.getPayload().size(); i++) {
            if (writeDocument(payload.getPayload().get(i), writer.getBsonOutput().getPosition(), i + 1)) {
                payload.setPosition(i + 1);
            } else {
                break;
            }
        }
    }

    void writePayload(final SplittablePayload payload, final int startPosition) {
        for (int i = 0; i < payload.getPayload().size(); i++) {
            if (writeDocument(payload.getPayload().get(i), startPosition, i + 1)) {
                payload.setPosition(i + 1);
            } else {
                break;
            }
        }
    }

    boolean writeDocument(final BsonDocument document, final int startPosition, final int batchItemCount) {
        int currentPosition = writer.getBsonOutput().getPosition();
        getCodec(document).encode(writer, document, ENCODER_CONTEXT);
        if (exceedsLimits(writer.getBsonOutput().getPosition() - startPosition, batchItemCount)) {
            writer.getBsonOutput().truncateToPosition(currentPosition);
            return false;
        }
        return true;
    }

    void writeElements(final List<BsonElement> bsonElements) {
        for (BsonElement bsonElement : bsonElements) {
            writer.writeName(bsonElement.getName());
            getCodec(bsonElement.getValue()).encode(writer, bsonElement.getValue(), ENCODER_CONTEXT);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    Codec<BsonValue> getCodec(final BsonValue bsonValue) {
        return (Codec<BsonValue>) REGISTRY.get(bsonValue.getClass());
    }


    private boolean exceedsLimits(final int batchLength, final int batchItemCount) {
        return (exceedsBatchLengthLimit(batchLength, batchItemCount) || exceedsBatchItemCountLimit(batchItemCount));
    }

    // make a special exception for a command with only a single item added to it.  It's allowed to exceed maximum document size so that
    // it's possible to, say, send a replacement document that is itself 16MB, which would push the size of the containing command
    // document to be greater than the maximum document size.
    private boolean exceedsBatchLengthLimit(final int batchLength, final int batchItemCount) {
        return batchLength > writer.getBinaryWriterSettings().getMaxDocumentSize() && batchItemCount > 1;
    }

    private boolean exceedsBatchItemCountLimit(final int batchItemCount) {
        return batchItemCount > maxBatchLength;
    }

}
