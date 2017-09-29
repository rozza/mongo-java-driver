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

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.SplittablePayload;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBinaryWriterSettings;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.BsonWriterSettings;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BsonOutput;

import java.util.Map;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * A command message that uses OP_MSG or OP_QUERY to send the command.
 */
class SplittablePayloadCommandMessage extends CommandMessage {
    private static final CodecRegistry REGISTRY = fromProviders(new BsonValueCodecProvider());
    private static final FieldNameValidator NOOP_FIELD_NAME_VALIDATOR = new NoOpFieldNameValidator();
    private static final int HEADROOM = 16 * 1024;
    private final MongoNamespace namespace;
    private final BsonDocument command;
    private final SplittablePayload payload;

    SplittablePayloadCommandMessage(final MongoNamespace namespace, final BsonDocument command, final SplittablePayload payload,
                                    final MessageSettings settings) {
        super(namespace.getFullName(), getOpCode(settings), settings);
        this.namespace = namespace;
        this.command = command;
        this.payload = payload;
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput bsonOutput, final SessionContext sessionContext) {
        if (useOpMsg()) {
            bsonOutput.writeInt32(0);  // flag bits
            bsonOutput.writeByte(0);   // payload type

            int commandStartPosition = bsonOutput.getPosition();

            BsonBinaryWriter commandWriter = new BsonBinaryWriter(new BsonWriterSettings(),
                    new BsonBinaryWriterSettings(getSettings().getMaxDocumentSize()), bsonOutput, NOOP_FIELD_NAME_VALIDATOR);

            commandWriter.writeStartDocument();
            encodeCommand(command, commandWriter);
            encodeBsonValue("$db", new BsonString(namespace.getDatabaseName()), commandWriter);
            if (sessionContext.hasSession()) {
                if (sessionContext.getClusterTime() != null) {
                    encodeBsonValue("$clusterTime", sessionContext.getClusterTime(), commandWriter);
                }
                encodeBsonValue("lsid", sessionContext.getSessionId(), commandWriter);
            }
            commandWriter.writeEndDocument();

            bsonOutput.writeByte(1);   // payload type
            int sectionPosition = bsonOutput.getPosition();
            bsonOutput.writeInt32(0); // size
            bsonOutput.writeCString(payload.getPayloadName());
            BsonBinaryWriter payloadWriter = new BsonBinaryWriter(new BsonWriterSettings(),
                    new BsonBinaryWriterSettings(getSettings().getMaxDocumentSize() + HEADROOM), bsonOutput,
                    NOOP_FIELD_NAME_VALIDATOR);
            for (int i = 0; i < payload.getPayload().size(); i++) {
                if (writeDocument(payload.getPayload().get(i), bsonOutput.getPosition(), payloadWriter, i + 1)) {
                    payload.setPosition(i + 1);
                } else {
                    break;
                }
            }
            int messageLength = bsonOutput.getPosition() - sectionPosition;
            bsonOutput.writeInt32(sectionPosition, messageLength);
            return new EncodingMetadata(null, commandStartPosition);
        } else {
            bsonOutput.writeInt32(0);
            bsonOutput.writeCString(namespace.getFullName());
            bsonOutput.writeInt32(0);
            bsonOutput.writeInt32(-1);

            int commandStartPosition = bsonOutput.getPosition();
            BsonBinaryWriter commandWriter = new BsonBinaryWriter(new BsonWriterSettings(),
                    new BsonBinaryWriterSettings(getSettings().getMaxDocumentSize() + HEADROOM),
                    bsonOutput, NOOP_FIELD_NAME_VALIDATOR);
            commandWriter.writeStartDocument();
            encodeCommand(command, commandWriter);
            commandWriter.writeStartArray(payload.getPayloadName());
            for (int i = 0; i < payload.getPayload().size(); i++) {
                if (writeDocument(payload.getPayload().get(i), commandStartPosition, commandWriter, i + 1)) {
                    payload.setPosition(i + 1);
                } else {
                    break;
                }
            }
            commandWriter.writeEndArray();
            commandWriter.writeEndDocument();
            return new EncodingMetadata(null, commandStartPosition);
        }
    }

    private void encodeCommand(final BsonDocument command, final BsonBinaryWriter commandWriter) {
        for (Map.Entry<String, BsonValue> bsonValueEntry : command.entrySet()) {
            encodeBsonValue(bsonValueEntry.getKey(), bsonValueEntry.getValue(), commandWriter);
        }
    }

    @SuppressWarnings("unchecked")
    private void encodeBsonValue(final String fieldName, final BsonValue fieldValue, final BsonBinaryWriter writer) {
        writer.writeName(fieldName);
        ((Codec<BsonValue>) REGISTRY.get(fieldValue.getClass())).encode(writer, fieldValue, EncoderContext.builder().build());
    }

    @Override
    boolean isResponseExpected() {
        return true;
    }

    private boolean writeDocument(final BsonDocument document, final int startPosition, final BsonBinaryWriter writer,
                                  final int batchItemCount) {
        int currentPosition = writer.getBsonOutput().getPosition();
        getCodec(document).encode(writer, document, EncoderContext.builder().build());
        if (exceedsLimits(writer.getBsonOutput().getPosition() - startPosition, batchItemCount)) {
            writer.getBsonOutput().truncateToPosition(currentPosition);
            return false;
        }
        return true;
    }

    private boolean exceedsLimits(final int batchLength, final int batchItemCount) {
        return (exceedsBatchLengthLimit(batchLength, batchItemCount) || exceedsBatchItemCountLimit(batchItemCount));
    }

    // make a special exception for a command with only a single item added to it.  It's allowed to exceed maximum document size so that
    // it's possible to, say, send a replacement document that is itself 16MB, which would push the size of the containing command
    // document to be greater than the maximum document size.
    private boolean exceedsBatchLengthLimit(final int batchLength, final int batchItemCount) {
        return batchLength > getSettings().getMaxDocumentSize() && batchItemCount > 1;
    }

    private boolean exceedsBatchItemCountLimit(final int batchItemCount) {
        return batchItemCount > getSettings().getMaxBatchCount();
    }
}
