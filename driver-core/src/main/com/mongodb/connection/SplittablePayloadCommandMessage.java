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
import com.mongodb.ReadPreference;
import com.mongodb.internal.validator.MappedFieldNameValidator;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonWriter;
import org.bson.FieldNameValidator;
import org.bson.codecs.EncoderContext;
import org.bson.io.BsonOutput;

import java.util.HashMap;
import java.util.Map;

import static com.mongodb.connection.BsonWriterHelper.writePayload;

/**
 * A command message that uses OP_MSG or OP_QUERY to send the command.
 */
class SplittablePayloadCommandMessage extends CommandMessage {
    private final MongoNamespace namespace;
    private final BsonDocument command;
    private final SplittablePayload payload;
    private final FieldNameValidator commandFieldNameValidator;
    private final FieldNameValidator payloadFieldNameValidator;
    private final ReadPreference readPreference;
    private final boolean responseExpected;

    SplittablePayloadCommandMessage(final MongoNamespace namespace, final BsonDocument command, final SplittablePayload payload,
                                    final ReadPreference readPreference, final FieldNameValidator commandFieldNameValidator,
                                    final FieldNameValidator payloadFieldNameValidator, final MessageSettings settings,
                                    final boolean responseExpected) {
        super(namespace.getFullName(), getOpCode(settings), settings);
        this.namespace = namespace;
        this.command = command;
        this.payload = payload;
        this.commandFieldNameValidator = commandFieldNameValidator;
        this.payloadFieldNameValidator = payloadFieldNameValidator;
        this.readPreference = readPreference;
        this.responseExpected = responseExpected;
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput bsonOutput, final SessionContext sessionContext) {
        int commandStartPosition;
        if (useOpMsg()) {
            bsonOutput.writeInt32(getFlagBits());   // flag bits
            bsonOutput.writeByte(0);          // payload type
            commandStartPosition = bsonOutput.getPosition();

            addDocument(getCommandToEncode(command), bsonOutput, commandFieldNameValidator, getExtraElements(sessionContext));

            bsonOutput.writeByte(1);          // payload type
            int payloadPosition = bsonOutput.getPosition();
            bsonOutput.writeInt32(0);         // size
            bsonOutput.writeCString(payload.getPayloadName());
            writePayload(new BsonBinaryWriter(bsonOutput, payloadFieldNameValidator), bsonOutput, getSettings(),
                    commandStartPosition, payload);

            int payloadLength = bsonOutput.getPosition() - payloadPosition;
            bsonOutput.writeInt32(payloadPosition, payloadLength);
        } else {
            bsonOutput.writeInt32(0);
            bsonOutput.writeCString(namespace.getFullName());
            bsonOutput.writeInt32(0);
            bsonOutput.writeInt32(-1);

            commandStartPosition = bsonOutput.getPosition();
            addPayload(getCommandToEncode(command), bsonOutput, getPayloadArrayFieldNameValidator(), payload);
        }
        return new EncodingMetadata(null, commandStartPosition);
    }

    @Override
    boolean containsPayload() {
        return true;
    }

    private FieldNameValidator getPayloadArrayFieldNameValidator() {
        Map<String, FieldNameValidator> rootMap = new HashMap<String, FieldNameValidator>();
        rootMap.put(payload.getPayloadName(), payloadFieldNameValidator);
        return new MappedFieldNameValidator(commandFieldNameValidator, rootMap);
    }

    private void addPayload(final BsonDocument document, final BsonOutput bsonOutput, final FieldNameValidator validator,
                            final SplittablePayload payload) {
        BsonWriter writer = new BsonBinaryWriter(bsonOutput, validator);
        if (payload != null) {
            writer =  new SplittablePayloadBsonWriter(writer, bsonOutput, getSettings(), payload);
        }
        getCodec(document).encode(writer, document, EncoderContext.builder().build());
    }

    @Override
    boolean isResponseExpected() {
        return !useOpMsg() || responseExpected;
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    private int getFlagBits() {
        if (responseExpected) {
            return 0;
        } else {
            return 1 << 1;
        }
    }
}
