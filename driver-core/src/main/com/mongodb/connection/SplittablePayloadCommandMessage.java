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
import com.mongodb.internal.validator.MappedFieldNameValidator;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonString;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BsonOutput;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    private final FieldNameValidator fieldNameValidator;

    SplittablePayloadCommandMessage(final MongoNamespace namespace, final BsonDocument command, final SplittablePayload payload,
                                    final FieldNameValidator fieldNameValidator, final MessageSettings settings) {
        super(namespace.getFullName(), getOpCode(settings), settings);
        this.namespace = namespace;
        this.command = command;
        this.payload = payload;
        this.fieldNameValidator = fieldNameValidator;
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput bsonOutput, final SessionContext sessionContext) {
        int commandStartPosition;
        if (useOpMsg()) {
            bsonOutput.writeInt32(0);  // flag bits
            bsonOutput.writeByte(0);   // payload type
            commandStartPosition = bsonOutput.getPosition();

            List<BsonElement> extraElements = new ArrayList<BsonElement>();
            extraElements.add(new BsonElement("$db", new BsonString(new MongoNamespace(getCollectionName()).getDatabaseName())));
            if (sessionContext.hasSession()) {
                if (sessionContext.getClusterTime() != null) {
                    extraElements.add(new BsonElement("$clusterTime", sessionContext.getClusterTime()));
                }
                extraElements.add(new BsonElement("lsid", sessionContext.getSessionId()));
            }
            addDocument(command, bsonOutput, fieldNameValidator, extraElements);

            bsonOutput.writeByte(1);   // payload type
            int sectionPosition = bsonOutput.getPosition();
            bsonOutput.writeInt32(0); // size
            bsonOutput.writeCString(payload.getPayloadName());
            new BsonWriterHelper(new BsonBinaryWriter(bsonOutput, fieldNameValidator)).writePayload(payload);

            int messageLength = bsonOutput.getPosition() - sectionPosition;
            bsonOutput.writeInt32(sectionPosition, messageLength);
        } else {
            bsonOutput.writeInt32(0);
            bsonOutput.writeCString(namespace.getFullName());
            bsonOutput.writeInt32(0);
            bsonOutput.writeInt32(-1);

            commandStartPosition = bsonOutput.getPosition();
            addPayload(command, bsonOutput, getPayloadArrayFieldNameValidator(), payload);
        }
        return new EncodingMetadata(null, commandStartPosition);
    }

    private FieldNameValidator getPayloadArrayFieldNameValidator() {
        Map<String, FieldNameValidator> rootMap = new HashMap<String, FieldNameValidator>();
        rootMap.put(payload.getPayloadName(), fieldNameValidator);
        return new MappedFieldNameValidator(new NoOpFieldNameValidator(), rootMap);
    }

    @Override
    boolean isResponseExpected() {
        return true;
    }

}
