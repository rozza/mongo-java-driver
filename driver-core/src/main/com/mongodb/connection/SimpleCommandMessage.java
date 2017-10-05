/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

package com.mongodb.connection;

import com.mongodb.ReadPreference;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.io.BsonOutput;

/**
 * A command message that uses OP_MSG or OP_QUERY to send the command.
 */
class SimpleCommandMessage extends CommandMessage {
    private final ReadPreference readPreference;
    private final BsonDocument command;
    private final FieldNameValidator validator;

    SimpleCommandMessage(final String collectionName, final BsonDocument command, final ReadPreference readPreference,
                         final MessageSettings settings) {
        this(collectionName, command, readPreference, new NoOpFieldNameValidator(), settings);
    }

    SimpleCommandMessage(final String collectionName, final BsonDocument command, final ReadPreference readPreference,
                         final FieldNameValidator validator, final MessageSettings settings) {
        super(collectionName, getOpCode(settings), settings);
        this.readPreference = readPreference;
        this.command = command;
        this.validator = validator;
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput bsonOutput, final SessionContext sessionContext) {
        if (useOpMsg()) {
            bsonOutput.writeInt32(0);  // flag bits
            bsonOutput.writeByte(0);   // payload type

        } else {
            bsonOutput.writeInt32(readPreference.isSlaveOk() ? 1 << 2 : 0);
            bsonOutput.writeCString(getCollectionName());
            bsonOutput.writeInt32(0);
            bsonOutput.writeInt32(-1);
        }
        int firstDocumentPosition = bsonOutput.getPosition();

        addDocument(getCommandToEncode(command), bsonOutput, validator, getExtraElements(sessionContext));
        return new EncodingMetadata(null, firstDocumentPosition);
    }

    @Override
    boolean containsPayload() {
        return false;
    }

    @Override
    boolean isResponseExpected() {
        return true;
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }
}
