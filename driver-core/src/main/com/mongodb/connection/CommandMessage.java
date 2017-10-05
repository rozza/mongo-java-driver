/*
 * Copyright 2017 MongoDB, Inc.
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
 *
 */

package com.mongodb.connection;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonString;
import org.bson.io.BsonOutput;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.ReadPreference.primary;

abstract class CommandMessage extends RequestMessage {
    abstract ReadPreference getReadPreference();

    CommandMessage(final String collectionName, final OpCode opCode, final MessageSettings settings) {
        super(collectionName, opCode, settings);
    }

    abstract boolean containsPayload();

    abstract boolean isResponseExpected();

    abstract EncodingMetadata encodeMessageBodyWithMetadata(BsonOutput bsonOutput, SessionContext sessionContext);

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput bsonOutput, final int messageStartPosition,
                                                             final SessionContext sessionContext) {
        return encodeMessageBodyWithMetadata(bsonOutput, sessionContext);
    }

    protected static OpCode getOpCode(final MessageSettings settings) {
        return useOpMsg(settings) ? OpCode.OP_MSG : OpCode.OP_QUERY;
    }

    protected static boolean useOpMsg(final MessageSettings settings) {
        return isServerVersionAtLeastThreeDotSix(settings);
    }

    private static boolean isServerVersionAtLeastThreeDotSix(final MessageSettings settings) {
        return settings.getServerVersion().compareTo(new ServerVersion(3, 5)) >= 0;
    }

    protected boolean useOpMsg() {
        return useOpMsg(getSettings());
    }

    protected BsonDocument getCommandToEncode(final BsonDocument command) {
        BsonDocument commandToEncode = command;
        if (!useOpMsg() && !isDefaultReadPreference(getReadPreference())) {
            commandToEncode = new BsonDocument("$query", command).append("$readPreference", getReadPreference().toDocument());
        }
        return commandToEncode;
    }

    protected List<BsonElement> getExtraElements(final SessionContext sessionContext) {
        if (!useOpMsg()) {
            return null;
        }
        List<BsonElement> extraElements = new ArrayList<BsonElement>();
        extraElements.add(new BsonElement("$db", new BsonString(new MongoNamespace(getCollectionName()).getDatabaseName())));
        if (sessionContext.hasSession()) {
            if (sessionContext.getClusterTime() != null) {
                extraElements.add(new BsonElement("$clusterTime", sessionContext.getClusterTime()));
            }
            extraElements.add(new BsonElement("lsid", sessionContext.getSessionId()));
        }
        if (!isDefaultReadPreference(getReadPreference())) {
            extraElements.add(new BsonElement("$readPreference", getReadPreference().toDocument()));
        }
        return extraElements;
    }

    protected boolean isDefaultReadPreference(final ReadPreference readPreference) {
        return readPreference.equals(primary());
    }
}
