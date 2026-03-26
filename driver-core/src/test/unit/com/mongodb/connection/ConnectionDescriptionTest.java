/*
 * Copyright 2008-present MongoDB, Inc.
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

import com.mongodb.ServerAddress;
import org.bson.BsonArray;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class ConnectionDescriptionTest {

    private final ObjectId serverId = new ObjectId();
    private final ConnectionId id = new ConnectionId(new ServerId(new ClusterId(), new ServerAddress()));
    private final BsonArray saslSupportedMechanisms = new BsonArray(Collections.singletonList(new BsonString("SCRAM-SHA-256")));
    private final ConnectionDescription description = new ConnectionDescription(serverId, id, 5, ServerType.STANDALONE, 1, 2, 3,
            Collections.singletonList("zlib"), saslSupportedMechanisms);

    @Test
    void shouldInitializeAllValues() {
        assertEquals(serverId, description.getServiceId());
        assertEquals(id, description.getConnectionId());
        assertEquals(5, description.getMaxWireVersion());
        assertEquals(ServerType.STANDALONE, description.getServerType());
        assertEquals(1, description.getMaxBatchCount());
        assertEquals(2, description.getMaxDocumentSize());
        assertEquals(3, description.getMaxMessageSize());
        assertEquals(Collections.singletonList("zlib"), description.getCompressors());
        assertEquals(saslSupportedMechanisms, description.getSaslSupportedMechanisms());
    }

    @Test
    void withConnectionIdShouldReturnNewInstanceWithGivenConnectionIdAndPreserveTheRest() {
        ConnectionId newId = id.withServerValue(123);
        ConnectionDescription newDescription = description.withConnectionId(newId);

        assertNotSame(newDescription, description);
        assertEquals(serverId, newDescription.getServiceId());
        assertEquals(newId, newDescription.getConnectionId());
        assertEquals(5, newDescription.getMaxWireVersion());
        assertEquals(ServerType.STANDALONE, newDescription.getServerType());
        assertEquals(1, newDescription.getMaxBatchCount());
        assertEquals(2, newDescription.getMaxDocumentSize());
        assertEquals(3, newDescription.getMaxMessageSize());
        assertEquals(Collections.singletonList("zlib"), newDescription.getCompressors());
        assertEquals(saslSupportedMechanisms, description.getSaslSupportedMechanisms());
    }

    @Test
    void withServiceIdShouldReturnNewInstanceWithGivenServiceIdAndPreserveTheRest() {
        ObjectId newServerId = new ObjectId();
        ConnectionDescription newDescription = description.withServiceId(newServerId);

        assertNotSame(newDescription, description);
        assertEquals(newServerId, newDescription.getServiceId());
        assertEquals(id, newDescription.getConnectionId());
        assertEquals(5, newDescription.getMaxWireVersion());
        assertEquals(ServerType.STANDALONE, newDescription.getServerType());
        assertEquals(1, newDescription.getMaxBatchCount());
        assertEquals(2, newDescription.getMaxDocumentSize());
        assertEquals(3, newDescription.getMaxMessageSize());
        assertEquals(Collections.singletonList("zlib"), newDescription.getCompressors());
        assertEquals(saslSupportedMechanisms, description.getSaslSupportedMechanisms());
    }
}
