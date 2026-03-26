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

package com.mongodb.internal.operation;

import com.mongodb.ServerAddress;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.session.SessionContext;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.WriteConcern.ACKNOWLEDGED;
import static com.mongodb.WriteConcern.UNACKNOWLEDGED;
import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY;
import static com.mongodb.connection.ServerType.STANDALONE;
import static com.mongodb.internal.operation.OperationHelper.canRetryRead;
import static com.mongodb.internal.operation.OperationHelper.isRetryableWrite;
import static com.mongodb.internal.operation.OperationHelper.validateWriteRequests;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OperationHelperTest {

    private static final ConnectionId CONNECTION_ID = new ConnectionId(new ServerId(new ClusterId(), new ServerAddress()));
    private static final ConnectionDescription THREE_SIX_CONNECTION_DESCRIPTION = new ConnectionDescription(CONNECTION_ID, 6,
            STANDALONE, 1000, 100000, 100000, Collections.emptyList(), new BsonArray(), 30);
    private static final ConnectionDescription THREE_SIX_PRIMARY_CONNECTION_DESCRIPTION = new ConnectionDescription(CONNECTION_ID, 6,
            REPLICA_SET_PRIMARY, 1000, 100000, 100000, Collections.emptyList(), new BsonArray(), 30);
    private static final ConnectionDescription THREE_FOUR_CONNECTION_DESCRIPTION = new ConnectionDescription(CONNECTION_ID, 5,
            STANDALONE, 1000, 100000, 100000, Collections.emptyList(), new BsonArray(), null);

    private static final ServerDescription RETRYABLE_SERVER_DESCRIPTION = ServerDescription.builder()
            .address(new ServerAddress()).state(CONNECTED).logicalSessionTimeoutMinutes(1).build();

    private static final Collation EN_COLLATION = Collation.builder().locale("en").build();

    @Test
    void shouldAcceptValidWriteRequests() {
        assertDoesNotThrow(() -> validateWriteRequests(THREE_SIX_CONNECTION_DESCRIPTION, null,
                Collections.singletonList(new DeleteRequest(BsonDocument.parse("{a: \"a\"}"))),
                ACKNOWLEDGED));

        assertDoesNotThrow(() -> validateWriteRequests(THREE_SIX_CONNECTION_DESCRIPTION, null,
                Collections.singletonList(new DeleteRequest(BsonDocument.parse("{a: \"a\"}"))),
                UNACKNOWLEDGED));

        assertDoesNotThrow(() -> validateWriteRequests(THREE_SIX_CONNECTION_DESCRIPTION, null,
                Collections.singletonList(new DeleteRequest(BsonDocument.parse("{a: \"a\"}")).collation(EN_COLLATION)),
                ACKNOWLEDGED));

        assertDoesNotThrow(() -> validateWriteRequests(THREE_SIX_CONNECTION_DESCRIPTION, true,
                Collections.singletonList(new UpdateRequest(BsonDocument.parse("{a: \"a\"}"),
                        BsonDocument.parse("{$set: {a: \"A\"}}"),
                        WriteRequest.Type.REPLACE).collation(EN_COLLATION)),
                ACKNOWLEDGED));
    }

    @Test
    void shouldCheckIfValidRetryableWrite() {
        SessionContext noTransactionSessionContext = mock(SessionContext.class);
        when(noTransactionSessionContext.hasSession()).thenReturn(true);
        when(noTransactionSessionContext.hasActiveTransaction()).thenReturn(false);

        SessionContext activeTransactionSessionContext = mock(SessionContext.class);
        when(activeTransactionSessionContext.hasSession()).thenReturn(true);
        when(activeTransactionSessionContext.hasActiveTransaction()).thenReturn(true);

        // false, ACKNOWLEDGED, threeSixConnectionDescription -> false
        assertFalse(isRetryableWrite(false, ACKNOWLEDGED, THREE_SIX_CONNECTION_DESCRIPTION, noTransactionSessionContext));
        assertFalse(isRetryableWrite(false, ACKNOWLEDGED, THREE_SIX_CONNECTION_DESCRIPTION, activeTransactionSessionContext));

        // true, UNACKNOWLEDGED, threeSixConnectionDescription -> false
        assertFalse(isRetryableWrite(true, UNACKNOWLEDGED, THREE_SIX_CONNECTION_DESCRIPTION, noTransactionSessionContext));
        assertFalse(isRetryableWrite(true, UNACKNOWLEDGED, THREE_SIX_CONNECTION_DESCRIPTION, activeTransactionSessionContext));

        // true, ACKNOWLEDGED, threeSixConnectionDescription -> false (STANDALONE)
        assertFalse(isRetryableWrite(true, ACKNOWLEDGED, THREE_SIX_CONNECTION_DESCRIPTION, noTransactionSessionContext));
        assertFalse(isRetryableWrite(true, ACKNOWLEDGED, THREE_SIX_CONNECTION_DESCRIPTION, activeTransactionSessionContext));

        // true, ACKNOWLEDGED, threeFourConnectionDescription -> false
        assertFalse(isRetryableWrite(true, ACKNOWLEDGED, THREE_FOUR_CONNECTION_DESCRIPTION, noTransactionSessionContext));
        assertFalse(isRetryableWrite(true, ACKNOWLEDGED, THREE_FOUR_CONNECTION_DESCRIPTION, activeTransactionSessionContext));

        // true, ACKNOWLEDGED, threeSixPrimaryConnectionDescription -> true (only without active transaction)
        assertTrue(isRetryableWrite(true, ACKNOWLEDGED, THREE_SIX_PRIMARY_CONNECTION_DESCRIPTION, noTransactionSessionContext));
        assertFalse(isRetryableWrite(true, ACKNOWLEDGED, THREE_SIX_PRIMARY_CONNECTION_DESCRIPTION, activeTransactionSessionContext));
    }

    @Test
    void shouldCheckIfValidRetryableRead() {
        SessionContext noTransactionSessionContext = mock(SessionContext.class);
        when(noTransactionSessionContext.hasSession()).thenReturn(true);
        when(noTransactionSessionContext.hasActiveTransaction()).thenReturn(false);

        SessionContext activeTransactionSessionContext = mock(SessionContext.class);
        when(activeTransactionSessionContext.hasSession()).thenReturn(true);
        when(activeTransactionSessionContext.hasActiveTransaction()).thenReturn(true);

        assertTrue(canRetryRead(RETRYABLE_SERVER_DESCRIPTION,
                OPERATION_CONTEXT.withSessionContext(noTransactionSessionContext)));
        assertFalse(canRetryRead(RETRYABLE_SERVER_DESCRIPTION,
                OPERATION_CONTEXT.withSessionContext(activeTransactionSessionContext)));
    }
}
