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

package com.mongodb.internal.session;

import com.mongodb.ReadConcern;
import com.mongodb.session.ClientSession;
import com.mongodb.session.ServerSession;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ClientSessionContextTest {

    static class TestClientSessionContext extends ClientSessionContext {
        TestClientSessionContext(final ClientSession clientSession) {
            super(clientSession);
        }

        @Override
        public boolean hasActiveTransaction() {
            return false;
        }

        @Override
        public boolean isImplicitSession() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean notifyMessageSent() {
            return false;
        }

        @Override
        public ReadConcern getReadConcern() {
            return ReadConcern.DEFAULT;
        }
    }

    @Test
    void shouldHaveSession() {
        ClientSession clientSession = mock(ClientSession.class);
        TestClientSessionContext context = new TestClientSessionContext(clientSession);
        assertTrue(context.hasSession());
    }

    @Test
    void shouldForwardAllMethodsToWrappedSession() {
        BsonDocument expectedSessionId = new BsonDocument("id", new BsonInt32(1));
        BsonDocument expectedClusterTime = new BsonDocument("x", BsonBoolean.TRUE);
        BsonTimestamp expectedOperationTime = new BsonTimestamp(42, 1);
        long expectedTransactionNumber = 2;

        ServerSession serverSession = mock(ServerSession.class);
        ClientSession clientSession = mock(ClientSession.class);
        when(clientSession.getServerSession()).thenReturn(serverSession);

        TestClientSessionContext context = new TestClientSessionContext(clientSession);

        when(serverSession.getIdentifier()).thenReturn(expectedSessionId);
        assertEquals(expectedSessionId, context.getSessionId());
        verify(serverSession).getIdentifier();

        context.isCausallyConsistent();
        verify(clientSession).isCausallyConsistent();

        context.advanceClusterTime(expectedClusterTime);
        verify(clientSession).advanceClusterTime(expectedClusterTime);

        context.getOperationTime();
        verify(clientSession).getOperationTime();

        context.advanceOperationTime(expectedOperationTime);
        verify(clientSession).advanceOperationTime(expectedOperationTime);

        context.advanceTransactionNumber();
        verify(serverSession).advanceTransactionNumber();

        when(serverSession.getTransactionNumber()).thenReturn(expectedTransactionNumber);
        assertEquals(expectedTransactionNumber, context.getTransactionNumber());
        verify(serverSession).getTransactionNumber();

        when(clientSession.getClusterTime()).thenReturn(expectedClusterTime);
        assertEquals(expectedClusterTime, context.getClusterTime());
        verify(clientSession).getClusterTime();
    }
}
