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

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerSettings;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.Server;
import com.mongodb.internal.connection.ServerTuple;
import com.mongodb.session.ServerSession;
import org.bson.BsonBinarySubType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.ClusterFixture.getServerApi;
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY;
import static com.mongodb.connection.ServerType.UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ServerSessionPoolTest {

    private final ClusterDescription connectedDescription = new ClusterDescription(MULTIPLE, REPLICA_SET,
            Collections.singletonList(
                    ServerDescription.builder().ok(true)
                            .state(CONNECTED)
                            .address(new ServerAddress())
                            .type(REPLICA_SET_PRIMARY)
                            .logicalSessionTimeoutMinutes(30)
                            .build()
            ), ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress())).build(),
            ServerSettings.builder().build());

    private final ClusterDescription unconnectedDescription = new ClusterDescription(MULTIPLE, REPLICA_SET,
            Collections.singletonList(
                    ServerDescription.builder().ok(true)
                            .state(CONNECTING)
                            .address(new ServerAddress())
                            .type(UNKNOWN)
                            .logicalSessionTimeoutMinutes(null)
                            .build()
            ), ClusterSettings.builder().hosts(Collections.singletonList(new ServerAddress())).build(),
            ServerSettings.builder().build());

    @Test
    void shouldGetSession() {
        Cluster cluster = mock(Cluster.class);
        when(cluster.getCurrentDescription()).thenReturn(connectedDescription);
        ServerSessionPool pool = new ServerSessionPool(cluster, TIMEOUT_SETTINGS, getServerApi());

        ServerSession session = pool.get();
        assertNotNull(session);
    }

    @Test
    void shouldThrowIllegalStateExceptionIfPoolIsClosed() {
        Cluster cluster = mock(Cluster.class);
        when(cluster.getCurrentDescription()).thenReturn(connectedDescription);
        ServerSessionPool pool = new ServerSessionPool(cluster, TIMEOUT_SETTINGS, getServerApi());
        pool.close();

        assertThrows(IllegalStateException.class, pool::get);
    }

    @Test
    void shouldPoolSession() {
        Cluster cluster = mock(Cluster.class);
        when(cluster.getCurrentDescription()).thenReturn(connectedDescription);
        ServerSessionPool pool = new ServerSessionPool(cluster, TIMEOUT_SETTINGS, getServerApi());
        ServerSession session = pool.get();

        pool.release(session);
        ServerSession pooledSession = pool.get();

        assertEquals(session, pooledSession);
    }

    @Test
    void shouldPruneSessionsWhenGetting() {
        Cluster cluster = mock(Cluster.class);
        when(cluster.getCurrentDescription()).thenReturn(connectedDescription);

        long[] clockValues = {0, TimeUnit.MINUTES.toMillis(29) + 1, TimeUnit.MINUTES.toMillis(29) + 1};
        int[] clockIndex = {0};
        ServerSessionPool.Clock clock = () -> clockValues[clockIndex[0]++];

        ServerSessionPool pool = new ServerSessionPool(cluster, OPERATION_CONTEXT, clock);
        ServerSession sessionOne = pool.get();

        pool.release(sessionOne);
        assertFalse(((ServerSessionPool.ServerSessionImpl) sessionOne).isClosed());

        ServerSession sessionTwo = pool.get();
        assertNotEquals(sessionTwo, sessionOne);
        assertTrue(((ServerSessionPool.ServerSessionImpl) sessionOne).isClosed());
    }

    @Test
    void shouldNotPruneSessionWhenTimeoutIsNull() {
        Cluster cluster = mock(Cluster.class);
        when(cluster.getCurrentDescription()).thenReturn(unconnectedDescription);

        ServerSessionPool.Clock clock = () -> 0;
        ServerSessionPool pool = new ServerSessionPool(cluster, OPERATION_CONTEXT, clock);
        ServerSession session = pool.get();

        pool.release(session);
        ServerSession newSession = pool.get();

        assertEquals(session, newSession);
    }

    @Test
    void shouldInitializeSession() {
        Cluster cluster = mock(Cluster.class);
        when(cluster.getCurrentDescription()).thenReturn(connectedDescription);

        ServerSessionPool.Clock clock = () -> 42;
        ServerSessionPool pool = new ServerSessionPool(cluster, OPERATION_CONTEXT, clock);

        ServerSessionPool.ServerSessionImpl session = (ServerSessionPool.ServerSessionImpl) pool.get();
        assertEquals(42, session.getLastUsedAtMillis());
        assertEquals(0, session.getTransactionNumber());
        assertNotNull(session.getIdentifier().getBinary("id"));
        assertEquals(BsonBinarySubType.UUID_STANDARD.getValue(), session.getIdentifier().getBinary("id").getType());
        assertEquals(16, session.getIdentifier().getBinary("id").getData().length);
    }

    @Test
    void shouldAdvanceTransactionNumber() {
        Cluster cluster = mock(Cluster.class);
        when(cluster.getCurrentDescription()).thenReturn(connectedDescription);

        ServerSessionPool.Clock clock = () -> 42;
        ServerSessionPool pool = new ServerSessionPool(cluster, OPERATION_CONTEXT, clock);

        ServerSessionPool.ServerSessionImpl session = (ServerSessionPool.ServerSessionImpl) pool.get();
        assertEquals(0, session.getTransactionNumber());
        assertEquals(1, session.advanceTransactionNumber());
        assertEquals(1, session.getTransactionNumber());
    }

    @Test
    void shouldEndPooledSessionsWhenPoolIsClosed() {
        Connection connection = mock(Connection.class);
        Server server = mock(Server.class);
        when(server.getConnection(any())).thenReturn(connection);

        Cluster cluster = mock(Cluster.class);
        when(cluster.getCurrentDescription()).thenReturn(connectedDescription);

        ServerSessionPool pool = new ServerSessionPool(cluster, TIMEOUT_SETTINGS, getServerApi());
        List<ServerSession> sessions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            sessions.add(pool.get());
        }
        for (ServerSession cur : sessions) {
            pool.release(cur);
        }

        when(cluster.selectServer(any(), any())).thenReturn(
                new ServerTuple(server, connectedDescription.getServerDescriptions().get(0)));
        when(connection.command(any(), any(), any(), any(), any(), any())).thenReturn(new org.bson.BsonDocument());

        pool.close();

        verify(cluster).selectServer(any(), any());
        verify(connection).command(any(), any(), any(), any(), any(), any());
        verify(connection).release();
    }
}
