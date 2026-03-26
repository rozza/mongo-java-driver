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

package com.mongodb.client.internal;

import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.ClientSession;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.binding.ClusterAwareReadWriteBinding;
import com.mongodb.internal.binding.ClusterBinding;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.Server;
import com.mongodb.internal.connection.ServerTuple;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ClientSessionBindingTest {

    @Test
    @DisplayName("should call underlying wrapped binding")
    void shouldCallUnderlyingWrappedBinding() {
        ClientSession session = mock(ClientSession.class);
        ClusterBinding wrappedBinding = mock(ClusterBinding.class);
        ClientSessionBinding binding = new ClientSessionBinding(session, false, wrappedBinding);

        when(wrappedBinding.getReadConnectionSource(OPERATION_CONTEXT)).thenReturn(mock(ConnectionSource.class));
        binding.getReadConnectionSource(OPERATION_CONTEXT);
        verify(wrappedBinding).getReadConnectionSource(OPERATION_CONTEXT);

        when(wrappedBinding.getWriteConnectionSource(OPERATION_CONTEXT)).thenReturn(mock(ConnectionSource.class));
        binding.getWriteConnectionSource(OPERATION_CONTEXT);
        verify(wrappedBinding).getWriteConnectionSource(OPERATION_CONTEXT);
    }

    @Test
    @DisplayName("should close client session when binding reference count drops to zero if it is owned by the binding")
    void shouldCloseClientSessionWhenBindingRefCountDropsToZeroIfOwned() {
        ClientSession session = mock(ClientSession.class);
        ClusterAwareReadWriteBinding wrappedBinding = createStubBinding();
        ClientSessionBinding binding = new ClientSessionBinding(session, true, wrappedBinding);
        binding.retain();

        binding.release();
        verify(session, never()).close();

        binding.release();
        verify(session).close();
    }

    @Test
    @DisplayName("should close client session when binding reference count drops to zero due to connection source if it is owned")
    void shouldCloseClientSessionWhenRefCountDropsDueToConnectionSource() {
        ClientSession session = mock(ClientSession.class);
        ClusterAwareReadWriteBinding wrappedBinding = createStubBinding();
        ClientSessionBinding binding = new ClientSessionBinding(session, true, wrappedBinding);
        ConnectionSource readConnectionSource = binding.getReadConnectionSource(OPERATION_CONTEXT);
        ConnectionSource writeConnectionSource = binding.getWriteConnectionSource(OPERATION_CONTEXT);

        binding.release();
        verify(session, never()).close();

        writeConnectionSource.release();
        verify(session, never()).close();

        readConnectionSource.release();
        verify(session).close();
    }

    @Test
    @DisplayName("should not close client session when binding reference count drops to zero if it is not owned by the binding")
    void shouldNotCloseClientSessionWhenRefCountDropsToZeroIfNotOwned() {
        ClientSession session = mock(ClientSession.class);
        ClusterAwareReadWriteBinding wrappedBinding = createStubBinding();
        ClientSessionBinding binding = new ClientSessionBinding(session, false, wrappedBinding);
        binding.retain();

        binding.release();
        verify(session, never()).close();

        binding.release();
        verify(session, never()).close();
    }

    private ClusterAwareReadWriteBinding createStubBinding() {
        Cluster cluster = mock(Cluster.class);
        ServerTuple serverTuple = new ServerTuple(mock(Server.class),
                ServerDescription.builder().address(new ServerAddress())
                        .state(com.mongodb.connection.ServerConnectionState.CONNECTED).build());
        when(cluster.selectServer(any(), any())).thenReturn(serverTuple);
        return new ClusterBinding(cluster, ReadPreference.primary());
    }
}
