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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncClusterAwareReadWriteBinding;
import com.mongodb.internal.binding.AsyncClusterBinding;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.Server;
import com.mongodb.internal.connection.ServerTuple;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.selector.ServerSelector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.mockito.Mockito;

@ExtendWith(MockitoExtension.class)
class ClientSessionBindingTest {

    @Test
    @DisplayName("should return the session context from the connection source")
    void shouldReturnTheSessionContextFromTheConnectionSource() {
        ClientSession session = mock(ClientSession.class);
        AsyncClusterAwareReadWriteBinding wrappedBinding = mock(AsyncClusterAwareReadWriteBinding.class);
        when(wrappedBinding.retain()).thenReturn(wrappedBinding);

        AsyncConnectionSource connectionSource = mock(AsyncConnectionSource.class);

        doAnswer(invocation -> {
            SingleResultCallback<AsyncConnectionSource> callback = invocation.getArgument(1);
            callback.onResult(connectionSource, null);
            return null;
        }).when(wrappedBinding).getReadConnectionSource(eq(OPERATION_CONTEXT), any());

        AsyncConnectionSource writeConnectionSource = mock(AsyncConnectionSource.class);

        doAnswer(invocation -> {
            SingleResultCallback<AsyncConnectionSource> callback = invocation.getArgument(1);
            callback.onResult(writeConnectionSource, null);
            return null;
        }).when(wrappedBinding).getWriteConnectionSource(eq(OPERATION_CONTEXT), any());

        ClientSessionBinding binding = new ClientSessionBinding(session, false, wrappedBinding);

        FutureResultCallback<AsyncConnectionSource> futureResultCallback = new FutureResultCallback<>();
        binding.getReadConnectionSource(OPERATION_CONTEXT, futureResultCallback);

        verify(wrappedBinding, times(1)).getReadConnectionSource(eq(OPERATION_CONTEXT), any());

        futureResultCallback = new FutureResultCallback<>();
        binding.getWriteConnectionSource(OPERATION_CONTEXT, futureResultCallback);

        verify(wrappedBinding, times(1)).getWriteConnectionSource(eq(OPERATION_CONTEXT), any());
    }

    @Test
    @DisplayName("should close client session when binding reference count drops to zero if it is owned by the binding")
    void shouldCloseClientSessionWhenBindingReferenceCountDropsToZeroIfOwnedByBinding() {
        ClientSession session = mock(ClientSession.class);
        AsyncClusterAwareReadWriteBinding wrappedBinding = createStubBinding();
        ClientSessionBinding binding = new ClientSessionBinding(session, true, wrappedBinding);
        binding.retain();

        binding.release();
        verify(session, never()).close();

        binding.release();
        verify(session, times(1)).close();
    }

    @Test
    @DisplayName("should close client session when binding reference count drops to zero due to connection source if it is owned by the binding")
    void shouldCloseClientSessionWhenBindingReferenceCountDropsToZeroDueToConnectionSourceIfOwnedByBinding() {
        ClientSession session = mock(ClientSession.class);
        AsyncClusterAwareReadWriteBinding wrappedBinding = createStubBinding();
        ClientSessionBinding binding = new ClientSessionBinding(session, true, wrappedBinding);

        FutureResultCallback<AsyncConnectionSource> futureResultCallback = new FutureResultCallback<>();
        binding.getReadConnectionSource(OPERATION_CONTEXT, futureResultCallback);
        AsyncConnectionSource readConnectionSource = futureResultCallback.get();

        futureResultCallback = new FutureResultCallback<>();
        binding.getWriteConnectionSource(OPERATION_CONTEXT, futureResultCallback);
        AsyncConnectionSource writeConnectionSource = futureResultCallback.get();

        binding.release();
        verify(session, never()).close();

        writeConnectionSource.release();
        verify(session, never()).close();

        readConnectionSource.release();
        verify(session, times(1)).close();
    }

    @Test
    @DisplayName("should not close client session when binding reference count drops to zero if it is not owned by the binding")
    void shouldNotCloseClientSessionWhenBindingReferenceCountDropsToZeroIfNotOwnedByBinding() {
        ClientSession session = mock(ClientSession.class);
        AsyncClusterAwareReadWriteBinding wrappedBinding = createStubBinding();
        ClientSessionBinding binding = new ClientSessionBinding(session, false, wrappedBinding);
        binding.retain();

        binding.release();
        verify(session, never()).close();

        binding.release();
        verify(session, never()).close();
    }

    @SuppressWarnings("unchecked")
    private AsyncClusterAwareReadWriteBinding createStubBinding() {
        Cluster cluster = mock(Cluster.class);
        Mockito.lenient().doAnswer(invocation -> {
            SingleResultCallback<ServerTuple> callback = invocation.getArgument(2);
            callback.onResult(new ServerTuple(mock(Server.class), ServerDescription.builder()
                    .type(ServerType.STANDALONE)
                    .state(ServerConnectionState.CONNECTED)
                    .address(new ServerAddress())
                    .build()), null);
            return null;
        }).when(cluster).selectServerAsync(any(ServerSelector.class), any(OperationContext.class), any(SingleResultCallback.class));

        return new AsyncClusterBinding(cluster, ReadPreference.primary());
    }
}
