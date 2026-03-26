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

package com.mongodb.internal.binding;

import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.Server;
import com.mongodb.internal.connection.ServerTuple;
import org.junit.jupiter.api.Test;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SingleServerBindingTest {

    private Cluster createCluster() {
        Cluster cluster = mock(Cluster.class);
        when(cluster.selectServer(any(), any())).thenReturn(new ServerTuple(mock(Server.class),
                ServerDescription.builder()
                        .type(ServerType.STANDALONE)
                        .state(ServerConnectionState.CONNECTED)
                        .address(new ServerAddress())
                        .build()));
        return cluster;
    }

    @Test
    void shouldImplementGetters() {
        Cluster cluster = createCluster();
        ServerAddress address = new ServerAddress();

        SingleServerBinding binding = new SingleServerBinding(cluster, address);
        assertEquals(ReadPreference.primary(), binding.getReadPreference());
    }

    @Test
    void shouldIncrementAndDecrementReferenceCounts() {
        Cluster cluster = createCluster();
        ServerAddress address = new ServerAddress();

        SingleServerBinding binding = new SingleServerBinding(cluster, address);
        assertEquals(1, binding.getCount());

        ConnectionSource source = binding.getReadConnectionSource(OPERATION_CONTEXT);
        assertEquals(1, source.getCount());
        assertEquals(2, binding.getCount());

        source.retain();
        assertEquals(2, source.getCount());
        assertEquals(2, binding.getCount());

        source.release();
        assertEquals(1, source.getCount());
        assertEquals(2, binding.getCount());

        source.release();
        assertEquals(0, source.getCount());
        assertEquals(1, binding.getCount());

        source = binding.getWriteConnectionSource(OPERATION_CONTEXT);
        assertEquals(1, source.getCount());
        assertEquals(2, binding.getCount());

        source.retain();
        assertEquals(2, source.getCount());
        assertEquals(2, binding.getCount());

        source.release();
        assertEquals(1, source.getCount());
        assertEquals(2, binding.getCount());

        source.release();
        assertEquals(0, source.getCount());
        assertEquals(1, binding.getCount());

        binding.release();
        assertEquals(0, binding.getCount());
    }
}
