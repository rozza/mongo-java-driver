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

package com.mongodb.internal.selector;

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.ReadPreference.secondary;
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReadPreferenceServerSelectorTest {

    private final ServerDescription primaryServer = ServerDescription.builder()
            .state(CONNECTED)
            .address(new ServerAddress())
            .ok(true)
            .type(ServerType.REPLICA_SET_PRIMARY)
            .build();

    private final ServerDescription secondaryServer = ServerDescription.builder()
            .state(CONNECTED)
            .address(new ServerAddress("localhost", 27018))
            .ok(true)
            .type(ServerType.REPLICA_SET_SECONDARY)
            .build();

    @Test
    void constructorShouldThrowIfReadPreferenceIsNull() {
        assertThrows(IllegalArgumentException.class, () -> new ReadPreferenceServerSelector(null));
    }

    @Test
    void shouldGetReadPreference() {
        assertEquals(primary(), new ReadPreferenceServerSelector(primary()).getReadPreference());
    }

    @Test
    void shouldOverrideToString() {
        assertEquals("ReadPreferenceServerSelector{readPreference=primary}",
                new ReadPreferenceServerSelector(primary()).toString());
    }

    @Test
    void shouldSelectServerMatchingReadPreferenceWhenConnectionModeIsMultiple() {
        assertEquals(Collections.singletonList(primaryServer),
                new ReadPreferenceServerSelector(primary())
                        .select(new ClusterDescription(MULTIPLE, REPLICA_SET, Arrays.asList(primaryServer, secondaryServer))));
        assertEquals(Collections.singletonList(secondaryServer),
                new ReadPreferenceServerSelector(secondary())
                        .select(new ClusterDescription(MULTIPLE, REPLICA_SET, Arrays.asList(primaryServer, secondaryServer))));
    }

    @Test
    void shouldSelectAnyOkServerWhenConnectionModeIsSingle() {
        assertEquals(Collections.singletonList(secondaryServer),
                new ReadPreferenceServerSelector(primary())
                        .select(new ClusterDescription(SINGLE, REPLICA_SET, Collections.singletonList(secondaryServer))));
    }
}
