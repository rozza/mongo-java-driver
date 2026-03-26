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
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY;
import static com.mongodb.connection.ServerType.REPLICA_SET_SECONDARY;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PrimaryServerSelectorTest {

    private static final ServerDescription PRIMARY_SERVER = ServerDescription.builder()
            .state(CONNECTED)
            .address(new ServerAddress())
            .ok(true)
            .type(REPLICA_SET_PRIMARY)
            .build();

    private static final ServerDescription SECONDARY_SERVER = ServerDescription.builder()
            .state(CONNECTED)
            .address(new ServerAddress())
            .ok(true)
            .type(REPLICA_SET_SECONDARY)
            .build();

    static Stream<Arguments> shouldChoosePrimaryServer() {
        return Stream.of(
                Arguments.of(
                        Collections.singletonList(PRIMARY_SERVER),
                        new ClusterDescription(MULTIPLE, ClusterType.REPLICA_SET, Collections.singletonList(PRIMARY_SERVER))),
                Arguments.of(
                        Collections.singletonList(PRIMARY_SERVER),
                        new ClusterDescription(MULTIPLE, ClusterType.REPLICA_SET, Arrays.asList(PRIMARY_SERVER, SECONDARY_SERVER))),
                Arguments.of(
                        Collections.emptyList(),
                        new ClusterDescription(MULTIPLE, ClusterType.REPLICA_SET, Collections.singletonList(SECONDARY_SERVER)))
        );
    }

    @ParameterizedTest
    @MethodSource
    void shouldChoosePrimaryServer(final List<ServerDescription> expectedServerList,
                                   final ClusterDescription clusterDescription) {
        PrimaryServerSelector selector = new PrimaryServerSelector();
        assertEquals(expectedServerList, selector.select(clusterDescription));
    }
}
