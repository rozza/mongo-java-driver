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

package com.mongodb.internal.connection;

import com.mongodb.LoggerSettings;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.OperationFunctionalSpecification;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.SocketSettings;
import com.mongodb.internal.connection.netty.NettyStreamFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.CLIENT_METADATA;
import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ClusterFixture.getCredentialWithCache;
import static com.mongodb.ClusterFixture.getServerApi;
import static com.mongodb.ClusterFixture.getSslSettings;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag("Slow")
class AsyncStreamTimeoutsSpecificationTest extends OperationFunctionalSpecification {

    private static final SocketSettings OPEN_SOCKET_SETTINGS = SocketSettings.builder()
            .connectTimeout(1, TimeUnit.MILLISECONDS).build();

    @Test
    void shouldThrowMongoSocketOpenExceptionWhenAsynchronousSocketStreamFailsToOpen() {
        assumeTrue(!getSslSettings().isEnabled());

        InternalConnection connection = new InternalStreamConnectionFactory(ClusterConnectionMode.SINGLE,
                new AsynchronousSocketChannelStreamFactory(new DefaultInetAddressResolver(), OPEN_SOCKET_SETTINGS, getSslSettings()),
                getCredentialWithCache(), CLIENT_METADATA, Collections.emptyList(),
                LoggerSettings.builder().build(), null, getServerApi())
                .create(new ServerId(new ClusterId(),
                        new ServerAddress(new InetSocketAddress("192.168.255.255", 27017))));

        assertThrows(MongoSocketOpenException.class, () -> connection.open(OPERATION_CONTEXT));
    }

    @Test
    void shouldThrowMongoSocketOpenExceptionWhenNettyStreamFailsToOpen() {
        InternalConnection connection = new InternalStreamConnectionFactory(ClusterConnectionMode.SINGLE,
                new NettyStreamFactory(OPEN_SOCKET_SETTINGS, getSslSettings()),
                getCredentialWithCache(), CLIENT_METADATA, Collections.emptyList(),
                LoggerSettings.builder().build(), null, getServerApi())
                .create(new ServerId(new ClusterId(),
                        new ServerAddress(new InetSocketAddress("192.168.255.255", 27017))));

        assertThrows(MongoSocketOpenException.class, () -> connection.open(OPERATION_CONTEXT));
    }
}
