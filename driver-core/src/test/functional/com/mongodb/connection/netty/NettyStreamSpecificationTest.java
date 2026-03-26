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

package com.mongodb.connection.netty;

import com.mongodb.MongoSocketOpenException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.internal.connection.Stream;
import com.mongodb.internal.connection.netty.NettyStreamFactory;
import com.mongodb.spi.dns.InetAddressResolver;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ClusterFixture.getSslSettings;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class NettyStreamSpecificationTest {

    @Tag("Slow")
    @Test
    void shouldSuccessfullyConnectWithWorkingIpAddressGroup() throws java.io.IOException {
        assumeTrue(!getSslSettings().isEnabled());

        SocketSettings socketSettings = SocketSettings.builder().connectTimeout(1000, TimeUnit.MILLISECONDS).build();
        SslSettings sslSettings = SslSettings.builder().build();
        InetAddressResolver inetAddressResolver = host -> {
            try {
                return Arrays.asList(
                        InetAddress.getByName("192.168.255.255"),
                        InetAddress.getByName("1.2.3.4"),
                        InetAddress.getByName("127.0.0.1"));
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        };
        NettyStreamFactory factory = new NettyStreamFactory(inetAddressResolver, socketSettings, sslSettings,
                new NioEventLoopGroup(), NioSocketChannel.class, PooledByteBufAllocator.DEFAULT, null);

        Stream stream = factory.create(new ServerAddress());
        stream.open(OPERATION_CONTEXT);

        assertFalse(stream.isClosed());
    }

    @Tag("Slow")
    @Test
    void shouldThrowExceptionWithNonWorkingIpAddressGroup() {
        assumeTrue(!getSslSettings().isEnabled());

        SocketSettings socketSettings = SocketSettings.builder().connectTimeout(1000, TimeUnit.MILLISECONDS).build();
        SslSettings sslSettings = SslSettings.builder().build();
        InetAddressResolver inetAddressResolver = host -> {
            try {
                return Arrays.asList(
                        InetAddress.getByName("192.168.255.255"),
                        InetAddress.getByName("1.2.3.4"),
                        InetAddress.getByName("1.2.3.5"));
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        };
        NettyStreamFactory factory = new NettyStreamFactory(inetAddressResolver, socketSettings, sslSettings,
                new NioEventLoopGroup(), NioSocketChannel.class, PooledByteBufAllocator.DEFAULT, null);

        Stream stream = factory.create(new ServerAddress());

        assertThrows(MongoSocketOpenException.class, () -> stream.open(OPERATION_CONTEXT));
    }

    // NOTE: The 'should fail AsyncCompletionHandler if name resolution fails' test used Spock Stub
    // for ServerAddress which is not directly mockable in JUnit without Mockito.
    // TODO: Convert with Mockito if needed
}
