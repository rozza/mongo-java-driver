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

package com.mongodb.internal.connection.netty;

import com.mongodb.ClusterFixture;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.socket.oio.OioSocketChannel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NettyStreamFactoryTest {

    private static final SocketSettings SOCKET_SETTINGS = SocketSettings.builder().build();
    private static final SslSettings SSL_SETTINGS = SslSettings.builder().enabled(true).invalidHostNameAllowed(true).build();
    private static final OioEventLoopGroup EVENT_LOOP_GROUP = new OioEventLoopGroup();
    @SuppressWarnings("unchecked")
    private static final Class<OioSocketChannel> SOCKET_CHANNEL_CLASS = OioSocketChannel.class;
    private static final UnpooledByteBufAllocator ALLOCATOR = UnpooledByteBufAllocator.DEFAULT;

    @AfterAll
    static void afterAll() {
        EVENT_LOOP_GROUP.shutdownGracefully().awaitUninterruptibly();
    }

    static Stream<Object[]> factoryProvider() {
        return Stream.of(
                new Object[]{NioEventLoopGroup.class, NioSocketChannel.class, PooledByteBufAllocator.DEFAULT,
                        new NettyStreamFactory(SOCKET_SETTINGS, SSL_SETTINGS)},
                new Object[]{OioEventLoopGroup.class, NioSocketChannel.class, PooledByteBufAllocator.DEFAULT,
                        new NettyStreamFactory(SOCKET_SETTINGS, SSL_SETTINGS, EVENT_LOOP_GROUP)},
                new Object[]{OioEventLoopGroup.class, NioSocketChannel.class, ALLOCATOR,
                        new NettyStreamFactory(SOCKET_SETTINGS, SSL_SETTINGS, EVENT_LOOP_GROUP, ALLOCATOR)},
                new Object[]{OioEventLoopGroup.class, SOCKET_CHANNEL_CLASS, ALLOCATOR,
                        new NettyStreamFactory(SOCKET_SETTINGS, SSL_SETTINGS, EVENT_LOOP_GROUP, SOCKET_CHANNEL_CLASS, ALLOCATOR)}
        );
    }

    @ParameterizedTest
    @MethodSource("factoryProvider")
    void shouldUseArgumentsToCreateNettyStream(Class<? extends EventLoopGroup> eventLoopGroupClass,
                                               Class<?> socketChannelClass,
                                               Object allocator,
                                               NettyStreamFactory factory) {
        NettyStream stream = (NettyStream) factory.create(ClusterFixture.getPrimary());

        try {
            assertEquals(ClusterFixture.getPrimary(), stream.getAddress());
            assertEquals(SOCKET_SETTINGS, stream.getSettings());
            assertEquals(SSL_SETTINGS, stream.getSslSettings());
            assertEquals(socketChannelClass, stream.getSocketChannelClass());
            assertEquals(eventLoopGroupClass, stream.getWorkerGroup().getClass());
            assertEquals(allocator, stream.getAllocator());
        } finally {
            stream.close();
            if (stream.getWorkerGroup().getClass() != eventLoopGroupClass) {
                stream.getWorkerGroup().shutdownGracefully().awaitUninterruptibly();
            }
        }
    }
}
