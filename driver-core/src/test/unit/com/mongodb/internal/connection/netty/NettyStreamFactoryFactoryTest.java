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

import com.mongodb.ServerAddress;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.NettyTransportSettings;
import com.mongodb.connection.TransportSettings;
import com.mongodb.internal.connection.DefaultInetAddressResolver;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.socket.oio.OioSocketChannel;
import io.netty.handler.ssl.SslContextBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.net.ssl.SSLException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NettyStreamFactoryFactoryTest {

    private final SocketSettings socketSettings = SocketSettings.builder().build();
    private final SslSettings sslSettings = SslSettings.builder().build();
    private final ServerAddress serverAddress = new ServerAddress();

    private static final NettyStreamFactoryFactory DEFAULT_FACTORY = NettyStreamFactoryFactory.builder()
            .inetAddressResolver(new DefaultInetAddressResolver())
            .build();
    private static final NettyStreamFactoryFactory CUSTOM_FACTORY = NettyStreamFactoryFactory.builder()
            .allocator(UnpooledByteBufAllocator.DEFAULT)
            .socketChannelClass(OioSocketChannel.class)
            .eventLoopGroup(new OioEventLoopGroup())
            .inetAddressResolver(new DefaultInetAddressResolver())
            .build();

    @Test
    void shouldApplyNettySettings() throws SSLException {
        NettyTransportSettings nettySettings = TransportSettings.nettyBuilder()
                .allocator(UnpooledByteBufAllocator.DEFAULT)
                .socketChannelClass(OioSocketChannel.class)
                .eventLoopGroup(new OioEventLoopGroup())
                .sslContext(SslContextBuilder.forClient().build())
                .build();

        NettyStreamFactoryFactory factoryFactory = NettyStreamFactoryFactory.builder()
                .inetAddressResolver(new DefaultInetAddressResolver())
                .applySettings(nettySettings)
                .build();

        assertEquals(nettySettings.getAllocator(), factoryFactory.getAllocator());
        assertEquals(nettySettings.getEventLoopGroup(), factoryFactory.getEventLoopGroup());
        assertEquals(nettySettings.getSocketChannelClass(), factoryFactory.getSocketChannelClass());
        assertEquals(nettySettings.getSslContext(), factoryFactory.getSslContext());
    }

    static Stream<Object[]> factoryProvider() {
        return Stream.of(
                new Object[]{"default", DEFAULT_FACTORY, ByteBufAllocator.DEFAULT, NioSocketChannel.class, NioEventLoopGroup.class},
                new Object[]{"custom", CUSTOM_FACTORY, UnpooledByteBufAllocator.DEFAULT, OioSocketChannel.class, OioEventLoopGroup.class}
        );
    }

    @ParameterizedTest
    @MethodSource("factoryProvider")
    void shouldCreateTheExpectedNettyStream(String description, NettyStreamFactoryFactory factoryFactory,
                                            ByteBufAllocator allocator, Class<?> socketChannelClass,
                                            Class<?> eventLoopGroupClass) {
        NettyStreamFactory factory = (NettyStreamFactory) factoryFactory.create(socketSettings, sslSettings);

        NettyStream stream = (NettyStream) factory.create(serverAddress);

        assertEquals(socketSettings, stream.getSettings());
        assertEquals(sslSettings, stream.getSslSettings());
        assertEquals(serverAddress, stream.getAddress());
        assertEquals(allocator, stream.getAllocator());
        assertEquals(socketChannelClass, stream.getSocketChannelClass());
        assertEquals(eventLoopGroupClass, stream.getWorkerGroup().getClass());
    }
}
