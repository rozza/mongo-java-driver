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

import io.netty.buffer.ByteBufAllocator;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ByteBufTest {

    static Stream<ByteBuf> bufProvider() {
        return Stream.of(
                new ByteBufNIO(ByteBuffer.allocate(16)),
                new NettyByteBuf(ByteBufAllocator.DEFAULT.buffer(16))
        );
    }

    @ParameterizedTest
    @MethodSource("bufProvider")
    void shouldSetPositionAndLimitCorrectly(ByteBuf buf) {
        assertEquals(16, buf.capacity());
        assertEquals(0, buf.position());
        assertEquals(16, buf.limit());

        buf.put(new byte[10], 0, 10);
        assertEquals(10, buf.position());
        assertEquals(16, buf.limit());

        buf.flip();
        assertEquals(0, buf.position());
        assertEquals(10, buf.limit());

        buf.position(3);
        assertEquals(3, buf.position());
        assertEquals(10, buf.limit());

        buf.limit(7);
        assertEquals(3, buf.position());
        assertEquals(7, buf.limit());

        buf.get(new byte[4]);
        assertEquals(7, buf.position());
        assertEquals(7, buf.limit());
    }

    @Test
    void shouldThrowWhenSettingLimitWhileWriting() {
        NettyByteBuf buf = new NettyByteBuf(ByteBufAllocator.DEFAULT.buffer(16));

        assertThrows(UnsupportedOperationException.class, () -> buf.limit(10));
    }

    @Test
    void shouldManageReferenceCountOfProxiedNettyByteBufCorrectly() {
        io.netty.buffer.ByteBuf nettyBuf = ByteBufAllocator.DEFAULT.buffer(16);

        NettyByteBuf buf = new NettyByteBuf(nettyBuf);
        assertEquals(1, nettyBuf.refCnt());

        buf.retain();
        assertEquals(2, nettyBuf.refCnt());

        buf.release();
        assertEquals(1, nettyBuf.refCnt());

        buf.release();
        assertEquals(0, nettyBuf.refCnt());
    }

    @Test
    void shouldManageReferenceCountOfDuplicatedProxiedNettyByteBufCorrectly() {
        io.netty.buffer.ByteBuf nettyBuf = ByteBufAllocator.DEFAULT.buffer(16);
        NettyByteBuf buf = new NettyByteBuf(nettyBuf);

        ByteBuf duplicated = buf.duplicate();
        assertEquals(2, nettyBuf.refCnt());

        buf.retain();
        assertEquals(3, nettyBuf.refCnt());

        buf.release();
        assertEquals(2, nettyBuf.refCnt());

        duplicated.release();
        assertEquals(1, nettyBuf.refCnt());

        buf.release();
        assertEquals(0, nettyBuf.refCnt());
    }
}
