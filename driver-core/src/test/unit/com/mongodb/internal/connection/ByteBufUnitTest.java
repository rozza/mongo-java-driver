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

import com.mongodb.internal.connection.netty.NettyByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import org.bson.ByteBuf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ByteBufUnitTest {

    static Stream<BufferProvider> providerStream() {
        return Stream.of(new NettyBufferProvider(), new SimpleBufferProvider());
    }

    @ParameterizedTest
    @MethodSource("providerStream")
    void shouldPutAByte(BufferProvider provider) {
        ByteBuf buffer = provider.getBuffer(1024);
        try {
            buffer.put((byte) 42);
            buffer.flip();
            assertEquals(42, buffer.get());
        } finally {
            buffer.release();
        }
    }

    @ParameterizedTest
    @MethodSource("providerStream")
    void shouldPutSeveralBytes(BufferProvider provider) {
        ByteBuf buffer = provider.getBuffer(1024);
        try {
            buffer.put((byte) 42);
            buffer.put((byte) 43);
            buffer.put((byte) 44);
            buffer.flip();
            assertEquals(42, buffer.get());
            assertEquals(43, buffer.get());
            assertEquals(44, buffer.get());
        } finally {
            buffer.release();
        }
    }

    @ParameterizedTest
    @MethodSource("providerStream")
    void shouldPutBytesAtIndex(BufferProvider provider) {
        ByteBuf buffer = provider.getBuffer(1024);
        try {
            buffer.put((byte) 0);
            buffer.put((byte) 0);
            buffer.put((byte) 0);
            buffer.put((byte) 0);
            buffer.put((byte) 43);
            buffer.put((byte) 44);
            buffer.put(0, (byte) 22);
            buffer.put(1, (byte) 23);
            buffer.put(2, (byte) 24);
            buffer.put(3, (byte) 25);
            buffer.flip();

            assertEquals(22, buffer.get());
            assertEquals(23, buffer.get());
            assertEquals(24, buffer.get());
            assertEquals(25, buffer.get());
            assertEquals(43, buffer.get());
            assertEquals(44, buffer.get());
        } finally {
            buffer.release();
        }
    }

    @ParameterizedTest
    @MethodSource("providerStream")
    void whenWritingRemainingShouldBeTheNumberOfBytesThatCanBeWritten(BufferProvider provider) {
        ByteBuf buffer = provider.getBuffer(1024);
        try {
            assertEquals(1024, buffer.remaining());
            buffer.put((byte) 1);
            assertEquals(1023, buffer.remaining());
        } finally {
            buffer.release();
        }
    }

    @ParameterizedTest
    @MethodSource("providerStream")
    void whenWritingHasRemainingShouldBeTrueIfThereIsStillRoomToWrite(BufferProvider provider) {
        ByteBuf buffer = provider.getBuffer(2);
        try {
            assertTrue(buffer.hasRemaining());
            buffer.put((byte) 1);
            assertTrue(buffer.hasRemaining());
            buffer.put((byte) 1);
            assertFalse(buffer.hasRemaining());
        } finally {
            buffer.release();
        }
    }

    @ParameterizedTest
    @MethodSource("providerStream")
    void shouldReturnNioBufferWithSameCapacityAndLimit(BufferProvider provider) {
        ByteBuf buffer = provider.getBuffer(36);
        try {
            ByteBuffer nioBuffer = buffer.asNIO();
            assertEquals(36, nioBuffer.limit());
            assertEquals(0, nioBuffer.position());
            assertEquals(36, nioBuffer.remaining());
        } finally {
            buffer.release();
        }
    }

    @ParameterizedTest
    @MethodSource("providerStream")
    void shouldReturnNioBufferWithSameContents(BufferProvider provider) {
        ByteBuf buffer = provider.getBuffer(1024);
        try {
            buffer.put((byte) 42);
            buffer.put((byte) 43);
            buffer.put((byte) 44);
            buffer.put((byte) 45);
            buffer.put((byte) 46);
            buffer.put((byte) 47);
            buffer.flip();

            ByteBuffer nioBuffer = buffer.asNIO();
            assertEquals(6, nioBuffer.limit());
            assertEquals(0, nioBuffer.position());
            assertEquals(42, nioBuffer.get());
            assertEquals(43, nioBuffer.get());
            assertEquals(44, nioBuffer.get());
            assertEquals(45, nioBuffer.get());
            assertEquals(46, nioBuffer.get());
            assertEquals(47, nioBuffer.get());
            assertEquals(0, nioBuffer.remaining());
        } finally {
            buffer.release();
        }
    }

    @ParameterizedTest
    @MethodSource("providerStream")
    void shouldEnforceReferenceCounts(BufferProvider provider) {
        ByteBuf buffer = provider.getBuffer(1024);
        buffer.put((byte) 1);
        assertEquals(1, buffer.getReferenceCount());

        buffer.retain();
        buffer.put((byte) 1);
        assertEquals(2, buffer.getReferenceCount());

        buffer.release();
        buffer.put((byte) 1);
        assertEquals(1, buffer.getReferenceCount());

        buffer.release();
        assertEquals(0, buffer.getReferenceCount());

        assertThrows(Exception.class, () -> buffer.put((byte) 1));
    }

    static final class NettyBufferProvider implements BufferProvider {
        private final ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;

        @Override
        public ByteBuf getBuffer(final int size) {
            io.netty.buffer.ByteBuf buffer = allocator.directBuffer(size, size);
            return new NettyByteBuf(buffer);
        }
    }
}
