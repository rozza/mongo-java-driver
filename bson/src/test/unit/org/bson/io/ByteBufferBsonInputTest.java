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

package org.bson.io;

import org.bson.BsonSerializationException;
import org.bson.ByteBufNIO;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ByteBufferBsonInputTest {

    @Test
    void constructorShouldThrowIfBufferIsNull() {
        assertThrows(IllegalArgumentException.class, () -> new ByteBufferBsonInput(null));
    }

    @Test
    void positionShouldStartAtZero() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[4])));
        assertEquals(0, stream.getPosition());
    }

    @Test
    void shouldReadAByte() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[]{11})));
        assertEquals(11, stream.readByte());
        assertEquals(1, stream.getPosition());
    }

    @Test
    void shouldReadIntoByteArray() {
        byte[] bytes = new byte[]{11, 12, 13};
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes)));
        byte[] bytesRead = new byte[bytes.length];
        stream.readBytes(bytesRead);
        assertArrayEquals(bytes, bytesRead);
        assertEquals(3, stream.getPosition());
    }

    @Test
    void shouldReadIntoByteArrayAtOffsetUntilLength() {
        byte[] bytes = new byte[]{11, 12, 13};
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes)));
        byte[] bytesRead = new byte[bytes.length + 2];
        stream.readBytes(bytesRead, 1, 3);
        assertEquals(11, bytesRead[1]);
        assertEquals(12, bytesRead[2]);
        assertEquals(13, bytesRead[3]);
        assertEquals(3, stream.getPosition());
    }

    @Test
    void shouldReadLittleEndianInt32() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[]{4, 3, 2, 1})));
        assertEquals(16909060, stream.readInt32());
        assertEquals(4, stream.getPosition());
    }

    @Test
    void shouldReadLittleEndianInt64() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[]{8, 7, 6, 5, 4, 3, 2, 1})));
        assertEquals(72623859790382856L, stream.readInt64());
        assertEquals(8, stream.getPosition());
    }

    @Test
    void shouldReadDouble() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[]{8, 7, 6, 5, 4, 3, 2, 1})));
        assertEquals(Double.longBitsToDouble(72623859790382856L), stream.readDouble());
        assertEquals(8, stream.getPosition());
    }

    @Test
    void shouldReadObjectId() {
        byte[] objectIdAsByteArray = new byte[]{12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(objectIdAsByteArray)));
        assertEquals(new ObjectId(objectIdAsByteArray), stream.readObjectId());
        assertEquals(12, stream.getPosition());
    }

    @Test
    void shouldReadEmptyString() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 0, 0, 0, 0})));
        assertEquals("", stream.readString());
        assertEquals(5, stream.getPosition());
    }

    @ParameterizedTest
    @ValueSource(bytes = {0x0, 0x1, 0x20, 0x7e, 0x7f})
    void shouldReadOneByteString(final byte b) {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[]{2, 0, 0, 0, b, 0})));
        assertEquals(new String(new byte[]{b}, Charset.forName("UTF-8")), stream.readString());
        assertEquals(6, stream.getPosition());
    }

    @Test
    void shouldReadInvalidOneByteString() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[]{2, 0, 0, 0, (byte) 0xff, 0})));
        assertEquals("\uFFFD", stream.readString());
        assertEquals(6, stream.getPosition());
    }

    @Test
    void shouldReadAsciiString() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{5, 0, 0, 0, 0x4a, 0x61, 0x76, 0x61, 0})));
        assertEquals("Java", stream.readString());
        assertEquals(9, stream.getPosition());
    }

    @Test
    void shouldReadUtf8String() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{4, 0, 0, 0, (byte) 0xe0, (byte) 0xa4, (byte) 0x80, 0})));
        assertEquals("\u0900", stream.readString());
        assertEquals(8, stream.getPosition());
    }

    @Test
    void shouldReadEmptyCString() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[]{0})));
        assertEquals("", stream.readCString());
        assertEquals(1, stream.getPosition());
    }

    @ParameterizedTest
    @ValueSource(bytes = {0x1, 0x20, 0x7e, 0x7f})
    void shouldReadOneByteCString(final byte b) {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[]{b, 0})));
        assertEquals(new String(new byte[]{b}, Charset.forName("UTF-8")), stream.readCString());
        assertEquals(2, stream.getPosition());
    }

    @Test
    void shouldReadInvalidOneByteCString() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[]{(byte) 0xff, 0})));
        assertEquals("\uFFFD", stream.readCString());
        assertEquals(2, stream.getPosition());
    }

    @Test
    void shouldReadAsciiCString() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{0x4a, 0x61, 0x76, 0x61, 0})));
        assertEquals("Java", stream.readCString());
        assertEquals(5, stream.getPosition());
    }

    @Test
    void shouldReadUtf8CString() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{(byte) 0xe0, (byte) 0xa4, (byte) 0x80, 0})));
        assertEquals("\u0900", stream.readCString());
        assertEquals(4, stream.getPosition());
    }

    @Test
    void shouldHandleInvalidCStringNotNullTerminated() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{(byte) 0xe0, (byte) 0xa4, (byte) 0x80})));
        BsonSerializationException e = assertThrows(BsonSerializationException.class, stream::readCString);
        assertEquals("Found a BSON string that is not null-terminated", e.getMessage());
    }

    @Test
    void shouldHandleInvalidCStringNotNullTerminatedWhenSkipping() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{(byte) 0xe0, (byte) 0xa4, (byte) 0x80})));
        BsonSerializationException e = assertThrows(BsonSerializationException.class, stream::skipCString);
        assertEquals("Found a BSON string that is not null-terminated", e.getMessage());
    }

    @Test
    void shouldReadFromPosition() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[]{4, 3, 2, 1})));
        assertEquals(4, stream.readByte());
        assertEquals(3, stream.readByte());
        assertEquals(2, stream.readByte());
        assertEquals(1, stream.readByte());
    }

    @Test
    void shouldSkipCString() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{0x4a, 0x61, 0x76, 0x61, 0})));
        stream.skipCString();
        assertEquals(5, stream.getPosition());
    }

    @Test
    void shouldSkip() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{0x4a, 0x61, 0x76, 0x61, 0})));
        stream.skip(5);
        assertEquals(5, stream.getPosition());
    }

    @Test
    void shouldResetToTheBsonInputMark() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{0x4a, 0x61, 0x76, 0x61, 0})));
        stream.readByte();
        stream.readByte();
        BsonInputMark markOne = stream.getMark(1024);
        stream.readByte();
        stream.readByte();
        BsonInputMark markTwo = stream.getMark(1025);
        stream.readByte();

        markOne.reset();
        assertEquals(2, stream.getPosition());

        markTwo.reset();
        assertEquals(4, stream.getPosition());
    }

    @Test
    void shouldHaveRemainingWhenThereAreMoreBytes() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{0x4a, 0x61, 0x76, 0x61, 0})));
        assertTrue(stream.hasRemaining());
    }

    @Test
    void shouldNotHaveRemainingWhenThereAreNoMoreBytes() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[0])));
        assertFalse(stream.hasRemaining());
    }

    @Test
    void shouldCloseTheStream() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[0])));
        stream.close();
        assertThrows(IllegalStateException.class, stream::hasRemaining);
    }

    @Test
    void shouldThrowBsonSerializationExceptionReadingByteIfNoByteAvailable() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[0])));
        assertThrows(BsonSerializationException.class, stream::readByte);
    }

    @Test
    void shouldThrowBsonSerializationExceptionReadingInt32IfLessThan4BytesAvailable() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[]{0, 0, 0})));
        assertThrows(BsonSerializationException.class, stream::readInt32);
    }

    @Test
    void shouldThrowBsonSerializationExceptionReadingInt64IfLessThan8BytesAvailable() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[]{0, 0, 0, 0, 0, 0, 0})));
        assertThrows(BsonSerializationException.class, stream::readInt64);
    }

    @Test
    void shouldThrowBsonSerializationExceptionReadingDoubleIfLessThan8BytesAvailable() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[]{0, 0, 0, 0, 0, 0, 0})));
        assertThrows(BsonSerializationException.class, stream::readDouble);
    }

    @Test
    void shouldThrowBsonSerializationExceptionReadingObjectIdIfLessThan12BytesAvailable() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})));
        assertThrows(BsonSerializationException.class, stream::readObjectId);
    }

    @Test
    void shouldThrowBsonSerializationExceptionReadingBytesIfNotEnoughAvailable() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[]{0, 0, 0, 0, 0, 0, 0})));
        assertThrows(BsonSerializationException.class, () -> stream.readBytes(new byte[8]));
    }

    @Test
    void shouldThrowBsonSerializationExceptionReadingPartialBytesIfNotEnoughAvailable() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[]{0, 0, 0, 0})));
        assertThrows(BsonSerializationException.class, () -> stream.readBytes(new byte[8], 2, 5));
    }

    @Test
    void shouldThrowBsonSerializationExceptionIfBsonStringLengthNotPositive() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{-1, -1, -1, -1, 41, 42, 43, 0})));
        assertThrows(BsonSerializationException.class, stream::readString);
    }

    @Test
    void shouldThrowBsonSerializationExceptionIfBsonStringNotNullTerminated() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{4, 0, 0, 0, 41, 42, 43, 99})));
        assertThrows(BsonSerializationException.class, stream::readString);
    }

    @Test
    void shouldThrowBsonSerializationExceptionIfOneByteStringNotNullTerminated() {
        ByteBufferBsonInput stream = new ByteBufferBsonInput(
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{2, 0, 0, 0, 1, 3})));
        assertThrows(BsonSerializationException.class, stream::readString);
    }
}
