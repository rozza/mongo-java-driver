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
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BasicOutputBufferUnitTest {

    private byte[] getBytes(final BasicOutputBuffer basicOutputBuffer) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(basicOutputBuffer.getSize());
        basicOutputBuffer.pipe(baos);
        return baos.toByteArray();
    }

    @Test
    void positionAndSizeShouldBeZeroAfterConstructor() {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        assertEquals(0, bsonOutput.getPosition());
        assertEquals(0, bsonOutput.getSize());
    }

    @Test
    void shouldWriteAByte() throws IOException {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        bsonOutput.writeByte(11);
        assertArrayEquals(new byte[]{11}, getBytes(bsonOutput));
        assertEquals(1, bsonOutput.getPosition());
        assertEquals(1, bsonOutput.getSize());
    }

    @Test
    void writeBytesShorthandShouldExtendBuffer() throws IOException {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer(3);
        bsonOutput.write(new byte[]{1, 2, 3, 4});
        assertArrayEquals(new byte[]{1, 2, 3, 4}, getBytes(bsonOutput));
        assertEquals(4, bsonOutput.getPosition());
        assertEquals(4, bsonOutput.getSize());
    }

    @Test
    void shouldWriteBytes() throws IOException {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer(3);
        bsonOutput.writeBytes(new byte[]{1, 2, 3, 4});
        assertArrayEquals(new byte[]{1, 2, 3, 4}, getBytes(bsonOutput));
        assertEquals(4, bsonOutput.getPosition());
        assertEquals(4, bsonOutput.getSize());
    }

    @Test
    void shouldWriteBytesFromOffsetUntilLength() throws IOException {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer(5);
        bsonOutput.writeBytes(new byte[]{0, 1, 2, 3, 4, 5}, 1, 4);
        assertArrayEquals(new byte[]{1, 2, 3, 4}, getBytes(bsonOutput));
        assertEquals(4, bsonOutput.getPosition());
        assertEquals(4, bsonOutput.getSize());
    }

    @Test
    void toByteArrayShouldBeIdempotent() throws IOException {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer(10);
        bsonOutput.writeBytes(new byte[]{1, 2, 3, 4});
        byte[] first = bsonOutput.toByteArray();
        byte[] second = bsonOutput.toByteArray();
        assertArrayEquals(new byte[]{1, 2, 3, 4}, getBytes(bsonOutput));
        assertArrayEquals(new byte[]{1, 2, 3, 4}, first);
        assertArrayEquals(new byte[]{1, 2, 3, 4}, second);
        assertEquals(4, bsonOutput.getPosition());
        assertEquals(4, bsonOutput.getSize());
    }

    @Test
    void toByteArrayCreatesACopy() {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer(10);
        bsonOutput.writeBytes(new byte[]{1, 2, 3, 4});
        byte[] first = bsonOutput.toByteArray();
        byte[] second = bsonOutput.toByteArray();
        assertNotSame(first, second);
        assertArrayEquals(new byte[]{1, 2, 3, 4}, first);
        assertArrayEquals(new byte[]{1, 2, 3, 4}, second);
    }

    @Test
    void shouldWriteLittleEndianInt32() throws IOException {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer(3);
        bsonOutput.writeInt32(0x1020304);
        assertArrayEquals(new byte[]{4, 3, 2, 1}, getBytes(bsonOutput));
        assertEquals(4, bsonOutput.getPosition());
        assertEquals(4, bsonOutput.getSize());
    }

    @Test
    void shouldWriteLittleEndianInt64() throws IOException {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer(7);
        bsonOutput.writeInt64(0x102030405060708L);
        assertArrayEquals(new byte[]{8, 7, 6, 5, 4, 3, 2, 1}, getBytes(bsonOutput));
        assertEquals(8, bsonOutput.getPosition());
        assertEquals(8, bsonOutput.getSize());
    }

    @Test
    void shouldWriteDouble() throws IOException {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer(7);
        bsonOutput.writeDouble(Double.longBitsToDouble(0x102030405060708L));
        assertArrayEquals(new byte[]{8, 7, 6, 5, 4, 3, 2, 1}, getBytes(bsonOutput));
        assertEquals(8, bsonOutput.getPosition());
        assertEquals(8, bsonOutput.getSize());
    }

    @Test
    void shouldWriteObjectId() throws IOException {
        byte[] objectIdAsByteArray = new byte[]{12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer(11);
        bsonOutput.writeObjectId(new ObjectId(objectIdAsByteArray));
        assertArrayEquals(objectIdAsByteArray, getBytes(bsonOutput));
        assertEquals(12, bsonOutput.getPosition());
        assertEquals(12, bsonOutput.getSize());
    }

    @Test
    void writeObjectIdShouldThrowAfterClose() {
        byte[] objectIdAsByteArray = new byte[]{12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        bsonOutput.close();
        assertThrows(IllegalStateException.class, () -> bsonOutput.writeObjectId(new ObjectId(objectIdAsByteArray)));
    }

    @Test
    void shouldWriteEmptyString() throws IOException {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        bsonOutput.writeString("");
        assertArrayEquals(new byte[]{1, 0, 0, 0, 0}, getBytes(bsonOutput));
        assertEquals(5, bsonOutput.getPosition());
        assertEquals(5, bsonOutput.getSize());
    }

    @Test
    void shouldWriteAsciiString() throws IOException {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        bsonOutput.writeString("Java");
        assertArrayEquals(new byte[]{5, 0, 0, 0, 0x4a, 0x61, 0x76, 0x61, 0}, getBytes(bsonOutput));
        assertEquals(9, bsonOutput.getPosition());
        assertEquals(9, bsonOutput.getSize());
    }

    @Test
    void shouldWriteUtf8String() throws IOException {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer(7);
        bsonOutput.writeString("\u0900");
        assertArrayEquals(new byte[]{4, 0, 0, 0, (byte) 0xe0, (byte) 0xa4, (byte) 0x80, 0}, getBytes(bsonOutput));
        assertEquals(8, bsonOutput.getPosition());
        assertEquals(8, bsonOutput.getSize());
    }

    @Test
    void shouldWriteEmptyCString() throws IOException {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        bsonOutput.writeCString("");
        assertArrayEquals(new byte[]{0}, getBytes(bsonOutput));
        assertEquals(1, bsonOutput.getPosition());
        assertEquals(1, bsonOutput.getSize());
    }

    @Test
    void shouldWriteAsciiCString() throws IOException {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        bsonOutput.writeCString("Java");
        assertArrayEquals(new byte[]{0x4a, 0x61, 0x76, 0x61, 0}, getBytes(bsonOutput));
        assertEquals(5, bsonOutput.getPosition());
        assertEquals(5, bsonOutput.getSize());
    }

    @Test
    void shouldWriteUtf8CString() throws IOException {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        bsonOutput.writeCString("\u0900");
        assertArrayEquals(new byte[]{(byte) 0xe0, (byte) 0xa4, (byte) 0x80, 0}, getBytes(bsonOutput));
        assertEquals(4, bsonOutput.getPosition());
        assertEquals(4, bsonOutput.getSize());
    }

    @Test
    void nullCharacterInCStringShouldThrowSerializationException() {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        assertThrows(BsonSerializationException.class, () -> bsonOutput.writeCString("hell\u0000world"));
    }

    @Test
    void nullCharacterInStringShouldNotThrowSerializationException() throws IOException {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        bsonOutput.writeString("h\u0000i");
        assertArrayEquals(new byte[]{4, 0, 0, 0, (byte) 'h', 0, (byte) 'i', 0}, getBytes(bsonOutput));
    }

    @Test
    void writeInt32AtPositionShouldThrowWithInvalidPosition() {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        bsonOutput.writeBytes(new byte[]{1, 2, 3, 4});
        assertThrows(IllegalArgumentException.class, () -> bsonOutput.writeInt32(-1, 0x1020304));
        assertThrows(IllegalArgumentException.class, () -> bsonOutput.writeInt32(1, 0x1020304));
    }

    @Test
    void shouldWriteInt32AtPosition() throws IOException {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        bsonOutput.writeBytes(new byte[]{0, 0, 0, 0, 1, 2, 3, 4});
        bsonOutput.writeInt32(0, 0x1020304);
        assertArrayEquals(new byte[]{4, 3, 2, 1, 1, 2, 3, 4}, getBytes(bsonOutput));
        assertEquals(8, bsonOutput.getPosition());
        assertEquals(8, bsonOutput.getSize());

        bsonOutput.writeInt32(4, 0x1020304);
        assertArrayEquals(new byte[]{4, 3, 2, 1, 4, 3, 2, 1}, getBytes(bsonOutput));
        assertEquals(8, bsonOutput.getPosition());
        assertEquals(8, bsonOutput.getSize());
    }

    @Test
    void absoluteWriteShouldThrowWithInvalidPosition() {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        bsonOutput.writeBytes(new byte[]{1, 2, 3, 4});
        assertThrows(IllegalArgumentException.class, () -> bsonOutput.write(-1, 0x1020304));
        assertThrows(IllegalArgumentException.class, () -> bsonOutput.write(4, 0x1020304));
    }

    @Test
    void absoluteWriteShouldWriteLowerByteAtPosition() throws IOException {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        bsonOutput.writeBytes(new byte[]{0, 0, 0, 0, 1, 2, 3, 4});
        bsonOutput.write(0, 0x1020304);
        assertArrayEquals(new byte[]{4, 0, 0, 0, 1, 2, 3, 4}, getBytes(bsonOutput));
        assertEquals(8, bsonOutput.getPosition());
        assertEquals(8, bsonOutput.getSize());

        bsonOutput.write(7, 0x1020304);
        assertArrayEquals(new byte[]{4, 0, 0, 0, 1, 2, 3, 4}, getBytes(bsonOutput));
        assertEquals(8, bsonOutput.getPosition());
        assertEquals(8, bsonOutput.getSize());
    }

    @Test
    void truncateShouldThrowWithInvalidPosition() {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        bsonOutput.writeBytes(new byte[]{1, 2, 3, 4});
        assertThrows(IllegalArgumentException.class, () -> bsonOutput.truncateToPosition(5));
        assertThrows(IllegalArgumentException.class, () -> bsonOutput.truncateToPosition(-1));
    }

    @Test
    void shouldTruncateToPosition() throws IOException {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        bsonOutput.writeBytes(new byte[]{1, 2, 3, 4});
        bsonOutput.truncateToPosition(2);
        assertArrayEquals(new byte[]{1, 2}, getBytes(bsonOutput));
        assertEquals(2, bsonOutput.getPosition());
        assertEquals(2, bsonOutput.getSize());
    }

    @Test
    void shouldGrow() throws IOException {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer(4);
        bsonOutput.writeBytes(new byte[]{1, 2, 3, 4});
        bsonOutput.writeBytes(new byte[]{5, 6, 7, 8, 9, 10});
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, getBytes(bsonOutput));
        assertEquals(10, bsonOutput.getPosition());
        assertEquals(10, bsonOutput.getSize());
    }

    @Test
    void shouldGetByteBufferAsLittleEndian() {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer(4);
        bsonOutput.writeBytes(new byte[]{1, 0, 0, 0});
        assertEquals(1, bsonOutput.getByteBuffers().get(0).getInt());
    }

    @Test
    void shouldGetByteBufferWithLimit() {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer(8);
        bsonOutput.writeBytes(new byte[]{1, 0, 0, 0});
        assertEquals(1, bsonOutput.getByteBuffers().size());
        assertEquals(0, bsonOutput.getByteBuffers().get(0).position());
        assertEquals(4, bsonOutput.getByteBuffers().get(0).limit());
    }

    @Test
    void shouldGetInternalBuffer() {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer(4);
        bsonOutput.writeBytes(new byte[]{1, 2});
        assertArrayEquals(new byte[]{1, 2, 0, 0}, bsonOutput.getInternalBuffer());
    }

    @Test
    void shouldClose() {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        bsonOutput.close();
        assertThrows(IllegalStateException.class, () -> bsonOutput.writeByte(11));
    }
}
