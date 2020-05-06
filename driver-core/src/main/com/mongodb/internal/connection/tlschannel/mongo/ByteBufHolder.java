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
 *
 * Original Work: MIT License, Copyright (c) [2015-2020] all contributors
 * https://github.com/marianobarrios/tls-channel
 */

package com.mongodb.internal.connection.tlschannel.mongo;

import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.connection.tlschannel.impl.TlsChannelImpl;
import org.bson.ByteBuf;

import java.nio.ByteBuffer;
import java.util.Optional;

import static java.lang.String.format;

public class ByteBufHolder {

  private static final Logger logger = Loggers.getLogger("connection.tls");
  // Round to next highest power of two to account for PowerOfTwoBufferPool allocation style
  private static final byte[] zeros = new byte[roundUpToNextHighestPowerOfTwo(TlsChannelImpl.maxTlsPacketSize)];

  public final String name;
  public final ByteBufAllocator allocator;
  public final boolean plainData;
  public final int maxSize;
  public final boolean opportunisticDispose;

  public ByteBuf byteBuf;
  public ByteBuffer buffer;

  public int lastSize;

  public ByteBufHolder(
      String name,
      Optional<ByteBuf> byteBuf,
      ByteBufAllocator allocator,
      int initialSize,
      int maxSize,
      boolean plainData,
      boolean opportunisticDispose) {
    this.name = name;
    this.allocator = allocator;
    this.byteBuf = byteBuf.orElse(null);
    this.maxSize = maxSize;
    this.plainData = plainData;
    this.opportunisticDispose = opportunisticDispose;
    this.lastSize = byteBuf.map(ByteBuf::capacity).orElse(initialSize);
  }

  public void prepare() {
    if (buffer == null) {
      byteBuf = allocator.allocate(lastSize);
      buffer = byteBuf.asNIO();
    }
  }

  public boolean release() {
    if (opportunisticDispose && buffer.position() == 0) {
      return dispose();
    } else {
      return false;
    }
  }

  public boolean dispose() {
    if (buffer != null) {
      allocator.free(byteBuf);
      buffer = null;
      return true;
    } else {
      return false;
    }
  }

  public void resize(int newCapacity) {
    if (newCapacity > maxSize)
      throw new IllegalArgumentException(
          format(
              "new capacity (%s) bigger than absolute max size (%s)", newCapacity, maxSize));
    if (logger.isTraceEnabled()) {
      logger.trace(format(
              "resizing buffer %s, increasing from %s to %s (manual sizing)",
              name,
              buffer.capacity(),
              newCapacity));
    }
    resizeImpl(newCapacity);
  }

  public void enlarge() {
    if (buffer.capacity() >= maxSize) {
      throw new IllegalStateException(
          format(
              "%s buffer insufficient despite having capacity of %d", name, buffer.capacity()));
    }
    int newCapacity = Math.min(buffer.capacity() * 2, maxSize);
    if (logger.isTraceEnabled()) {
      logger.trace(format(
              "enlarging buffer %s, increasing from %s to %s (automatic enlarge)",
              name,
              buffer.capacity(),
              newCapacity));
    }
    resizeImpl(newCapacity);
  }

  private void resizeImpl(int newCapacity) {
    ByteBuf newByteBuf = allocator.allocate(newCapacity);
    ByteBuffer newBuffer = newByteBuf.asNIO();
    buffer.flip();
    newBuffer.put(buffer);
    if (plainData) {
      zero();
    }
    allocator.free(byteBuf);
    byteBuf = newByteBuf;
    buffer = newBuffer;
    lastSize = newCapacity;
  }

  /**
   * Fill with zeros the remaining of the supplied buffer. This method does not change the buffer
   * position.
   *
   * <p>Typically used for security reasons, with buffers that contains now-unused plaintext.
   */
  public void zeroRemaining() {
    buffer.mark();
    buffer.put(zeros, 0, buffer.remaining());
    buffer.reset();
  }

  /**
   * Fill the buffer with zeros. This method does not change the buffer position.
   *
   * <p>Typically used for security reasons, with buffers that contains now-unused plaintext.
   */
  public void zero() {
    buffer.mark();
    buffer.position(0);
    buffer.put(zeros, 0, buffer.remaining());
    buffer.reset();
  }

  public boolean nullOrEmpty() {
    return buffer == null || buffer.position() == 0;
  }

  private static int roundUpToNextHighestPowerOfTwo(final int size) {
    int v = size;
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;
    return v;
  }
}
