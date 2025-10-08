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

package com.mongodb.internal.connection.tlschannel;

import java.nio.ByteBuffer;

/**
 * A factory for {@link ByteBuffer}s. Implementations are free to return heap or direct buffers, or
 * to do any kind of pooling. They are also expected to be thread-safe.
 */
public interface BufferAllocator {

  /**
   * Allocate a {@link ByteBuffer} with the given initial capacity.
   *
   * @param size the size to allocate
   * @return the newly created buffer
   */
  ByteBuffer allocate(int size);

  /**
   * Deallocate the given {@link ByteBuffer}.
   *
   * @param buffer the buffer to deallocate, that should have been allocated using the same {@link
   *     BufferAllocator} instance
   */
  void free(ByteBuffer buffer);
}
