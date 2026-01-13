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

package com.mongodb.internal;

import org.bson.ByteBuf;

/**
 * Utility methods for resource management, particularly {@link ByteBuf} instances.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class ResourceUtil {
    /**
     * Releases each buffer in the given collection by decrementing its reference count by 1.
     *
     * <p><strong>Reference Counting:</strong> This method calls {@link ByteBuf#release()} once on
     * each non-null buffer, which decrements its reference count by 1. It does NOT drop all references
     * (i.e., does not call {@code release(refCnt())}). Buffers may remain alive after this call if
     * other references exist elsewhere.</p>
     *
     * <p><strong>Ownership:</strong> Callers must ensure they own the buffers being released. This
     * typically means the buffers were either:</p>
     * <ul>
     *   <li>Created by the caller (initial reference count of 1), or</li>
     *   <li>Explicitly retained by the caller (reference count incremented), or</li>
     *   <li>Returned by a method that transfers ownership (e.g., {@link org.bson.io.OutputBuffer#getByteBuffers()})</li>
     * </ul>
     *
     * <p>This method assumes {@link ByteBuf#release()} does not throw exceptions.</p>
     *
     * @param buffers the collection of buffers to release; null buffers are skipped
     */
    public static void release(final Iterable<? extends ByteBuf> buffers) {
        // we assume `ByteBuf::release` does not complete abruptly
        buffers.forEach(buffer -> {
            if (buffer != null) {
                buffer.release();
            }
        });
    }

    private ResourceUtil() {
    }
}
