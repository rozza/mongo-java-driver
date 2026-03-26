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

package com.mongodb;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class MongoCompressorTest {

    @Test
    @DisplayName("should create zlib compressor")
    void shouldCreateZlibCompressor() {
        MongoCompressor compressor = MongoCompressor.createZlibCompressor();

        assertEquals("zlib", compressor.getName());
        assertEquals(-1, (int) compressor.getProperty(MongoCompressor.LEVEL, -1));
    }

    @Test
    @DisplayName("should create zstd compressor")
    void shouldCreateZstdCompressor() {
        MongoCompressor compressor = MongoCompressor.createZstdCompressor();

        assertEquals("zstd", compressor.getName());
        assertEquals(-1, (int) compressor.getProperty(MongoCompressor.LEVEL, -1));
    }

    @Test
    @DisplayName("should set property")
    void shouldSetProperty() {
        MongoCompressor compressor = MongoCompressor.createZlibCompressor();
        MongoCompressor newCompressor = compressor.withProperty(MongoCompressor.LEVEL, 5);

        assertNotEquals(compressor, newCompressor);
        assertEquals(-1, (int) compressor.getProperty(MongoCompressor.LEVEL, -1));
        assertEquals(5, (int) compressor.getProperty(MongoCompressor.LEVEL, 5));
    }
}
