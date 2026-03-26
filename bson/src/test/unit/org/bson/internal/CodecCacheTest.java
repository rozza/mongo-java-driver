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

package org.bson.internal;

import org.bson.codecs.MinKeyCodec;
import org.bson.types.MinKey;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CodecCacheTest {

    @Test
    void shouldReturnCachedCodecIfCodecForClassExists() {
        MinKeyCodec codec = new MinKeyCodec();
        CodecCache cache = new CodecCache();
        CodecCache.CodecCacheKey cacheKey = new CodecCache.CodecCacheKey(MinKey.class, null);
        cache.putIfAbsent(cacheKey, codec);
        assertTrue(cache.get(cacheKey).isPresent());
        assertSame(codec, cache.get(cacheKey).get());
    }

    @Test
    void shouldReturnEmptyIfCodecForClassDoesNotExist() {
        CodecCache cache = new CodecCache();
        CodecCache.CodecCacheKey cacheKey = new CodecCache.CodecCacheKey(MinKey.class, null);
        assertFalse(cache.get(cacheKey).isPresent());
    }

    @Test
    void shouldReturnCachedCodecIfCodecForParameterizedClassExists() {
        MinKeyCodec codec = new MinKeyCodec();
        CodecCache cache = new CodecCache();
        CodecCache.CodecCacheKey cacheKey = new CodecCache.CodecCacheKey(java.util.List.class, Arrays.asList(Integer.class));
        cache.putIfAbsent(cacheKey, codec);
        assertTrue(cache.get(cacheKey).isPresent());
        assertSame(codec, cache.get(cacheKey).get());
    }

    @Test
    void shouldReturnEmptyIfCodecForParameterizedClassDoesNotExist() {
        CodecCache cache = new CodecCache();
        CodecCache.CodecCacheKey cacheKey = new CodecCache.CodecCacheKey(java.util.List.class, Arrays.asList(Integer.class));
        assertFalse(cache.get(cacheKey).isPresent());
    }
}
