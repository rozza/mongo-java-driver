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

import org.bson.codecs.DateCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.BSONTimestamp;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DBObjectCodecProviderTest {
    private final DBObjectCodecProvider provider = new DBObjectCodecProvider();
    private final CodecRegistry registry = CodecRegistries.fromProviders(provider);

    @Test
    @DisplayName("should provide codec for BSONTimestamp")
    void shouldProvideCodecForBSONTimestamp() {
        assertEquals(BSONTimestampCodec.class, provider.get(BSONTimestamp.class, registry).getClass());
    }

    @Test
    @DisplayName("should provide codec for Date")
    void shouldProvideCodecForDate() {
        assertEquals(DateCodec.class, provider.get(java.util.Date.class, registry).getClass());
    }

    @Test
    @DisplayName("should provide codec for class assignable to DBObject")
    void shouldProvideCodecForDBObject() {
        assertEquals(DBObjectCodec.class, provider.get(BasicDBObject.class, registry).getClass());
    }

    @Test
    @DisplayName("should not provide codec for class assignable to DBObject that is also assignable to List")
    void shouldNotProvideCodecForDBObjectList() {
        assertNull(provider.get(BasicDBList.class, registry));
    }

    @Test
    @DisplayName("should not provide codec for unexpected class")
    void shouldNotProvideCodecForUnexpectedClass() {
        assertNull(provider.get(Integer.class, registry));
    }
}
