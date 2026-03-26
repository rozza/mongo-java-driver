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

package org.bson.codecs;

import org.bson.BsonType;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class IterableCodecProviderTest {

    @Test
    void shouldProvideCodecForIterables() {
        IterableCodecProvider provider = new IterableCodecProvider();
        CodecRegistry registry = fromProviders(provider, new BsonValueCodecProvider(),
                new ValueCodecProvider(), new DocumentCodecProvider());

        assertInstanceOf(IterableCodec.class, provider.get(Iterable.class, registry));
        assertInstanceOf(IterableCodec.class, provider.get(List.class, registry));
        assertInstanceOf(IterableCodec.class, provider.get(ArrayList.class, registry));
    }

    @Test
    void shouldNotProvideCodecForNonIterables() {
        IterableCodecProvider provider = new IterableCodecProvider();
        CodecRegistry registry = fromProviders(provider, new BsonValueCodecProvider(),
                new ValueCodecProvider(), new DocumentCodecProvider());

        assertNull(provider.get(Integer.class, registry));
    }

    @Test
    void identicalInstancesShouldBeEqualAndHaveSameHashCode() {
        IterableCodecProvider first = new IterableCodecProvider();
        IterableCodecProvider second = new IterableCodecProvider();

        assertEquals(first, first);
        assertEquals(first, second);
        assertEquals(first.hashCode(), first.hashCode());
        assertEquals(first.hashCode(), second.hashCode());
    }

    @Test
    void unidenticalInstancesShouldNotBeEqual() {
        IterableCodecProvider first = new IterableCodecProvider();
        Map<BsonType, Class<?>> replacements = new HashMap<>();
        replacements.put(BsonType.BOOLEAN, String.class);
        IterableCodecProvider second = new IterableCodecProvider(new BsonTypeClassMap(replacements));
        IterableCodecProvider third = new IterableCodecProvider(new BsonTypeClassMap(), from -> from);

        assertNotEquals(first, Map.class);
        assertNotEquals(first, second);
        assertNotEquals(first, third);
        assertNotEquals(second, third);
    }
}
