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

package org.bson.codecs.jsr310;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class Jsr310CodecProviderTest {

    private static final CodecRegistry EMPTY_REGISTRY = new CodecRegistry() {
        @Override
        public <T> Codec<T> get(final Class<T> clazz) {
            throw new CodecConfigurationException("No codec for " + clazz);
        }

        @Override
        public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
            throw new CodecConfigurationException("No codec for " + clazz);
        }
    };

    private static Stream<Class<?>> jsr310Classes() {
        return Stream.of(
                java.time.Instant.class,
                java.time.LocalDate.class,
                java.time.LocalDateTime.class,
                java.time.LocalTime.class
        );
    }

    @ParameterizedTest
    @MethodSource("jsr310Classes")
    void shouldProvideCodecForAllJsr310Classes(final Class<?> clazz) {
        Jsr310CodecProvider provider = new Jsr310CodecProvider();
        assertNotNull(provider.get(clazz, EMPTY_REGISTRY));
    }
}
