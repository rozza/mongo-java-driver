/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.codecs;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;

import static java.lang.String.format;
import static org.bson.assertions.Assertions.notNull;

/**
 * A root codec registry that throws an exception for missing codecs.
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public final class RootCodecRegistry implements CodecRegistry {
    private final CodecRegistry codecRegistry;

    /**
     * A static helper to create a codec registry that throws a {@link CodecConfigurationException} if no codec is found when calling
     * {@link CodecRegistry#get} on the underlying codecRegistry.
     *
     * @param codecRegistry the codec registry to make the root registry
     * @return a codec registry that throws an exception for missing codecs.
     */
    public static RootCodecRegistry createRootRegistry(final CodecRegistry codecRegistry) {
        if (codecRegistry instanceof RootCodecRegistry) {
            return (RootCodecRegistry) codecRegistry;
        }
        return new RootCodecRegistry(codecRegistry);
    }

    /**
     * Get the underlying codec registry
     *
     * @return the underlying codec registry
     */
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    @Override
    public <T> Codec<T> get(final Class<T> clazz) {
        Codec<T> codec = codecRegistry.get(clazz);
        if (codec == null) {
            throw new CodecConfigurationException(format("Can't find a codec for %s.", clazz));
        }
        return codec;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RootCodecRegistry that = (RootCodecRegistry) o;

        if (!codecRegistry.equals(that.codecRegistry)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return codecRegistry.hashCode();
    }

    private RootCodecRegistry(final CodecRegistry codecRegistry) {
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
    }
}
