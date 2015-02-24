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

package org.bson.codecs.configuration;

import org.bson.codecs.Codec;

import static org.bson.assertions.Assertions.notNull;

final class PreferredCodecRegistry implements CodecRegistry {
    private final CodecRegistry preferredCodecRegistry;
    private final CodecRegistry alternativeCodecRegistry;

    PreferredCodecRegistry(final CodecRegistry preferredCodecRegistry, final CodecRegistry alternativeCodecRegistry) {
        this.preferredCodecRegistry = notNull("preferredCodecRegistry", preferredCodecRegistry);
        this.alternativeCodecRegistry = notNull("alternativeCodecRegistry", alternativeCodecRegistry);
    }

    @Override
    public <T> Codec<T> get(final Class<T> clazz) {

        Codec<T> codec = preferredCodecRegistry.get(clazz);
        if (codec == null) {
            codec = alternativeCodecRegistry.get(clazz);
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

        PreferredCodecRegistry that = (PreferredCodecRegistry) o;

        if (!alternativeCodecRegistry.equals(that.alternativeCodecRegistry)) {
            return false;
        } else if (!preferredCodecRegistry.equals(that.preferredCodecRegistry)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = preferredCodecRegistry.hashCode();
        result = 31 * result + alternativeCodecRegistry.hashCode();
        return result;
    }
}
