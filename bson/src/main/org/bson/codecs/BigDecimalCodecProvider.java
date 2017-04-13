/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs;

import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

/**
 * A {@code BigDecimal} codec provider
 *
 * @since 3.5
 */
public final class BigDecimalCodecProvider implements CodecProvider {
    private static final BigDecimalCodec BIG_DECIMAL_CODEC = new BigDecimalCodec();

    /**
     * Constructs a new instance
     */
    public BigDecimalCodecProvider() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (clazz.equals(BIG_DECIMAL_CODEC.getEncoderClass())) {
            return (Codec<T>) BIG_DECIMAL_CODEC;
        }
        return null;
    }

    @Override
    public int hashCode() {
        return 1;
    }

    @Override
    public boolean equals(final Object obj) {
        return obj instanceof BigDecimalCodecProvider;
    }
}
