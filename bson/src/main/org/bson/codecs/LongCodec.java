/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import org.bson.BsonReader;
import org.bson.BsonWriter;

import static org.bson.assertions.Assertions.isTrueArgument;
import static org.bson.codecs.NumberCodecHelper.DEFAULT_DELTA;
import static org.bson.codecs.NumberCodecHelper.decodeNumber;

/**
 * Encodes and decodes {@code Long} objects.
 *
 * @since 3.0
 */

public class LongCodec implements Codec<Long> {
    private final double delta;

    /**
     * Construct a new instance
     */
    public LongCodec() {
        this(DEFAULT_DELTA);
    }

    /**
     * Construct a new instance
     *
     * @param delta the maximum delta between {@code expected} and {@code actual} for which both numbers are still
     * considered equal. Required when converting Double values to {@code Long} values. Defaults to {@code 0.00000000000001d}.
     *
     * @since 3.5
     */
    public LongCodec(final double delta) {
        isTrueArgument("The delta must be greater than or equal to zero and less than one", delta >= 0 && delta < 1);
        this.delta = delta;
    }

    @Override
    public void encode(final BsonWriter writer, final Long value, final EncoderContext encoderContext) {
        writer.writeInt64(value);
    }

    @Override
    public Long decode(final BsonReader reader, final DecoderContext decoderContext) {
        return decodeNumber(reader, Long.class, delta);
    }

    @Override
    public Class<Long> getEncoderClass() {
        return Long.class;
    }
}
