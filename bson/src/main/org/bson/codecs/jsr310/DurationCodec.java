/*
 * Copyright 2008-present MongoDB, Inc.
 * Copyright 2018 Cezary Bartosiak
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

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.time.Duration;

import static java.lang.String.format;
import static org.bson.codecs.jsr310.Jsr310DecoderHelper.validateIsDecimal128;

/**
 * Duration Codec.
 *
 * <p>
 * Encodes and decodes {@code Duration} objects to and from {@code Decimal128}. Stored in {@code %d.%09d} format, where the first part
 * represents seconds and the decimal represents nanoseconds.
 * </p>
 * <p>Requires Java 8 or greater.</p>
 *
 * @mongodb.driver.manual reference/bson-types
 * @since 3.7
 */
public class DurationCodec implements Codec<Duration> {

    @Override
    public Duration decode(final BsonReader reader, final DecoderContext decoderContext) {
        validateIsDecimal128(reader, Duration.class);
        BigDecimal value = reader.readDecimal128().bigDecimalValue();
        long seconds = value.longValue();
        return Duration.ofSeconds(seconds, value.subtract(new BigDecimal(seconds)).scaleByPowerOfTen(9).abs().intValue());
    }

    @Override
    public void encode(final BsonWriter writer, final Duration value, final EncoderContext encoderContext) {
        writer.writeDecimal128(Decimal128.parse(format("%d.%09d", value.getSeconds(), value.getNano())));
    }

    @Override
    public Class<Duration> getEncoderClass() {
        return Duration.class;
    }
}
