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
import java.time.MonthDay;

import static java.lang.String.format;
import static org.bson.codecs.jsr310.Jsr310DecoderHelper.validateIsDecimal128;

/**
 * MonthDay Codec.
 *
 * <p>
 * Encodes and decodes {@code MonthDay} objects to and from {@code Decimal128}. Stored in {@code %d.%02d} format, where the first part
 * represents the month and the decimal represents the day.
 * </p>
 * <p>Note: Requires Java 8 or greater.</p>
 *
 * @mongodb.driver.manual reference/bson-types
 * @since 3.7
 */
public class MonthDayCodec implements Codec<MonthDay> {

    @Override
    public MonthDay decode(final BsonReader reader, final DecoderContext decoderContext) {
        validateIsDecimal128(reader, MonthDay.class);
        BigDecimal value = reader.readDecimal128().bigDecimalValue();
        int month = value.intValue();
        int day = value.subtract(new BigDecimal(month)).scaleByPowerOfTen(2).abs().intValue();
        return MonthDay.of(month, day);
    }

    @Override
    public void encode(final BsonWriter writer, final MonthDay value, final EncoderContext encoderContext) {
        writer.writeDecimal128(Decimal128.parse(format("%d.%02d", value.getMonthValue(), value.getDayOfMonth())));
    }

    @Override
    public Class<MonthDay> getEncoderClass() {
        return MonthDay.class;
    }
}
