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
import org.bson.codecs.configuration.CodecConfigurationException;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

import static java.lang.String.format;
import static org.bson.codecs.jsr310.Jsr310DecoderHelper.validateIsDateTime;

/**
 * LocalDate Codec.
 *
 * <p>
 * Encodes and decodes {@code LocalDate} objects to and from {@code DateTime}.
 * </p>
 * <p>Note: Requires Java 8 or greater.</p>
 *
 * @mongodb.driver.manual reference/bson-types
 * @since 3.7
 */
public class LocalDateCodec implements Codec<LocalDate> {

    @Override
    public LocalDate decode(final BsonReader reader, final DecoderContext decoderContext) {
        validateIsDateTime(reader, Instant.class);
        return Instant.ofEpochMilli(reader.readDateTime()).atZone(ZoneOffset.UTC).toLocalDate();
    }

    /**
     * {@inheritDoc}
     * @throws CodecConfigurationException if the LocalDate cannot be converted to a valid Bson DateTime.
     */
    @Override
    public void encode(final BsonWriter writer, final LocalDate value, final EncoderContext encoderContext) {
        try {
            writer.writeDateTime(value.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
        } catch (ArithmeticException e) {
            throw new CodecConfigurationException(format("Unsupported LocalDate '%s' could not be converted to milliseconds: %s",
                    value, e.getMessage()), e);
        }
    }

    @Override
    public Class<LocalDate> getEncoderClass() {
        return LocalDate.class;
    }
}
