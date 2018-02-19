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

import java.time.ZonedDateTime;

import static org.bson.codecs.jsr310.Jsr310DecoderHelper.validateIsString;

/**
 * ZonedDateTime Codec.
 *
 * <p>
 * Encodes and decodes {@code ZonedDateTime} objects to and from {@code String}.  The string format consists of the {@code LocalDateTime}
 * followed by the {@code ZoneOffset} and is compatible with ISO-8601 if both the offset and ID are the same.
 * </p>
 * <p>Note: Requires Java 8 or greater.</p>
 *
 * @mongodb.driver.manual reference/bson-types
 * @see ZonedDateTime#toString()
 * @since 3.7
 */
public class ZonedDateTimeCodec implements Codec<ZonedDateTime> {

    @Override
    public ZonedDateTime decode(final BsonReader reader, final DecoderContext decoderContext) {
        validateIsString(reader, ZonedDateTime.class);
        return ZonedDateTime.parse(reader.readString());
    }

    @Override
    public void encode(final BsonWriter writer, final ZonedDateTime value, final EncoderContext encoderContext) {
        writer.writeString(value.toString());
    }

    @Override
    public Class<ZonedDateTime> getEncoderClass() {
        return ZonedDateTime.class;
    }
}
