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

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.time.ZoneId;

import static org.bson.codecs.jsr310.Jsr310DecoderHelper.validateIsString;

/**
 * ZoneId Codec.
 *
 * <p>
 * Encodes and decodes {@code ZoneId} objects to and from a time-zone ID {@code String}.
 * </p>
 * <p>Note: Requires Java 8 or greater.</p>
 *
 * @mongodb.driver.manual reference/bson-types
 * @see ZoneId#getId()
 * @since 3.7
 */
public class ZoneIdCodec implements Codec<ZoneId> {

    @Override
    public ZoneId decode(final BsonReader reader, final DecoderContext decoderContext) {
        validateIsString(reader, ZoneId.class);
        return ZoneId.of(reader.readString());
    }

    @Override
    public void encode(final BsonWriter writer, final ZoneId value, final EncoderContext encoderContext) {
        writer.writeString(value.getId());
    }

    @Override
    public Class<ZoneId> getEncoderClass() {
        return ZoneId.class;
    }
}
