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
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.time.Month;

import static org.bson.codecs.jsr310.Jsr310DecoderHelper.validateIsNumeric;
import static org.bson.internal.NumberCodecHelper.decodeInt;

/**
 * Month Codec.
 *
 * <p>
 * Encodes and decodes {@code Month} objects to and from {@code Int32}.
 * </p>
 * <p>Note: Requires Java 8 or greater.</p>
 *
 * @mongodb.driver.manual reference/bson-types
 * @since 3.7
 */
public class MonthCodec implements Codec<Month> {

    @Override
    public Month decode(final BsonReader reader, final DecoderContext decoderContext) {
        validateIsNumeric(reader, BsonType.INT32, Month.class);
        return Month.of(decodeInt(reader));
    }

    @Override
    public void encode(final BsonWriter writer, final Month value, final EncoderContext encoderContext) {
        writer.writeInt32(value.getValue());
    }

    @Override
    public Class<Month> getEncoderClass() {
        return Month.class;
    }
}
