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

import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InstantCodecTest extends JsrTest {

    @Override
    Codec<?> getCodec() {
        return new InstantCodec();
    }

    @Test
    void shouldRoundTripInstantEpoch() {
        Instant instant = Instant.EPOCH;
        BsonDocumentWriter writer = encode(instant);
        assertEquals(0, writer.getDocument().get("key").asDateTime().getValue());
        Instant actual = (Instant) decode(writer);
        assertEquals(instant, actual);
    }

    @Test
    void shouldRoundTripInstantSpecificDate() {
        Instant instant = LocalDateTime.of(2007, 10, 20, 0, 35).toInstant(ZoneOffset.UTC);
        BsonDocumentWriter writer = encode(instant);
        assertEquals(1_192_840_500_000L, writer.getDocument().get("key").asDateTime().getValue());
        Instant actual = (Instant) decode(writer);
        assertEquals(instant, actual);
    }

    @Test
    void shouldWrapLongOverflowForInstantMin() {
        CodecConfigurationException e = assertThrows(CodecConfigurationException.class, () -> encode(Instant.MIN));
        assertEquals(ArithmeticException.class, e.getCause().getClass());
    }

    @Test
    void shouldWrapLongOverflowForInstantMax() {
        CodecConfigurationException e = assertThrows(CodecConfigurationException.class, () -> encode(Instant.MAX));
        assertEquals(ArithmeticException.class, e.getCause().getClass());
    }

    @Test
    void shouldThrowCodecConfigurationExceptionIfBsonTypeIsInvalidString() {
        assertThrows(CodecConfigurationException.class, () -> decode(BsonDocument.parse("{key: \"10 Minutes\"}")));
    }

    @Test
    void shouldThrowCodecConfigurationExceptionIfBsonTypeIsInvalidInt() {
        assertThrows(CodecConfigurationException.class, () -> decode(BsonDocument.parse("{key: 10}")));
    }
}
