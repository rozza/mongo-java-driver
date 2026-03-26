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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalTime;
import java.util.TimeZone;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LocalTimeCodecTest extends JsrTest {

    @Override
    Codec<?> getCodec() {
        return new LocalTimeCodec();
    }

    private static Stream<Arguments> roundTripLocalTimes() {
        return Stream.of(
                Arguments.of(LocalTime.MIN, 0L),
                Arguments.of(LocalTime.of(23, 59, 59, 999_000_000), 86_399_999L)
        );
    }

    @ParameterizedTest
    @MethodSource("roundTripLocalTimes")
    void shouldRoundTripLocalTimeSuccessfully(final LocalTime localTime, final long millis) {
        BsonDocumentWriter writer = encode(localTime);
        assertEquals(millis, writer.getDocument().get("key").asDateTime().getValue());
        LocalTime actual = (LocalTime) decode(writer);
        assertEquals(localTime, actual);
    }

    @ParameterizedTest
    @ValueSource(strings = {"Pacific/Auckland", "UTC", "US/Hawaii"})
    void shouldRoundTripDifferentTimezonesTheSame(final String timeZone) {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(timeZone));
            LocalTime localTime = LocalTime.MIDNIGHT;
            BsonDocumentWriter writer = encode(localTime);
            assertEquals(0, writer.getDocument().get("key").asDateTime().getValue());
            LocalTime actual = (LocalTime) decode(writer);
            assertEquals(localTime, actual);
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    @Test
    void shouldThrowCodecConfigurationExceptionIfBsonTypeIsInvalidString() {
        assertThrows(CodecConfigurationException.class, () -> decode(BsonDocument.parse("{key: \"10:00\"}")));
    }
}
