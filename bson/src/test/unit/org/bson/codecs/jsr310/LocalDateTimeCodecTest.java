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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.TimeZone;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LocalDateTimeCodecTest extends JsrTest {

    @Override
    Codec<?> getCodec() {
        return new LocalDateTimeCodec();
    }

    private static Stream<Arguments> roundTripLocalDateTimes() {
        return Stream.of(
                Arguments.of(LocalDateTime.of(2007, 10, 20, 0, 35), 1_192_840_500_000L),
                Arguments.of(LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC), 0L),
                Arguments.of(LocalDateTime.ofEpochSecond(-99_999_999_999L, 0, ZoneOffset.UTC), -99_999_999_999L * 1000),
                Arguments.of(LocalDateTime.ofEpochSecond(99_999_999_999L, 0, ZoneOffset.UTC), 99_999_999_999L * 1000)
        );
    }

    @ParameterizedTest
    @MethodSource("roundTripLocalDateTimes")
    void shouldRoundTripLocalDateTimeSuccessfully(final LocalDateTime localDateTime, final long millis) {
        BsonDocumentWriter writer = encode(localDateTime);
        assertEquals(millis, writer.getDocument().get("key").asDateTime().getValue());
        LocalDateTime actual = (LocalDateTime) decode(writer);
        assertEquals(localDateTime, actual);
    }

    @ParameterizedTest
    @ValueSource(strings = {"Pacific/Auckland", "UTC", "US/Hawaii"})
    void shouldRoundTripDifferentTimezonesTheSame(final String timeZone) {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(timeZone));
            LocalDateTime localDate = LocalDateTime.of(LocalDate.ofEpochDay(0), LocalTime.MIDNIGHT);
            BsonDocumentWriter writer = encode(localDate);
            assertEquals(0, writer.getDocument().get("key").asDateTime().getValue());
            LocalDateTime actual = (LocalDateTime) decode(writer);
            assertEquals(localDate, actual);
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    @Test
    void shouldWrapLongOverflowForLocalDateTimeMin() {
        CodecConfigurationException e = assertThrows(CodecConfigurationException.class, () -> encode(LocalDateTime.MIN));
        assertEquals(ArithmeticException.class, e.getCause().getClass());
    }

    @Test
    void shouldWrapLongOverflowForLocalDateTimeMax() {
        CodecConfigurationException e = assertThrows(CodecConfigurationException.class, () -> encode(LocalDateTime.MAX));
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
