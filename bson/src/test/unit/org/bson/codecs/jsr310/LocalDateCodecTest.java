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
import java.util.TimeZone;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LocalDateCodecTest extends JsrTest {

    @Override
    Codec<?> getCodec() {
        return new LocalDateCodec();
    }

    private static Stream<Arguments> roundTripLocalDates() {
        return Stream.of(
                Arguments.of(LocalDate.of(2007, 10, 20), 1_192_838_400_000L),
                Arguments.of(LocalDate.ofEpochDay(0), 0L),
                Arguments.of(LocalDate.ofEpochDay(-99_999_999_999L), -99_999_999_999L * 86_400_000L),
                Arguments.of(LocalDate.ofEpochDay(99_999_999_999L), 99_999_999_999L * 86_400_000L)
        );
    }

    @ParameterizedTest
    @MethodSource("roundTripLocalDates")
    void shouldRoundTripLocalDateSuccessfully(final LocalDate localDate, final long millis) {
        BsonDocumentWriter writer = encode(localDate);
        assertEquals(millis, writer.getDocument().get("key").asDateTime().getValue());
        LocalDate actual = (LocalDate) decode(writer);
        assertEquals(localDate, actual);
    }

    @ParameterizedTest
    @ValueSource(strings = {"Pacific/Auckland", "UTC", "US/Hawaii"})
    void shouldRoundTripDifferentTimezonesTheSame(final String timeZone) {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(timeZone));
            LocalDate localDate = LocalDate.ofEpochDay(0);
            BsonDocumentWriter writer = encode(localDate);
            assertEquals(0, writer.getDocument().get("key").asDateTime().getValue());
            LocalDate actual = (LocalDate) decode(writer);
            assertEquals(localDate, actual);
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    @Test
    void shouldWrapLongOverflowForLocalDateMin() {
        CodecConfigurationException e = assertThrows(CodecConfigurationException.class, () -> encode(LocalDate.MIN));
        assertEquals(ArithmeticException.class, e.getCause().getClass());
    }

    @Test
    void shouldWrapLongOverflowForLocalDateMax() {
        CodecConfigurationException e = assertThrows(CodecConfigurationException.class, () -> encode(LocalDate.MAX));
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
