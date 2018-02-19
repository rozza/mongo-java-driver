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

package org.bson.codecs.jsr310

import org.bson.BsonDocument
import org.bson.codecs.Codec
import org.bson.codecs.configuration.CodecConfigurationException
import spock.lang.IgnoreIf

@IgnoreIf({ javaVersion < 1.8 })
class LocalDateTimeCodecSpecification extends JsrSpecification {

    def 'should round trip LocalDateTime successfully'() {
        when:
        def writer = encode(localDateTime)

        then:
        writer.getDocument().get('key').asDateTime().value == millis

        when:
        java.time.LocalDateTime actual = decode(writer)

        then:
        localDateTime  == actual

        where:
        localDateTime                                                                        | millis
        java.time.LocalDateTime.of(2007, 10, 20, 0, 35)                                      | 1_192_840_500_000
        java.time.LocalDateTime.ofEpochSecond(0, 0, java.time.ZoneOffset.UTC)                | 0
        java.time.LocalDateTime.ofEpochSecond(-99_999_999_999, 0, java.time.ZoneOffset.UTC)  | -99_999_999_999 * 1000
        java.time.LocalDateTime.ofEpochSecond(99_999_999_999, 0, java.time.ZoneOffset.UTC)   | 99_999_999_999 * 1000
    }

    def 'should round trip different timezones the same'() {
        given:
        def defaultTimeZone = TimeZone.getDefault()
        TimeZone.setDefault(TimeZone.getTimeZone(timeZone))
        def localDate = java.time.LocalDateTime.of(java.time.LocalDate.ofEpochDay(0), java.time.LocalTime.MIDNIGHT)

        when:
        def writer = encode(localDate)

        then:
        writer.getDocument().get('key').asDateTime().value == 0

        when:
        def actual = decode(writer)

        then:
        localDate == actual

        cleanup:
        TimeZone.setDefault(defaultTimeZone)

        where:
        timeZone << ['Pacific/Auckland', 'UTC', 'US/Hawaii']
    }

    def 'should wrap long overflow error in a CodecConfigurationException'() {
        when:
        encode(localDateTime)

        then:
        def e = thrown(CodecConfigurationException)
        e.getCause().getClass() == ArithmeticException

        where:
        localDateTime  << [
                java.time.LocalDateTime.MIN,
                java.time.LocalDateTime.MAX
        ]
    }

    def 'should throw a CodecConfiguration exception if BsonType is invalid'() {
        when:
        decode(invalidDuration)

        then:
        thrown(CodecConfigurationException)

        where:
        invalidDuration << [
                BsonDocument.parse('{key: "10 Minutes"}'),
                BsonDocument.parse('{key: 10}')
        ]
    }

    @Override
    Codec<?> getCodec() {
        new LocalDateTimeCodec()
    }
}
