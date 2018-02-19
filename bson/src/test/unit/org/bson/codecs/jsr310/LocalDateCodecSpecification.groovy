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
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.BsonReader
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.codecs.configuration.CodecConfigurationException
import spock.lang.IgnoreIf
import spock.lang.Specification

@IgnoreIf({ javaVersion < 1.8 })
class LocalDateCodecSpecification extends Specification {

    def 'should round trip LocalDate successfully'() {
        given:
        def codec = new LocalDateCodec()

        when:
        def writer = new BsonDocumentWriter(new BsonDocument())
        writer.writeStartDocument()
        writer.writeName('localDate')
        codec.encode(writer, localDate, EncoderContext.builder().build())
        writer.writeEndDocument()

        then:
        writer.getDocument().get('localDate').isDateTime()

        when:
        BsonReader bsonReader = new BsonDocumentReader(writer.getDocument())
        bsonReader.readStartDocument()
        bsonReader.readName()
        java.time.LocalDate actual = codec.decode(bsonReader, DecoderContext.builder().build())

        then:
        localDate == actual

        where:
        localDate << [
            java.time.LocalDate.now(),
            java.time.LocalDate.ofEpochDay(0),
            java.time.LocalDate.ofEpochDay(-99_999_999_999),
            java.time.LocalDate.ofEpochDay(99_999_999_999)
        ]
    }

    def 'should wrap long overflow error in a CodecConfigurationException'() {
        given:
        def codec = new LocalDateCodec()

        when:
        def writer = new BsonDocumentWriter(new BsonDocument())
        writer.writeStartDocument()
        writer.writeName('localDate')
        codec.encode(writer, localDate, EncoderContext.builder().build())

        then:
        def e = thrown(CodecConfigurationException)
        e.getCause().getClass() == ArithmeticException

        where:
        localDate << [
                java.time.LocalDate.MIN,
                java.time.LocalDate.MAX
        ]
    }

    def 'should throw a CodecConfiguration exception if BsonType is invalid'() {
        given:
        def codec = new LocalDateCodec()

        when:
        BsonReader bsonReader = new BsonDocumentReader(invalidDuration)
        bsonReader.readStartDocument()
        bsonReader.readName()
        codec.decode(bsonReader, DecoderContext.builder().build())

        then:
        thrown(CodecConfigurationException)

        where:
        invalidDuration << [
                BsonDocument.parse('{key: "10 Minutes"}'),
                BsonDocument.parse('{key: 10}')
        ]
    }
}
