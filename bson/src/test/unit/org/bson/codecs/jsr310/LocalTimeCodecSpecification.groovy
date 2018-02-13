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
class LocalTimeCodecSpecification extends Specification {

    def 'should round trip LocalTime successfully'() {
        given:
        def codec = new LocalTimeCodec()

        when:
        def writer = new BsonDocumentWriter(new BsonDocument())
        writer.writeStartDocument()
        writer.writeName('localTime')
        codec.encode(writer, localTime, EncoderContext.builder().build())
        writer.writeEndDocument()

        then:
        writer.getDocument().get('localTime').isDateTime()

        when:
        BsonReader bsonReader = new BsonDocumentReader(writer.getDocument())
        bsonReader.readStartDocument()
        bsonReader.readName()
        java.time.LocalTime actual = codec.decode(bsonReader, DecoderContext.builder().build())

        then:
        localTime == actual

        where:
        localTime << [
            // Can't use LocalTime.now() as JDK 9 uses a higher resolution clock.
            java.time.Instant.ofEpochMilli(System.currentTimeMillis()).atZone(java.time.ZoneOffset.UTC).toLocalDateTime().toLocalTime(),
            java.time.LocalTime.MIN,
            java.time.LocalTime.of(23, 59, 59, 999_000_000)
        ]
    }

    def 'should throw a CodecConfiguration exception if BsonType is invalid'() {
        given:
        def codec = new LocalTimeCodec()

        when:
        BsonReader bsonReader = new BsonDocumentReader(invalidDuration)
        bsonReader.readStartDocument()
        bsonReader.readName()
        codec.decode(bsonReader, DecoderContext.builder().build())

        then:
        thrown(CodecConfigurationException)

        where:
        invalidDuration << [
            BsonDocument.parse('{key: "10:00"}')
        ]
    }
}
