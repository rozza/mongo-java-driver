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
class YearMonthCodecSpecification extends Specification {

    def 'should round trip YearMonth successfully'() {
        given:
        def codec = new YearMonthCodec()

        when:
        def writer = new BsonDocumentWriter(new BsonDocument())
        writer.writeStartDocument()
        writer.writeName('yearMonth')
        codec.encode(writer, yearMonth, EncoderContext.builder().build())
        writer.writeEndDocument()

        then:
        writer.getDocument().get('yearMonth').isDecimal128()

        when:
        BsonReader bsonReader = new BsonDocumentReader(writer.getDocument())
        bsonReader.readStartDocument()
        bsonReader.readName()
        java.time.YearMonth actual = codec.decode(bsonReader, DecoderContext.builder().build())

        then:
        yearMonth == actual

        where:
        yearMonth << [
            java.time.YearMonth.now(),
            java.time.YearMonth.of(java.time.Year.MIN_VALUE, java.time.Month.JANUARY),
            java.time.YearMonth.of(java.time.Year.MAX_VALUE, java.time.Month.DECEMBER)
        ]
    }

    def 'should throw a CodecConfiguration exception if BsonType is invalid'() {
        given:
        def codec = new YearMonthCodec()

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
