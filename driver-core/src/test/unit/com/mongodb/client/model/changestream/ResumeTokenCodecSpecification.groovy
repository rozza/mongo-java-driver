/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model.changestream

import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.BsonReader
import org.bson.Document
import org.bson.RawBsonDocument
import org.bson.codecs.DecoderContext
import org.bson.codecs.DocumentCodec
import org.bson.codecs.EncoderContext
import spock.lang.Specification

class ResumeTokenCodecSpecification extends Specification {

    def 'should round trip ResumeToken successfully'() {
        when:
        def codec = new ResumeTokenCodec()

        then:
        codec.getEncoderClass() == ResumeToken

        when:
        def rawToken = RawBsonDocument.parse('{token: true}')
        def resumeToken = new ResumeToken(rawToken)
        def writer = new BsonDocumentWriter(new BsonDocument())
        codec.encode(writer, resumeToken, EncoderContext.builder().build())

        then:
        rawToken == writer.getDocument()

        when:
        BsonReader bsonReader = new BsonDocumentReader(writer.getDocument())
        ResumeToken actual = codec.decode(bsonReader, DecoderContext.builder().build())

        then:
        resumeToken == actual
    }

    def 'should parse and decode as expected' () {
        when:
        def json = '{token: true}'
        def resumeToken = ResumeToken.parse(json)

        then:
        resumeToken.decode(new DocumentCodec()) == Document.parse(json)
    }

    def 'should accept bytes' () {
        when:
        def rawBsonDocument = RawBsonDocument.parse('{token: true}')
        def resumeToken = new ResumeToken(rawBsonDocument.getByteBuffer().array())

        then:
        resumeToken == new ResumeToken(rawBsonDocument)
    }
}
