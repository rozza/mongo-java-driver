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

package com.mongodb.connection

import org.bson.BsonBinaryWriter
import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonElement
import org.bson.BsonString
import org.bson.RawBsonDocument
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.EncoderContext
import org.bson.io.BasicOutputBuffer
import spock.lang.Specification

import static org.bson.BsonHelper.documentWithValuesOfEveryType

class ElementExtendingBsonWriterSpecification extends Specification {

    def 'should write all types'() {
        given:
        def encodedDoc = new BsonDocument()
        def bsonOutput = createBsonOutput(encodedDoc)
        def writer = new ElementExtendingBsonWriter(new BsonBinaryWriter(bsonOutput), [])

        when:
        new BsonDocumentCodec().encode(writer, encodedDoc, EncoderContext.builder().build())

        then:
        new RawBsonDocument(bsonOutput.getInternalBuffer().clone()) == encodedDoc
    }

    def 'should extend with extra elements'() {
        given:
        def encodedDoc = BsonDocument.parse("{a: 1}")
        def bsonOutput = createBsonOutput(encodedDoc)

        def extraElements = [
                new BsonElement('$db', new BsonString('test')),
                new BsonElement('$readPreference', new BsonDocument('mode', new BsonString('primary')))
        ]
        def writer = new ElementExtendingBsonWriter(new BsonBinaryWriter(bsonOutput), extraElements)

        def expectedDocument = BsonDocument.parse("{a: 1}")
        for (def cur : extraElements) {
            expectedDocument.put(cur.name, cur.value)
        }

        when:
        new BsonDocumentCodec().encode(writer, encodedDoc, EncoderContext.builder().build())

        then:
        new RawBsonDocument(bsonOutput.getInternalBuffer()) == expectedDocument
    }

    def 'should extend with extra elements when piping a reader at the top level'() {
        given:
        def encodedDoc = new BsonDocument()
        def extraElements = [
                new BsonElement('$db', new BsonString('test')),
                new BsonElement('$readPreference', new BsonDocument('mode', new BsonString('primary')))
        ]

        def bsonOutput = createBsonOutput(encodedDoc)
        def writer = new ElementExtendingBsonWriter(new BsonBinaryWriter(bsonOutput), extraElements)

        def expectedDocument = documentWithValuesOfEveryType()
        for (def cur : extraElements) {
            expectedDocument.put(cur.name, cur.value)
        }

        when:
        writer.pipe(new BsonDocumentReader(documentWithValuesOfEveryType()))

        then:
        new RawBsonDocument(bsonOutput.getInternalBuffer().clone()) == expectedDocument
    }

    def 'should not extend with extra elements when piping a reader at nested level'() {
        given:
        def encodedDoc = new BsonDocument()
        def extraElements = [
                new BsonElement('$db', new BsonString('test')),
                new BsonElement('$readPreference', new BsonDocument('mode', new BsonString('primary')))
        ]
        def expectedDocument = new BsonDocument('pipedDocument', new BsonDocument())
        for (def cur : extraElements) {
            expectedDocument.put(cur.name, cur.value)
        }
        def writer = new ElementExtendingBsonWriter(createBsonBinaryWriter(encodedDoc), extraElements)

        when:
        writer.writeStartDocument()
        writer.writeName('pipedDocument')
        writer.pipe(new BsonDocumentReader(new BsonDocument()))
        writer.writeEndDocument()

        then:
        encodedDoc == expectedDocument
    }

    BasicOutputBuffer createBsonOutput(final BsonDocument content) {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer()
        new BsonDocumentCodec().encode(new BsonBinaryWriter(bsonOutput), content, EncoderContext.builder().build())
        bsonOutput
    }
}
