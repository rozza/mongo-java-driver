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

package com.mongodb

import org.bson.BsonArray
import org.bson.BsonBinary
import org.bson.BsonBinaryReader
import org.bson.BsonBinarySubType
import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.BsonInt32
import org.bson.BsonJavaScriptWithScope
import org.bson.BsonNull
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.BsonSymbol
import org.bson.BsonTimestamp
import org.bson.codecs.BinaryCodec
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.codecs.UuidCodec
import org.bson.codecs.ValueCodecProvider
import org.bson.types.BSONTimestamp
import org.bson.types.Binary
import org.bson.types.CodeWScope
import org.bson.types.ObjectId
import org.bson.types.Symbol
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.ByteBuffer
import java.sql.Timestamp

import static org.bson.UuidRepresentation.C_SHARP_LEGACY
import static org.bson.UuidRepresentation.JAVA_LEGACY
import static org.bson.UuidRepresentation.PYTHON_LEGACY
import static org.bson.UuidRepresentation.STANDARD
import static org.bson.UuidRepresentation.UNSPECIFIED
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries

class DBObjectCodecSpecification extends Specification {

    def bsonDoc = new BsonDocument()
    def codecRegistry = fromRegistries(fromCodecs(new UuidCodec(JAVA_LEGACY)),
            fromProviders([new ValueCodecProvider(), new DBObjectCodecProvider(), new BsonValueCodecProvider()]))
    def dbObjectCodec = new DBObjectCodec(codecRegistry).withUuidRepresentation(JAVA_LEGACY)

    def 'default registry should include necessary providers'() {
        when:
        def registry = DBObjectCodec.getDefaultRegistry()

        then:
        registry.get(Integer) != null
        registry.get(BsonInt32) != null
        registry.get(BSONTimestamp) != null
        registry.get(BasicDBObject) != null
    }

    def 'should encode with default registry'() {
        given:
        def document = new BsonDocument()
        def dBObject = new BasicDBObject('a', 0).append('b', new BsonInt32(1)).append('c', new BSONTimestamp())

        when:
         new DBObjectCodec().encode(new BsonDocumentWriter(document), dBObject, EncoderContext.builder().build())

        then:
        document == new BsonDocument('a', new BsonInt32(0)).append('b', new BsonInt32(1)).append('c', new BsonTimestamp())
    }

    def 'should encode and decode UUID as UUID'() {
        given:
        def uuid = UUID.fromString('01020304-0506-0708-090a-0b0c0d0e0f10')
        def doc = new BasicDBObject('uuid', uuid)

        when:
        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build())

        then:
        bsonDoc.getBinary('uuid') == new BsonBinary(BsonBinarySubType.UUID_LEGACY,
                                                    [8, 7, 6, 5, 4, 3, 2, 1, 16, 15, 14, 13, 12, 11, 10, 9] as byte[])
        when:
        def decodedUuid = dbObjectCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build())

        then:
        decodedUuid.get('uuid') == uuid
    }

    def 'should decode binary subtypes for UUID that are not 16 bytes into Binary'() {
        given:
        def reader = new BsonBinaryReader(ByteBuffer.wrap(bytes as byte[]))

        when:
        def document = new DBObjectCodec().decode(reader, DecoderContext.builder().build())

        then:
        value == document.get('f')

        where:
        value                                            | bytes
        new Binary((byte) 0x03, (byte[]) [115, 116, 11]) | [16, 0, 0, 0, 5, 102, 0, 3, 0, 0, 0, 3, 115, 116, 11, 0]
        new Binary((byte) 0x04, (byte[]) [115, 116, 11]) | [16, 0, 0, 0, 5, 102, 0, 3, 0, 0, 0, 4, 115, 116, 11, 0]
    }

    @SuppressWarnings(['LineLength'])
    @Unroll
    def 'should decode binary subtype 3 for UUID'() {
        given:
        def reader = new BsonBinaryReader(ByteBuffer.wrap(bytes as byte[]))

        when:
        def document = new DBObjectCodec(fromCodecs(new UuidCodec(representation), new BinaryCodec()))
                .withUuidRepresentation(representation)
                .decode(reader, DecoderContext.builder().build())

        then:
        value == document.get('f')

        where:
        representation | value                                                   | bytes
        JAVA_LEGACY    | UUID.fromString('08070605-0403-0201-100f-0e0d0c0b0a09') | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
        C_SHARP_LEGACY | UUID.fromString('04030201-0605-0807-090a-0b0c0d0e0f10') | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
        PYTHON_LEGACY  | UUID.fromString('01020304-0506-0708-090a-0b0c0d0e0f10') | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
        STANDARD       | new Binary((byte) 3, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] as byte[]) | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
        UNSPECIFIED    | new Binary((byte) 3, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] as byte[]) | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
    }

    def 'should encode and decode UUID as UUID with alternate UUID Codec'() {
        given:
        def codecWithAlternateUUIDCodec = new DBObjectCodec(fromRegistries(fromCodecs(new UuidCodec(STANDARD)), codecRegistry))
                .withUuidRepresentation(STANDARD)
        def uuid = UUID.fromString('01020304-0506-0708-090a-0b0c0d0e0f10')
        def doc = new BasicDBObject('uuid', uuid)

        when:
        codecWithAlternateUUIDCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build())

        then:
        bsonDoc.getBinary('uuid') == new BsonBinary(BsonBinarySubType.UUID_STANDARD,
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] as byte[])

        when:
        def decodedDoc = codecWithAlternateUUIDCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build())

        then:
        decodedDoc.get('uuid') == uuid
    }

    @SuppressWarnings(['LineLength'])
    @Unroll
    def 'should decode binary subtype 4 for UUID'() {
        given:
        def reader = new BsonBinaryReader(ByteBuffer.wrap(bytes as byte[]))

        when:
        def document = new DBObjectCodec().withUuidRepresentation(representation)
                .decode(reader, DecoderContext.builder().build())

        then:
        value == document.get('f')

        where:
        representation | value                                                                                   | bytes
        STANDARD       | UUID.fromString('01020304-0506-0708-090a-0b0c0d0e0f10')                                 | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
        JAVA_LEGACY    | new Binary((byte) 4, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] as byte[]) | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
        C_SHARP_LEGACY | new Binary((byte) 4, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] as byte[]) | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
        PYTHON_LEGACY  | new Binary((byte) 4, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] as byte[]) | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
        UNSPECIFIED    | new Binary((byte) 4, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] as byte[]) | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
    }

    def 'should encode and decode byte array value as binary'() {
        given:
        def array = [0, 1, 2, 4, 4] as byte[]
        def doc = new BasicDBObject('byteArray', array)

        when:
        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build())

        then:
        bsonDoc.getBinary('byteArray') == new BsonBinary(array)

        when:
        DBObject decodedUuid = dbObjectCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build())

        then:
        decodedUuid.get('byteArray') == array
    }

    def 'should encode and decode Binary value as binary'() {
        given:
        def subType = (byte) 42
        def array = [0, 1, 2, 4, 4] as byte[]
        def doc = new BasicDBObject('byteArray', new Binary(subType, array))

        when:
        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build())

        then:
        bsonDoc.getBinary('byteArray') == new BsonBinary(subType, array)

        when:
        DBObject decodedUuid = dbObjectCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build())

        then:
        decodedUuid.get('byteArray') == new Binary(subType, array)
    }

    def 'should encode Symbol to BsonSymbol and decode BsonSymbol to String'() {
        given:
        def symbol = new Symbol('symbol')
        def doc = new BasicDBObject('symbol', symbol)

        when:
        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build())

        then:
        bsonDoc.get('symbol') == new BsonSymbol('symbol')

        when:
        def decodedSymbol = dbObjectCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build())

        then:
        decodedSymbol.get('symbol') == symbol.toString()
    }

    def 'should encode java.sql.Date as date'() {
        given:
        def sqlDate = new java.sql.Date(System.currentTimeMillis())
        def doc = new BasicDBObject('d', sqlDate)

        when:
        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build())

        then:
        def decodededDoc = dbObjectCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build())

        then:
        ((Date) decodededDoc.get('d')).getTime() == sqlDate.getTime()
    }

    def 'should encode java.sql.Timestamp as date'() {
        given:
        def sqlTimestamp = new Timestamp(System.currentTimeMillis())
        def doc = new BasicDBObject('d', sqlTimestamp)

        when:
        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build())

        then:
        def decodededDoc = dbObjectCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build())

        then:
        ((Date) decodededDoc.get('d')).getTime() == sqlTimestamp.getTime()
    }

    def 'should encode collectible document with _id'() {
        given:
        def doc = new BasicDBObject('y', 1).append('_id', new BasicDBObject('x', 1))

        when:
        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc,
                EncoderContext.builder().isEncodingCollectibleDocument(true).build())

        then:
        bsonDoc == new BsonDocument('_id', new BsonDocument('x', new BsonInt32(1))).append('y', new BsonInt32(1))
    }

    def 'should encode collectible document without _id'() {
        given:
        def doc = new BasicDBObject('y', 1)

        when:
        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc,
                EncoderContext.builder().isEncodingCollectibleDocument(true).build())

        then:
        bsonDoc == new BsonDocument('y', new BsonInt32(1))
    }

    def 'should encode all types'() {
        given:
        def id = new ObjectId()
        def dbRef = new DBRef('c', 1)
        def doc = new BasicDBObject('_id', id)
                .append('n', null)
                .append('r', dbRef)
                .append('m', ['f': 1])
                .append('i', [1, 2])
                .append('c', new CodeWScope('c', new BasicDBObject('f', 1)))
                .append('b', new byte[0])
                .append('a', [1, 2].toArray())
                .append('s', new Symbol('s'))

        when:
        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build())

        then:
        bsonDoc == new BsonDocument('_id', new BsonObjectId(id))
                .append('n', new BsonNull())
                .append('r', new BsonDocument('$ref', new BsonString('c')).append('$id', new BsonInt32(1)))
                .append('m', new BsonDocument('f', new BsonInt32(1)))
                .append('i', new BsonArray([new BsonInt32(1), new BsonInt32(2)]))
                .append('c', new BsonJavaScriptWithScope('c', new BsonDocument('f', new BsonInt32(1))))
                .append('b', new BsonBinary(new byte[0]))
                .append('a', new BsonArray([new BsonInt32(1), new BsonInt32(2)]))
                .append('s', new BsonSymbol('s'))
    }
}
