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

import com.mongodb.MongoNamespace
import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.BsonReader
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import spock.lang.Specification

class MongoNamespaceCodecSpecification extends Specification {

    def 'should round trip MongoNamespace successfully'() {
        when:
        def codec = new MongoNamespaceCodec()

        then:
        codec.getEncoderClass() == MongoNamespace

        when:
        def namespace = new MongoNamespace('databaseName.collectionName')
        def writer = new BsonDocumentWriter(new BsonDocument())
        codec.encode(writer, namespace, EncoderContext.builder().build())

        then:
        BsonDocument.parse('{db: "databaseName", coll: "collectionName"}') == writer.getDocument()

        when:
        BsonReader bsonReader = new BsonDocumentReader(writer.getDocument())
        MongoNamespace actual = codec.decode(bsonReader, DecoderContext.builder().build())

        then:
        namespace == actual
    }
}
