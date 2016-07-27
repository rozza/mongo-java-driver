/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.operation

import category.Slow
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.bulk.DeleteRequest
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category

import static com.mongodb.WriteConcern.ACKNOWLEDGED

class DeleteOperationSpecification extends OperationFunctionalSpecification {
    def 'should remove a document'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('_id', 1))
        def operation = new DeleteOperation(getNamespace(), true, ACKNOWLEDGED,
                                     [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1)))])

        when:
        execute(operation, async)

        then:
        getCollectionHelper().count() == 0

        where:
        async << [true, false]

    }

    @Category(Slow)
    def 'should split removes into batches'() {
        given:
        def bigDoc = new BsonDocument('bytes', new BsonBinary(new byte[1024 * 1024 * 16 - 2127]))
        def smallerDoc = new BsonDocument('bytes', new BsonBinary(new byte[1024 * 16 + 1980]))
        def simpleDoc = new BsonDocument('_id', new BsonInt32(1))
        getCollectionHelper().insertDocuments(new BsonDocumentCodec(), simpleDoc)
        def operation = new DeleteOperation(getNamespace(), true, ACKNOWLEDGED,
                [new DeleteRequest(bigDoc), new DeleteRequest(smallerDoc), new DeleteRequest(simpleDoc)])

        when:
        execute(operation, async)

        then:
        getCollectionHelper().count() == 0

        where:
        async << [true, false]
    }

    def 'should throw an exception when using an unsupported Collation'() {
        given:
        def operation = new DeleteOperation(getNamespace(), false, ACKNOWLEDGED, requests)

        when:
        testOperationThrows(operation, [3, 2, 0], async)

        then:
        def exception = thrown(IllegalArgumentException)
        exception.getMessage().startsWith('Unsupported collation')

        where:
        [async, requests] << [
                [true, false],
                [[new DeleteRequest(BsonDocument.parse('{x: 1}}')).collation(defaultCollation)],
                 [new DeleteRequest(BsonDocument.parse('{x: 1}}')),
                  new DeleteRequest(BsonDocument.parse('{y: 1}}')).collation(defaultCollation)]]
        ].combinations()
    }

}
