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
import com.mongodb.MongoException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.WriteConcernResult
import com.mongodb.bulk.UpdateRequest
import com.mongodb.bulk.WriteRequest
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonObjectId
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.bson.types.ObjectId
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static java.util.Arrays.asList

class UpdateOperationSpecification extends OperationFunctionalSpecification {

    def 'should update nothing if no documents match'() {
        given:
        def operation = new UpdateOperation(getNamespace(), true, ACKNOWLEDGED,
                                     asList(new UpdateRequest(new BsonDocument('x', new BsonInt32(1)),
                                                              new BsonDocument('$set', new BsonDocument('y', new BsonInt32(2))),
                                                              WriteRequest.Type.UPDATE)
                                                    .multi(false)))

        when:
        WriteConcernResult result = execute(operation, async)

        then:
        result.wasAcknowledged()
        result.count == 0
        result.upsertedId == null
        !result.isUpdateOfExisting()
        getCollectionHelper().count() == 0

        where:
        async << [true, false]
    }

    def 'when multi is false should update one matching document'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                                              new Document('x', 1),
                                              new Document('x', 1))
        def operation = new UpdateOperation(getNamespace(), true, ACKNOWLEDGED,
                                     asList(new UpdateRequest(new BsonDocument('x', new BsonInt32(1)),
                                                              new BsonDocument('$set', new BsonDocument('y', new BsonInt32(2))),
                                                              WriteRequest.Type.UPDATE)
                                                    .multi(false)))
        when:
        WriteConcernResult result = execute(operation, async)

        then:
        result.wasAcknowledged()
        result.count == 1
        result.upsertedId == null
        result.isUpdateOfExisting()
        getCollectionHelper().count(new Document('y', 2)) == 1

        where:
        async << [true, false]
    }

    def 'when multi is true should update all matching documents'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                                              new Document('x', 1),
                                              new Document('x', 1))
        def operation = new UpdateOperation(getNamespace(), true, ACKNOWLEDGED,
                                     asList(new UpdateRequest(new BsonDocument('x', new BsonInt32(1)),
                                                              new BsonDocument('$set', new BsonDocument('y', new BsonInt32(2))),
                                                              WriteRequest.Type.UPDATE)
                                                    .multi(true)))
        when:
        WriteConcernResult result = execute(operation, async)

        then:
        result.wasAcknowledged()
        result.count == 2
        result.upsertedId == null
        result.isUpdateOfExisting()
        getCollectionHelper().count(new Document('y', 2)) == 2

        where:
        async << [true, false]
    }

    def 'when upsert is true should insert a document if there are no matching documents'() {
        given:
        def operation = new UpdateOperation(getNamespace(), true, ACKNOWLEDGED,
                                     asList(new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                                              new BsonDocument('$set', new BsonDocument('y', new BsonInt32(2))),
                                                              WriteRequest.Type.UPDATE)
                                                    .upsert(true)))
        when:
        WriteConcernResult result = execute(operation, async)

        then:
        result.wasAcknowledged()
        result.count == 1
        result.upsertedId == (serverVersionAtLeast(asList(2, 6, 0)) ? new BsonInt32(1) : null)
        !result.isUpdateOfExisting()
        getCollectionHelper().count(new Document('y', 2)) == 1

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    @Category(Slow)
    def 'should allow update larger than 16MB'() {
        // small enough so the update document is 16MB, but enough to push the the request as a whole over 16MB
        def binary = new BsonBinary(new byte[16 * 1024 * 1024 - 24])
        given:
        def operation = new UpdateOperation(getNamespace(), true, ACKNOWLEDGED,
                                     asList(new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                                              new BsonDocument('$set', new BsonDocument('y', binary)),
                                                              WriteRequest.Type.UPDATE)
                                                    .upsert(true)))
        when:
        WriteConcernResult result = execute(operation, async)

        then:
        result.wasAcknowledged()
        result.count == 1
        result.upsertedId == new BsonInt32(1)
        !result.isUpdateOfExisting()
        getCollectionHelper().count(new Document('_id', 1)) == 1

        where:
        async << [true, false]
    }


    def 'should return correct result for upsert'() {
        given:
        def id = new ObjectId()
        def operation = new UpdateOperation(getNamespace(), true, ACKNOWLEDGED,
                                     asList(new UpdateRequest(new BsonDocument('_id', new BsonObjectId(id)),
                                                              new BsonDocument('$set', new BsonDocument('x', new BsonInt32(1))),
                                                              WriteRequest.Type.UPDATE).
                                                    upsert(true)))

        when:
        WriteConcernResult result = execute(operation, async)

        then:
        result.wasAcknowledged()
        result.count == 1
        result.upsertedId == new BsonObjectId(id)
        !result.isUpdateOfExisting()

        where:
        async << [true, false]
    }

    def 'when an update request document contains a non $-prefixed key, update should throw IllegalArgumentException'() {
        given:
        def operation = new UpdateOperation(getNamespace(), ordered, ACKNOWLEDGED,
                            [new UpdateRequest(new BsonDocument(),
                                               new BsonDocument('$set', new BsonDocument('x', new BsonInt32(2)))
                                                       .append('y', new BsonInt32(2)), WriteRequest.Type.UPDATE)])

        when:
        execute(operation, async)

        then:
        def ex = thrown(Exception)
        if (async) {
            ex instanceof MongoException
            ex.cause instanceof IllegalArgumentException
        } else {
            ex instanceof IllegalArgumentException
        }

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'when an update document is empty, update should throw IllegalArgumentException'() {
        given:
        def operation =  new UpdateOperation(getNamespace(), ordered, ACKNOWLEDGED,
                [new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.UPDATE)])

        when:
        execute(operation, [3, 4, 0], async)

        then:
        def ex = thrown(Exception)
        if (async) {
            ex instanceof MongoException
            ex.cause instanceof IllegalArgumentException
        } else {
            ex instanceof IllegalArgumentException
        }

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'should throw an exception when using an unsupported Collation'() {
        given:
        def operation = new UpdateOperation(getNamespace(), false, ACKNOWLEDGED, requests)

        when:
        testOperationThrows(operation, [3, 2, 0], async)

        then:
        def exception = thrown(IllegalArgumentException)
        exception.getMessage().startsWith('Unsupported collation')

        where:
        [async, requests] << [
                [true, false],
                [[new UpdateRequest(new BsonDocument(), BsonDocument.parse('{$set: {x: 1}}'), WriteRequest.Type.UPDATE)
                         .collation(defaultCollation)],
                 [new UpdateRequest(new BsonDocument(), BsonDocument.parse('{$set: {y: 1}}'), WriteRequest.Type.UPDATE),
                  new UpdateRequest(new BsonDocument(), BsonDocument.parse('{$set: {a: 1}}'), WriteRequest.Type.UPDATE)
                          .collation(defaultCollation)]]
        ].combinations()
    }

}
