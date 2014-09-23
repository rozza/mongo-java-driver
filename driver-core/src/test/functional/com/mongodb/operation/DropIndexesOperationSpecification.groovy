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

import category.Async
import com.mongodb.MongoException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.codecs.DocumentCodec
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.junit.experimental.categories.Category
import org.mongodb.Document

import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding

class DropIndexesOperationSpecification extends OperationFunctionalSpecification {

    def 'should not error when dropping non-existent index on non-existent collection'() {
        when:
        new DropIndexOperation(getNamespace(), 'made_up_index_1').execute(getBinding())

        then:
        getIndexes().size() == 0
    }

    @Category(Async)
    def 'should not error when dropping non-existent index on non-existent collection asynchronously'() {
        when:
        new DropIndexOperation(getNamespace(), 'made_up_index_1').executeAsync(getAsyncBinding()).get()

        then:
        getIndexes().size() == 0
    }

    def 'should error when dropping non-existent index on existing collection'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentThat', 'forces creation of the Collection'))

        when:
        new DropIndexOperation(getNamespace(), 'made_up_index_1').execute(getBinding())

        then:
        thrown(MongoException)
    }

    @Category(Async)
    def 'should error when dropping non-existent index  on existing collection asynchronously'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentThat', 'forces creation of the Collection'))

        when:
        new DropIndexOperation(getNamespace(), 'made_up_index_1').executeAsync(getAsyncBinding()).get()

        then:
        thrown(MongoException)
    }

    def 'should drop existing index'() {
        given:
        collectionHelper.createIndexes([new BsonDocument('key', new BsonDocument('theField', new BsonInt32(1)))])

        when:
        new DropIndexOperation(getNamespace(), 'theField_1').execute(getBinding())
        List<Document> indexes = getIndexes()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'
    }

    @Category(Async)
    def 'should drop existing index asynchronously'() {
        given:
        collectionHelper.createIndexes([new BsonDocument('key', new BsonDocument('theField', new BsonInt32(1)))])
        def operation = new DropIndexOperation(getNamespace(), 'theField_1');

        when:
        operation.executeAsync(getAsyncBinding()).get()
        List<Document> indexes = getIndexes()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'
    }

    def 'should drop all indexes when passed *'() {
        given:
        collectionHelper.createIndexes([new BsonDocument('key', new BsonDocument('theField', new BsonInt32(1))),
                                        new BsonDocument('key', new BsonDocument('theOtherField', new BsonInt32(1)))])
        when:
        new DropIndexOperation(getNamespace(), '*').execute(getBinding())
        List<Document> indexes = getIndexes()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'
    }

    @Category(Async)
    def 'should drop all indexes when passed * asynchronously'() {
        given:
        collectionHelper.createIndexes([new BsonDocument('key', new BsonDocument('theField', new BsonInt32(1))),
                                        new BsonDocument('key', new BsonDocument('theOtherField', new BsonInt32(1)))])
        when:
        new DropIndexOperation(getNamespace(), '*').executeAsync(getAsyncBinding()).get()
        List<Document> indexes = getIndexes()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'
    }

    def getIndexes() {
        new GetIndexesOperation(getNamespace(), new DocumentCodec()).execute(getBinding())
    }

}
