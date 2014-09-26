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
import com.mongodb.CommandFailureException
import com.mongodb.DuplicateKeyException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.codecs.DocumentCodec
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.junit.experimental.categories.Category
import org.mongodb.Document

import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding

class CreateIndexSpecification extends OperationFunctionalSpecification {
    def idIndex = ['_id': 1]
    def x1 = ['x': 1]
    def field1Index = ['field': 1]
    def field2Index = ['field2': 1]
    def xyIndex = ['x.y': 1]

    def 'should be able to create a single index'() {
        given:
        def keys = new BsonDocument('field', new BsonInt32(1))
        def createIndexOperation = new CreateIndexOperation(getNamespace(), keys)

        when:
        createIndexOperation.execute(getBinding())

        then:
        getIndexes()*.get('key') containsAll(idIndex, field1Index)
    }

    @Category(Async)
    def 'should be able to create a single index asynchronously'() {

        given:
        def keys = new BsonDocument('field', new BsonInt32(1))
        def createIndexOperation = new CreateIndexOperation(getNamespace(), keys)

        when:
        createIndexOperation.executeAsync(getAsyncBinding()).get()

        then:
        getIndexes()*.get('key') containsAll(idIndex, field1Index)
    }

    def 'should be able to create a single index on a nested field'() {
        given:
        def keys = new BsonDocument('x.y', new BsonInt32(1))
        def createIndexOperation = new CreateIndexOperation(getNamespace(), keys)

        when:
        createIndexOperation.execute(getBinding())

        then:
        getIndexes()*.get('key') containsAll(idIndex, xyIndex)
    }

    def 'should be able to handle duplicate key errors when indexing'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), x1 as Document, x1 as Document)
        def createIndexOperation = new CreateIndexOperation(getNamespace(), new BsonDocument('field', new BsonInt32(1))).unique(true)

        when:
        createIndexOperation.execute(getBinding())

        then:
        thrown(DuplicateKeyException)
    }

    @Category(Async)
    def 'should be able to handle duplicate key errors when indexing asynchronously'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), x1 as Document, x1 as Document)
        def createIndexOperation = new CreateIndexOperation(getNamespace(), new BsonDocument('field', new BsonInt32(1))).unique(true)

        when:
        createIndexOperation.executeAsync(getAsyncBinding()).get()

        then:
        thrown(DuplicateKeyException)
    }

    def 'should throw when trying to build an invalid index'() {
        given:
        def createIndexOperation = new CreateIndexOperation(getNamespace(), new BsonDocument())

        when:
        createIndexOperation.execute(getBinding())

        then:
        thrown(CommandFailureException)
    }

    @Category(Async)
    def 'should throw when trying to build an invalid index asynchronously'() {
        given:
        def createIndexOperation = new CreateIndexOperation(getNamespace(), new BsonDocument())

        when:
        createIndexOperation.execute(getBinding())

        then:
        thrown(CommandFailureException)
    }

    def getIndexes() {
        new GetIndexesOperation(getNamespace(), new DocumentCodec()).execute(getBinding())
    }

}
