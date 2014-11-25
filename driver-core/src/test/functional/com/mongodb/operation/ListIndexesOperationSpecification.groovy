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
import com.mongodb.OperationFunctionalSpecification
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding

class ListIndexesOperationSpecification extends OperationFunctionalSpecification {

    def 'should return empty list for nonexistent collection'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec())

        when:
        List<Document> indexes = operation.execute(getBinding())

        then:
        indexes.size() == 0
    }

    @Category(Async)
    def 'should return empty list for nonexistent collection asynchronously'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec())

        when:
        List<Document> indexes = executeAsync(operation)

        then:
        indexes.size() == 0
    }


    def 'should return default index on Collection that exists'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec())
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentThat', 'forces creation of the Collection'))

        when:
        List<Document> indexes = operation.execute(getBinding())

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'
    }

    @Category(Async)
    def 'should return default index on Collection that exists asynchronously'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec())
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentThat', 'forces creation of the Collection'))

        when:
        List<Document> indexes = executeAsync(operation)

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'
    }

    def 'should return created indexes on Collection'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec())
        collectionHelper.createIndex(new BsonDocument('theField', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('compound', new BsonInt32(1)).append('index', new BsonInt32(-1)))
        new CreateIndexOperation(namespace, new BsonDocument('unique', new BsonInt32(1))).unique(true).execute(getBinding())

        when:
        List<Document> indexes = operation.execute(getBinding())

        then:
        indexes.size() == 4
        indexes*.name.containsAll(['_id_', 'theField_1', 'compound_1_index_-1', 'unique_1'])
        indexes.find { it.name == 'unique_1' }.unique
    }

    @Category(Async)
    def 'should return created indexes on Collection asynchronously'() {
        given:
        def operation = new ListIndexesOperation(getNamespace(), new DocumentCodec())
        collectionHelper.createIndex(new BsonDocument('theField', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('compound', new BsonInt32(1)).append('index', new BsonInt32(-1)))
        new CreateIndexOperation(namespace, new BsonDocument('unique', new BsonInt32(1))).unique(true).execute(getBinding())

        when:
        List<Document> indexes = executeAsync(operation)

        then:
        indexes.size() == 4
        indexes*.name.containsAll(['_id_', 'theField_1', 'compound_1_index_-1', 'unique_1'])
        indexes.find { it.name == 'unique_1' }.unique
    }

}
