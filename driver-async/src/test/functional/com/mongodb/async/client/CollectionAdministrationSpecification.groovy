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

package com.mongodb.async.client

import com.mongodb.client.model.CreateIndexModel
import org.mongodb.Document

class CollectionAdministrationSpecification extends FunctionalSpecification {

    def idIndex = ['_id': 1]
    def index1 = ['index': 1]
    def index1Model = new CreateIndexModel(index1 as Document)
    def index2Model = new CreateIndexModel(['index2': 1] as Document)

    def 'Drop should drop the collection'() {
        when:
        def client = Fixture.getMongoClient()
        def database = client.getDatabase(databaseName)
        database.createCollection(collectionName).get()

        then:
        database.getCollectionNames().get().contains(collectionName)

        when:
        collection.dropCollection().get()

        then:
        !database.getCollectionNames().get().contains(collectionName)
    }

    def 'getIndexes should not error for a nonexistent collection'() {
        when:
        def database = Fixture.getMongoClient().getDatabase(databaseName)

        then:
        !database.getCollectionNames().get().contains(collectionName)
        collection.getIndexes().get() == []
    }

    @SuppressWarnings(['FactoryMethodName'])
    def 'createIndexes should add indexes to the collection'() {
        when:
        collection.createIndexes([index1Model]).get()

        then:
        collection.getIndexes().get()*.get('key') containsAll(idIndex, index1)
    }

    def 'dropIndex should drop index'() {
        when:
        collection.createIndexes([index1Model]).get()

        then:
        collection.getIndexes().get()*.get('key') containsAll(idIndex, index1)

        when:
        collection.dropIndex('index_1').get()

        then:
        collection.getIndexes().get()*.get('key') == [idIndex]
    }

    def 'dropIndexes should drop all indexes apart from _id'() {
        when:
        collection.createIndexes([index1Model, index2Model]).get()

        then:
        collection.getIndexes().get()*.get('key') containsAll(idIndex, index1)

        when:
        collection.dropIndexes().get()

        then:
        collection.getIndexes().get()*.get('key') == [idIndex]
    }

}
