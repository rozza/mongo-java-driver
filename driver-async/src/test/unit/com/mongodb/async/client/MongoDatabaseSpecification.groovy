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

import com.mongodb.client.OperationOptions
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.operation.CommandReadOperation
import com.mongodb.operation.CommandWriteOperation
import com.mongodb.operation.CreateCollectionOperation
import com.mongodb.operation.DropDatabaseOperation
import com.mongodb.operation.ListCollectionNamesOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import spock.lang.Specification

import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.primaryPreferred
import static com.mongodb.ReadPreference.secondary

class MongoDatabaseSpecification extends Specification {

    def name = 'databaseName'
    def options = OperationOptions.builder().readPreference(secondary()).codecRegistry(MongoClientImpl.getDefaultCodecRegistry()).build()

    def 'should return the correct name from getName'() {
        given:
        def database = new MongoDatabaseImpl(name, options, new TestOperationExecutor())

        expect:
        database.getName() == name
    }

    def 'should return the correct options'() {
        given:
        def database = new MongoDatabaseImpl(name, options, new TestOperationExecutor())

        expect:
        database.getOptions() == options
    }

    def 'should be able to executeCommand correctly'() {
        given:
        def command = new BsonDocument('command', new BsonInt32(1))
        def executor = new TestOperationExecutor([null, null])
        def database = new MongoDatabaseImpl(name, options, executor)

        when:
        database.executeCommand(command).get()
        def operation = executor.getWriteOperation() as CommandWriteOperation<Document>

        then:
        operation.command == command

        when:
        database.executeCommand(command, primaryPreferred()).get()
        operation = executor.getReadOperation() as CommandReadOperation<Document>

        then:
        operation.command == command
        executor.getReadPreference() == primaryPreferred()
    }

    def 'should use DropDatabaseOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null])

        when:
        new MongoDatabaseImpl(name, options, executor).dropDatabase().get()
        def operation = executor.getWriteOperation() as DropDatabaseOperation

        then:
        operation.databaseName == name
    }

    def 'should use ListCollectionNamesOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([['collectionName']])

        when:
        new MongoDatabaseImpl(name, options, executor).getCollectionNames().get()
        def operation = executor.getReadOperation() as ListCollectionNamesOperation

        then:
        operation.databaseName == name
        executor.getReadPreference() == primary()
    }

    def 'should use CreateCollectionOperation correctly'() {
        given:
        def collectionName = 'collectionName'
        def createCollectionOptions = new CreateCollectionOptions()
        def executor = new TestOperationExecutor([null, null])
        def database = new MongoDatabaseImpl(name, options, executor)

        when:
        database.createCollection(collectionName, createCollectionOptions).get()
        def operation = executor.getWriteOperation() as CreateCollectionOperation

        then:
        operation.databaseName == name
        operation.isAutoIndex()
        !operation.isCapped()
        !operation.isUsePowerOf2Sizes()
        operation.getMaxDocuments() == 0
        operation.getSizeInBytes() == 0

        when:
        createCollectionOptions.autoIndex(false).capped(true).usePowerOf2Sizes(true).maxDocuments(100).sizeInBytes(1000)
        database.createCollection(collectionName, createCollectionOptions).get()
        operation = executor.getWriteOperation() as CreateCollectionOperation

        then:
        operation.databaseName == name
        !operation.isAutoIndex()
        operation.isCapped()
        operation.isUsePowerOf2Sizes()
        operation.getMaxDocuments() == 100
        operation.getSizeInBytes() == 1000
    }

}
