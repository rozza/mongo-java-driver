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

import com.mongodb.WriteConcern
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.options.OperationOptions
import com.mongodb.operation.CommandReadOperation
import com.mongodb.operation.CommandWriteOperation
import com.mongodb.operation.CreateCollectionOperation
import com.mongodb.operation.DropDatabaseOperation
import com.mongodb.operation.ListCollectionNamesOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.codecs.configuration.RootCodecRegistry
import spock.lang.Specification

import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.primaryPreferred
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.ReadPreference.secondaryPreferred
import static com.mongodb.async.client.CustomMatchers.isTheSameAs
import static spock.util.matcher.HamcrestSupport.expect

class MongoDatabaseSpecification extends Specification {

    def name = 'databaseName'
    def options = OperationOptions.builder().readPreference(secondary()).codecRegistry(MongoClientImpl.getDefaultCodecRegistry()).build()

    def 'should return the correct name from getName'() {
        given:
        def database = new MongoDatabaseImpl(name, options, new TestOperationExecutor([]))

        expect:
        database.getName() == name
    }

    def 'should return the correct options'() {
        given:
        def database = new MongoDatabaseImpl(name, options, new TestOperationExecutor([]))

        expect:
        database.getOptions() == options
    }

    def 'should be able to executeCommand correctly'() {
        given:
        def command = new BsonDocument('command', new BsonInt32(1))
        def executor = new TestOperationExecutor([null, null, null, null])
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

        when:
        database.executeCommand(command, BsonDocument).get()
        operation = executor.getWriteOperation() as CommandWriteOperation<BsonDocument>

        then:
        operation.command == command

        when:
        database.executeCommand(command, primaryPreferred(), BsonDocument).get()
        operation = executor.getReadOperation() as CommandReadOperation<BsonDocument>

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
        expect operation, isTheSameAs(new DropDatabaseOperation(name))
    }

    def 'should use ListCollectionNamesOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([['collectionName']])

        when:
        new MongoDatabaseImpl(name, options, executor).getCollectionNames().get()
        def operation = executor.getReadOperation() as ListCollectionNamesOperation

        then:
        expect operation, isTheSameAs(new ListCollectionNamesOperation(name))
        executor.getReadPreference() == primary()
    }

    def 'should use CreateCollectionOperation correctly'() {
        given:
        def collectionName = 'collectionName'
        def executor = new TestOperationExecutor([null, null])
        def database = new MongoDatabaseImpl(name, options, executor)

        when:
        database.createCollection(collectionName).get()
        def operation = executor.getWriteOperation() as CreateCollectionOperation

        then:
        expect operation, isTheSameAs(new CreateCollectionOperation(name, collectionName))

        when:
        def createCollectionOptions = new CreateCollectionOptions()
                .autoIndex(false)
                .capped(true)
                .usePowerOf2Sizes(true)
                .maxDocuments(100)
                .sizeInBytes(1000)
        database.createCollection(collectionName, createCollectionOptions).get()
        operation = executor.getWriteOperation() as CreateCollectionOperation

        then:
        expect operation, isTheSameAs(new CreateCollectionOperation(name, collectionName)
                                              .autoIndex(false)
                                              .capped(true)
                                              .usePowerOf2Sizes(true)
                                              .maxDocuments(100)
                                              .sizeInBytes(1000))
    }

    def 'should pass the correct options to getCollection'() {
        given:
        def options = OperationOptions.builder()
                                        .readPreference(secondary())
                                        .writeConcern(WriteConcern.ACKNOWLEDGED)
                                        .codecRegistry(codecRegistry)
                                        .build()
        def executor = new TestOperationExecutor([])
        def database = new MongoDatabaseImpl(name, options, executor)

        when:
        def collectionOptions = customOptions ? database.getCollection('name', customOptions).getOptions()
                                              : database.getCollection('name').getOptions()
        then:
        collectionOptions.getReadPreference() == readPreference
        collectionOptions.getWriteConcern() == writeConcern
        collectionOptions.getCodecRegistry() == codecRegistry

        where:
        customOptions                                        | readPreference       | writeConcern              | codecRegistry
        null                                                 | secondary()          | WriteConcern.ACKNOWLEDGED | new RootCodecRegistry([])
        OperationOptions.builder().build()                   | secondary()          | WriteConcern.ACKNOWLEDGED | new RootCodecRegistry([])
        OperationOptions.builder()
                        .readPreference(secondaryPreferred())
                        .writeConcern(WriteConcern.MAJORITY)
                        .build()                             | secondaryPreferred() | WriteConcern.MAJORITY     | new RootCodecRegistry([])

    }

}
