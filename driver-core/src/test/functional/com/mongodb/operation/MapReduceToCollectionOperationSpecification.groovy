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
import com.mongodb.MongoCommandException
import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.ValidationOptions
import com.mongodb.client.test.CollectionHelper
import org.bson.BsonDocument
import org.bson.BsonJavaScript
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.client.model.Filters.gte
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.MILLISECONDS

class MapReduceToCollectionOperationSpecification extends OperationFunctionalSpecification {
    def mapReduceInputNamespace = new MongoNamespace(getDatabaseName(), 'mapReduceInput')
    def mapReduceOutputNamespace = new MongoNamespace(getDatabaseName(), 'mapReduceOutput')
    def mapReduceOperation = new MapReduceToCollectionOperation(mapReduceInputNamespace,
                                                                new BsonJavaScript('function(){ emit( this.name , 1 ); }'),
                                                                new BsonJavaScript('function(key, values){ return values.length; }'),
                                                                mapReduceOutputNamespace.getCollectionName())
    def expectedResults = [['_id': 'Pete', 'value': 2.0] as Document,
                           ['_id': 'Sam', 'value': 1.0] as Document]
    def helper = new CollectionHelper<Document>(new DocumentCodec(), mapReduceOutputNamespace)

    def setup() {
        CollectionHelper<Document> helper = new CollectionHelper<Document>(new DocumentCodec(), mapReduceInputNamespace)
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        helper.insertDocuments(new DocumentCodec(), pete, sam, pete2)
    }

    def cleanup() {
        new DropCollectionOperation(mapReduceInputNamespace).execute(getBinding())
        new DropCollectionOperation(mapReduceOutputNamespace).execute(getBinding())
    }

    def 'should have the correct defaults'() {
        given:
        def mapF = new BsonJavaScript('function(){ emit( "level" , 1 ); }')
        def reduceF = new BsonJavaScript('function(key, values){ return values.length; }')
        def out = 'outCollection'

        when:
        def operation =  new MapReduceToCollectionOperation(getNamespace(), mapF, reduceF, out)

        then:
        operation.getMapFunction() == mapF
        operation.getReduceFunction() == reduceF
        operation.getAction() == 'replace'
        operation.getCollectionName() == out
        operation.getDatabaseName() == null
        operation.getFilter() == null
        operation.getFinalizeFunction() == null
        operation.getLimit() == 0
        operation.getScope() == null
        operation.getSort() == null
        operation.getMaxTime(MILLISECONDS) == 0
        operation.getBypassDocumentValidation() == null
        !operation.isJsMode()
        !operation.isVerbose()
        !operation.isSharded()
        !operation.isNonAtomic()
    }

    def 'should set optional values correctly'(){
        given:
        def mapF = new BsonJavaScript('function(){ emit( "level" , 1 ); }')
        def reduceF = new BsonJavaScript('function(key, values){ return values.length; }')
        def finalizeF = new BsonJavaScript('function(key, value) { return value }')
        def filter = BsonDocument.parse('{level: {$gte: 5}}')
        def sort = BsonDocument.parse('{level: 1}')
        def scope = BsonDocument.parse('{level: 1}')
        def out = 'outCollection'
        def action = 'merge'
        def dbName = 'dbName'

        when:
        def operation =  new MapReduceToCollectionOperation(getNamespace(), mapF, reduceF, out).action(action).databaseName(dbName)
                .finalizeFunction(finalizeF).filter(filter).limit(10).scope(scope).sort(sort).maxTime(1, MILLISECONDS)
                .bypassDocumentValidation(true)

        then:
        operation.getMapFunction() == mapF
        operation.getReduceFunction() == reduceF
        operation.getAction() == action
        operation.getCollectionName() == out
        operation.getDatabaseName() == dbName
        operation.getFilter() == filter
        operation.getLimit() == 10
        operation.getScope() == scope
        operation.getSort() == sort
        operation.getMaxTime(MILLISECONDS) == 1
        operation.getBypassDocumentValidation() == true
    }

    def 'should return the correct statistics and save the results'() {
        when:
        MapReduceStatistics results = mapReduceOperation.execute(getBinding())

        then:
        results.emitCount == 3
        results.inputCount == 3
        results.outputCount == 2
        helper.count() == 2
        helper.find() == expectedResults
    }

    @Category(Async)
    def 'should return the correct statistics and save the results asynchronously'() {

        when:
        MapReduceStatistics results = executeAsync(mapReduceOperation)

        then:
        results.emitCount == 3
        results.inputCount == 3
        results.outputCount == 2
        helper.count() == 2
        helper.find() == expectedResults
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 8)) })
    def 'should support bypassDocumentValidation'() {
        given:
        def collectionOutHelper = getCollectionHelper(new MongoNamespace(getDatabaseName(), 'collectionOut'))
        collectionOutHelper.create('collectionOut', new CreateCollectionOptions().validationOptions(
                new ValidationOptions().validator(gte('level', 10))))
        getCollectionHelper().insertDocuments(new BsonDocument())

        when:
        def operation = new MapReduceToCollectionOperation(mapReduceInputNamespace,
                new BsonJavaScript('function(){ emit( "level" , 1 ); }'),
                new BsonJavaScript('function(key, values){ return values.length; }'),
                'collectionOut')
        operation.execute(getBinding())

        then:
        thrown(MongoCommandException)

        when:
        operation.bypassDocumentValidation(false).execute(getBinding())

        then:
        thrown(MongoCommandException)

        when:
        operation.bypassDocumentValidation(true).execute(getBinding())

        then:
        notThrown(MongoCommandException)

        cleanup:
        collectionOutHelper?.drop()
    }

    @Category(Async)
    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 8)) })
    def 'should support bypassDocumentValidation asynchronously'() {
        given:
        def collectionOutHelper = getCollectionHelper(new MongoNamespace(getDatabaseName(), 'collectionOut'))
        collectionOutHelper.create('collectionOut', new CreateCollectionOptions().validationOptions(
                new ValidationOptions().validator(gte('level', 10))))
        getCollectionHelper().insertDocuments(new BsonDocument())

        when:
        def operation = new MapReduceToCollectionOperation(mapReduceInputNamespace,
                new BsonJavaScript('function(){ emit( "level" , 1 ); }'),
                new BsonJavaScript('function(key, values){ return values.length; }'),
                'collectionOut')
        executeAsync(operation)

        then:
        thrown(MongoCommandException)

        when:
        executeAsync(operation.bypassDocumentValidation(false))

        then:
        thrown(MongoCommandException)

        when:
        executeAsync(operation.bypassDocumentValidation(true))

        then:
        notThrown(MongoCommandException)

        cleanup:
        collectionOutHelper?.drop()
    }

}
