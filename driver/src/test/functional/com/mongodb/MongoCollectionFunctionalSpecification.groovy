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

package com.mongodb

import category.Slow
import com.mongodb.client.model.AggregateModel
import com.mongodb.client.model.AggregateOptions
import com.mongodb.client.model.CountModel
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.CreateIndexOptions
import com.mongodb.client.model.FindModel
import com.mongodb.client.model.FindOptions
import com.mongodb.client.model.MapReduceModel
import org.junit.experimental.categories.Category
import org.mongodb.Document
import spock.lang.IgnoreIf
import spock.lang.Specification

import java.util.concurrent.CountDownLatch

import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.Fixture.getDefaultDatabaseName
import static com.mongodb.Fixture.getMongoClient
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.SECONDS

// Due to the implementation of explain using private classes, it can't be effectively unit tests, so instead there is this integration
// test.
class MongoCollectionFunctionalSpecification extends Specification {

    def database = getMongoClient().getDatabase(getDefaultDatabaseName())
    def collection = database.getCollection('MongoCollectionFunctionalSpecification')

    def 'should explain a find model'() {
        given:
        collection.createIndex(new Document('x', 1), new CreateIndexOptions().sparse(true));

        when:
        def model = new FindModel(new FindOptions().criteria(new Document('cold', true))
                                                   .batchSize(4)
                                                   .maxTime(1, SECONDS)
                                                   .skip(5)
                                                   .limit(100)
                                                   .modifiers(new Document('$hint', 'x_1'))
                                                   .projection(new Document('x', 1))
                                                   .sort(new Document('y', 1)))
        def result = collection.explain(model, ExplainVerbosity.ALL_PLANS_EXECUTIONS)

        then:
        result
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should explain an aggregate model'() {
        given:
        def model = new AggregateModel([new Document('$match', new Document('job', 'plumber'))],
                                       new AggregateOptions()
                                               .allowDiskUse(true)
                                               .batchSize(10)
                                               .maxTime(1, SECONDS)
                                               .useCursor(true))

        when:
        def result = collection.explain(model, ExplainVerbosity.ALL_PLANS_EXECUTIONS)

        then:
        result.containsKey('stages')
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 7, 7)) || isSharded() })
    def 'should explain a count model'() {
        when:
        def result = collection.explain(new CountModel(), ExplainVerbosity.ALL_PLANS_EXECUTIONS)

        then:
        result
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 7, 8)) || isSharded() }) // Todo Scheduled to be supported but currently not (2.7.7)
    def 'should explain a mapReduce model'() {
        when:
        def result = collection.explain(new MapReduceModel('map', 'reduce'), ExplainVerbosity.ALL_PLANS_EXECUTIONS)

        then:
        result
    }

    def 'should support tailable cursors'() {
        given:
        collection.dropCollection()
        database.createCollection(collection.getNamespace().getCollectionName(),
                                  new CreateCollectionOptions().capped(true).sizeInBytes(1024))
        def document1 = ['a': 1] as Document
        def document2 = ['a': 2] as Document

        when:
        collection.insertOne(document1)
        def cursor = collection.find(new FindOptions().sort(['$natural': 1] as Document).tailable(true)).iterator()

        then:
        cursor.tryNext() == document1
        cursor.tryNext() == null

        when:
        collection.insertOne(document2)

        then:
        cursor.tryNext() == document2
    }

    @SuppressWarnings('EmptyCatchBlock')
    @Category(Slow)
    def 'test tailable blocks when calling hasNext'() {
        given:
        collection.dropCollection()
        database.createCollection(collection.getNamespace().getCollectionName(),
                                  new CreateCollectionOptions().capped(true).sizeInBytes(1024))
        def document1 = ['_id': 1] as Document
        def document2 = ['_id': 2] as Document

        when:
        collection.insertOne(document1)
        def cursor = collection.find(new FindOptions().sort(['$natural': 1] as Document).tailable(true)).iterator()

        then:
        cursor.hasNext()
        cursor.next().get('_id') == 1

        when:
        def latch = new CountDownLatch(1)
        Thread.start {
            try {
                sleep(1000)
                collection.insertOne(document2)
            } catch (interrupt) {
                //pass
            } finally {
                latch.countDown()
            }
        }

        then:
        cursor.hasNext()
        cursor.next().get('_id') == 2

        cleanup:
        latch.await(5, SECONDS)
    }

    @SuppressWarnings('EmptyCatchBlock')
    @Category(Slow)
    def 'test tailable interrupt'() throws InterruptedException {
        given:
        collection.dropCollection()
        database.createCollection(collection.getNamespace().getCollectionName(),
                                  new CreateCollectionOptions().capped(true).sizeInBytes(1024))
        def document1 = ['_id': 1] as Document
        def document2 = ['_id': 2] as Document

        when:
        collection.insertOne(document1)
        def cursor = collection.find(new FindOptions().sort(['$natural': 1] as Document).tailable(true)).iterator()

        CountDownLatch latch = new CountDownLatch(1)
        def seen;
        def thread = Thread.start {
           try {
                cursor.next()
                seen = 1
                cursor.next()
                seen = 2
            } catch (interrupt) {
                //pass
            } finally {
                latch.countDown()
            }
        }
        sleep(1000)
        thread.interrupt()
        collection.insertOne(document2)
        latch.await()

        then:
        seen == 1
    }
}
