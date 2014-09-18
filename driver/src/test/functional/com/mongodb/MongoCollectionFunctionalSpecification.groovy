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

import com.mongodb.client.MongoCollectionOptions
import com.mongodb.client.model.AggregateModel
import com.mongodb.client.model.AggregateOptions
import com.mongodb.client.model.CountModel
import com.mongodb.client.model.FindModel
import com.mongodb.client.model.FindOptions
import com.mongodb.client.model.MapReduceModel
import com.mongodb.codecs.DocumentCodecProvider
import com.mongodb.operation.Index
import com.mongodb.operation.OperationExecutor
import com.mongodb.operation.ReadOperation
import com.mongodb.operation.WriteOperation
import org.bson.codecs.configuration.RootCodecRegistry
import org.mongodb.Document
import spock.lang.IgnoreIf

import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.operation.OrderBy.ASC
import static java.util.Arrays.asList

// Due to the implementation of explain using private classes, it can't be effectively unit tests, so instead there is this integration
// test.
class MongoCollectionFunctionalSpecification extends FunctionalSpecification {
    def namespace = new MongoNamespace('db', 'coll')
    def options = MongoCollectionOptions.builder().writeConcern(WriteConcern.JOURNALED)
                                        .readPreference(secondary())
                                        .codecRegistry(new RootCodecRegistry([new DocumentCodecProvider()]))
                                        .build()

    def executor = new OperationExecutor() {
        @Override
        def <T> T execute(final ReadOperation<T> operation, final ReadPreference readPreference) {
            operation.execute(getBinding())
        }

        @Override
        def <T> T execute(final WriteOperation<T> operation) {
            operation.execute(getBinding());
        }
    }
    def collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor);

    def 'should explain a find model'() {
        given:
        collection.tools().createIndexes([Index.builder().addKey('x', ASC).sparse().build()])

        when:
        def model = new FindModel(new FindOptions().criteria(new Document('cold', true))
                                                   .batchSize(4)
                                                   .maxTime(1, TimeUnit.SECONDS)
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
                                               .maxTime(1, TimeUnit.SECONDS)
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
}
