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
import com.mongodb.client.model.AggregateOptions
import com.mongodb.client.test.Worker
import com.mongodb.operation.AggregateOperation
import com.mongodb.operation.AggregateToCollectionOperation
import com.mongodb.operation.FindOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.CodecConfigurationException
import org.bson.codecs.configuration.RootCodecRegistry
import spock.lang.Specification

import static java.util.concurrent.TimeUnit.MILLISECONDS

class AggregateFluentSpecification extends Specification {

    def codecs = [new ValueCodecProvider(),
                  new DocumentCodecProvider(),
                  new DBObjectCodecProvider(),
                  new BsonValueCodecProvider()]
    def options = MongoCollectionOptions.builder()
                                        .codecRegistry(new RootCodecRegistry(codecs))
                                        .readPreference(ReadPreference.secondary())
                                        .build()

    def 'should build the expected pipeline'() {
        given:
        def executor = new TestOperationExecutor([null, null]);
        def pipeline = [new Document('$match', new Document('match', 0))]
        def aggregateOptions = new AggregateOptions().allowDiskUse(true).batchSize(100).maxTime(10, MILLISECONDS)
        def fluentAggregator = new AggregateFluentImpl<Document>(new MongoNamespace('db', 'coll'), options, executor,
                                                                 pipeline, aggregateOptions, Document)

        when:
        fluentAggregator.geoNear(new Document('geoNear', 1))
                        .group(new Document('group', 1))
                        .match(new Document('match', 1))
                        .sort(new Document('sort', 1))
                        .project(new Document('project', 1))
                        .redact(new Document('redact', 1))
                        .limit(100)
                        .skip(10)
                        .unwind('unwind')
                        .out('out')
                        .iterator()

        def operation = executor.getWriteOperation() as AggregateToCollectionOperation
        def findOperation = executor.getReadOperation() as FindOperation<Document>
        def readPreference = executor.getReadPreference()

        then:
        operation.pipeline == [new BsonDocument('$match', new BsonDocument('match', new BsonInt32(0))),
                               new BsonDocument('$geoNear', new BsonDocument('geoNear', new BsonInt32(1))),
                               new BsonDocument('$group', new BsonDocument('group', new BsonInt32(1))),
                               new BsonDocument('$match', new BsonDocument('match', new BsonInt32(1))),
                               new BsonDocument('$sort', new BsonDocument('sort', new BsonInt32(1))),
                               new BsonDocument('$project', new BsonDocument('project', new BsonInt32(1))),
                               new BsonDocument('$redact', new BsonDocument('redact', new BsonInt32(1))),
                               new BsonDocument('$limit', new BsonInt32(100)),
                               new BsonDocument('$skip', new BsonInt32(10)),
                               new BsonDocument('$unwind', new BsonString('unwind')),
                               new BsonDocument('$out', new BsonString('out'))
        ]
        operation.getMaxTime(MILLISECONDS) == aggregateOptions.getMaxTime(MILLISECONDS)
        operation.getAllowDiskUse()

        findOperation.getBatchSize() == 100
        findOperation.getMaxTime(MILLISECONDS) == aggregateOptions.getMaxTime(MILLISECONDS)

        readPreference == options.getReadPreference()
    }

    def 'should handle mixed types in the pipeline'() {
        given:
        def executor = new TestOperationExecutor([null, null]);
        def fluentAggregator = new AggregateFluentImpl<Document>(new MongoNamespace('db', 'coll'), options, executor,
                                                                 [], new AggregateOptions(), Document)
        when:
        fluentAggregator.match(new Document('match', 0))
                        .match(new BasicDBObject('match', 1))
                        .match(new BsonDocument('match', new BsonInt32(2)))
                        .iterator()


        def operation = executor.getReadOperation() as AggregateOperation<Document>

        then:
        operation.pipeline == [new BsonDocument('$match', new BsonDocument('match', new BsonInt32(0))),
                               new BsonDocument('$match', new BsonDocument('match', new BsonInt32(1))),
                               new BsonDocument('$match', new BsonDocument('match', new BsonInt32(2)))
        ]
    }

    def 'should give a nice error message if there is no codec to encode a value'() {
        given:
        def executor = new TestOperationExecutor([null, null]);
        def fluentAggregator = new AggregateFluentImpl<Document>(new MongoNamespace('db', 'coll'), options, executor,
                                                                 [], new AggregateOptions(), Document)
        def worker = new Worker('Pete', 'DBA', new Date(), 1);

        when:
        fluentAggregator.match(new Document('match', 0))
                        .match(worker)
                        .iterator()


        then:
        def exception = thrown(CodecConfigurationException)
        exception.getMessage().contains(worker.toString())
    }
}
