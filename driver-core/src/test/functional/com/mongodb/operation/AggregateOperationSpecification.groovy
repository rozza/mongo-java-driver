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
import com.mongodb.Block
import com.mongodb.ExplainVerbosity
import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadPreference
import com.mongodb.async.SingleResultCallback
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.binding.AsyncReadBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.ReadBinding
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerVersion
import org.bson.BsonDocument
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.loopCursor
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

class AggregateOperationSpecification extends OperationFunctionalSpecification {

    def setup() {
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)
    }

    def 'should have the correct defaults'() {
        when:
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())

        then:
        operation.getAllowDiskUse() == null
        operation.getBatchSize() == null
        operation.getMaxTime(MILLISECONDS) == 0
        operation.getPipeline() == []
        operation.getUseCursor() == null
    }

    def 'should set optional values correctly'(){
        when:
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec())
                .allowDiskUse(true)
                .batchSize(10)
                .maxTime(10, MILLISECONDS)
                .useCursor(true)


        then:
        operation.getAllowDiskUse()
        operation.getBatchSize() == 10
        operation.getMaxTime(MILLISECONDS) == 10
        operation.getUseCursor()
    }

    def 'should be able to aggregate'() {
        when:
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec()).useCursor(useCursor)
        def result = operation.execute(getBinding());

        then:
        List<String> results = result.iterator().next()*.getString('name')
        results.size() == 3
        results.containsAll(['Pete', 'Sam'])

        where:
        useCursor << useCursorOptions()
    }

    @Category(Async)
    def 'should be able to aggregate asynchronously'() {
        when:
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec()).useCursor(useCursor)
        List<Document> docList = []
        loopCursor(operation, new Block<Document>() {
            @Override
            void apply(final Document value) {
                if (value != null) {
                    docList += value
                }
            }
        });

        then:
        List<String> results = docList.iterator()*.getString('name')
        results.size() == 3
        results.containsAll(['Pete', 'Sam'])

        where:
        useCursor << useCursorOptions()
    }

    def 'should be able to aggregate with pipeline'() {
        when:
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(),
                                                                 [new BsonDocument('$match',
                                                                                   new BsonDocument('job', new BsonString('plumber')))],
                                                                 new DocumentCodec()).useCursor(useCursor)
        def result = operation.execute(getBinding());

        then:
        List<String> results = result.iterator().next()*.getString('name')
        results.size() == 1
        results == ['Sam']

        where:
        useCursor << useCursorOptions()
    }

    @Category(Async)
    def 'should be able to aggregate with pipeline asynchronously'() {
        when:
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(),
                                                                 [new BsonDocument('$match',
                                                                                   new BsonDocument('job', new BsonString('plumber')))],
                                                                 new DocumentCodec()).useCursor(useCursor)
        List<Document> docList = []
        loopCursor(operation, new Block<Document>() {
            @Override
            void apply(final Document value) {
                if (value != null) {
                    docList += value
                }
            }
        });

        then:
        List<String> results = docList.iterator()*.getString('name')
        results.size() == 1
        results == ['Sam']

        where:
        useCursor << useCursorOptions()
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should allow disk usage'() {
        when:
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec()).allowDiskUse(allowDiskUse)
        def cursor = operation.execute(getBinding())

        then:
        cursor.next()*.getString('name') == ['Pete', 'Sam', 'Pete']

        where:
        allowDiskUse << [null, true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should allow batch size'() {
        when:
        AggregateOperation operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec()).batchSize(batchSize)
        def cursor = operation.execute(getBinding())

        then:
        cursor.next()*.getString('name') == ['Pete', 'Sam', 'Pete']

        where:
        batchSize << [null, 0, 10]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should throw execution timeout exception from execute'() {
        given:
        def operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec()).maxTime(1, SECONDS)
        enableMaxTimeFailPoint()

        when:
        operation.execute(getBinding())

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    @Category(Async)
    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def 'should throw execution timeout exception from executeAsync'() {
        given:
        def operation = new AggregateOperation<Document>(getNamespace(), [], new DocumentCodec()).maxTime(1, SECONDS)
        enableMaxTimeFailPoint()

        when:
        executeAsync(operation.asExplainableOperationAsync(ExplainVerbosity.QUERY_PLANNER))

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    @IgnoreIf({ !serverVersionAtLeast([2, 6, 0]) })
    def 'should be able to explain an empty pipeline'() {
        given:
        AggregateOperation operation = new AggregateOperation(getNamespace(), [], new BsonDocumentCodec())

        when:
        def result = operation.asExplainableOperation(ExplainVerbosity.QUERY_PLANNER).execute(getBinding());

        then:
        result.containsKey('stages')
    }

    @Category(Async)
    @IgnoreIf({ !serverVersionAtLeast([2, 6, 0]) })
    def 'should be able to explain an empty pipeline asynchronously'() {
        given:
        AggregateOperation operation = new AggregateOperation(getNamespace(), [], new BsonDocumentCodec())

        when:
        def result = executeAsync(operation.asExplainableOperationAsync(ExplainVerbosity.QUERY_PLANNER));

        then:
        result.containsKey('stages')
    }

    def 'should use the ReadBindings readPreference to set slaveOK'() {
        given:
        def dbName = 'db'
        def collectionName = 'coll'
        def namespace = new MongoNamespace(dbName, collectionName)
        def readBinding = Mock(ReadBinding)
        def readPreference = Mock(ReadPreference)
        def connectionSource = Mock(ConnectionSource)
        def connection = Mock(Connection)
        def connectionDescription = Mock(ConnectionDescription)
        def inlineResult = BsonDocument.parse('{ok: 1.0}').append('result', new BsonArrayWrapper([]))
        def cursorResult = BsonDocument.parse('{ok: 1.0}')
                .append('cursor', new BsonDocument('id', new BsonInt64(0)).append('ns', new BsonString('db.coll'))
                .append('firstBatch', new BsonArrayWrapper([])))
        def operation = new AggregateOperation(namespace, [], new BsonDocumentCodec())

        when:
        operation.execute(readBinding)

        then:
        1 * readBinding.getReadConnectionSource() >> connectionSource
        1 * readBinding.getReadPreference() >> readPreference
        1 * connectionSource.getConnection() >> connection
        4 * connection.getDescription() >> connectionDescription
        3 * connectionDescription.getServerVersion() >> new ServerVersion([2, 4, 0])
        1 * readPreference.slaveOk >> slaveOk
        1 * connection.command(dbName, _, slaveOk, _, _) >> inlineResult
        1 * connection.release()
        1 * connectionSource.release()

        when: '2.6.0'
        operation.execute(readBinding)

        then:
        1 * readBinding.getReadConnectionSource() >> connectionSource
        1 * readBinding.getReadPreference() >> readPreference
        1 * connectionSource.getConnection() >> connection
        4 * connection.getDescription() >> connectionDescription
        3 * connectionDescription.getServerVersion() >> new ServerVersion([2, 6, 0])
        1 * readPreference.slaveOk >> slaveOk
        1 * connection.command(dbName, _, slaveOk, _, _) >> cursorResult
        1 * connection.release()
        1 * connectionSource.release()

        where:
        slaveOk << [true, false]
    }

    def 'should use the AsyncReadBindings readPreference to set slaveOK'() {
        given:
        def dbName = 'db'
        def collectionName = 'coll'
        def namespace = new MongoNamespace(dbName, collectionName)
        def readBinding = Mock(AsyncReadBinding)
        def readPreference = Mock(ReadPreference)
        def connectionSource = Mock(AsyncConnectionSource)
        def connection = Mock(AsyncConnection)
        def connectionDescription = Mock(ConnectionDescription)
        def inlineResult = BsonDocument.parse('{ok: 1.0}').append('result', new BsonArrayWrapper([]))
        def cursorResult = BsonDocument.parse('{ok: 1.0}')
                .append('cursor', new BsonDocument('id', new BsonInt64(0)).append('ns', new BsonString('db.coll'))
                .append('firstBatch', new BsonArrayWrapper([])))
        def operation = new AggregateOperation(namespace, [], new BsonDocumentCodec())

        when:
        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        1 * readBinding.getReadPreference() >> readPreference
        1 * readBinding.getReadConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        1 * connectionSource.getConnection(_) >> { it[0].onResult(connection, null) }
        4 * connection.getDescription() >> connectionDescription
        3 * connectionDescription.getServerVersion() >> new ServerVersion([2, 4, 0])
        1 * readPreference.slaveOk >> slaveOk
        1 * connection.commandAsync(dbName, _, slaveOk, _, _, _) >> { it[5].onResult(inlineResult, null) }
        1 * connection.release()
        1 * connectionSource.release()

        when: '2.6.0'
        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        1 * readBinding.getReadPreference() >> readPreference
        1 * readBinding.getReadConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        1 * connectionSource.getConnection(_) >> { it[0].onResult(connection, null) }
        4 * connection.getDescription() >> connectionDescription
        3 * connectionDescription.getServerVersion() >> new ServerVersion([2, 6, 0])
        1 * readPreference.slaveOk >> slaveOk
        1 * connection.commandAsync(dbName, _, slaveOk, _, _, _) >> { it[5].onResult(cursorResult, null) }
        1 * connection.release()
        1 * connectionSource.release()

        where:
        slaveOk << [true, false]
    }

    private static List<Boolean> useCursorOptions() {
        [null, true, false]
    }
}
