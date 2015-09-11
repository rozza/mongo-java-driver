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
import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadPreference
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.binding.AsyncReadBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.ReadBinding
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.Document
import org.bson.codecs.Decoder
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.concurrent.TimeUnit.MILLISECONDS

class ListDatabasesOperationSpecification extends OperationFunctionalSpecification {
    def codec = new DocumentCodec()

    def 'should return a list of database names'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('_id', 1))
        def operation = new ListDatabasesOperation(codec)

        when:
        def names = operation.execute(getBinding()).next()*.get('name')


        then:
        names.contains(getDatabaseName())
    }

    @Category(Async)
    def 'should return a list of database names asynchronously'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('_id', 1))
        def operation = new ListDatabasesOperation(codec)

        when:
        def callback = new FutureResultCallback()
        def cursor = executeAsync(operation)
        cursor.next(callback)
        def names = callback.get()*.get('name')

        then:
        names.contains(getDatabaseName())
    }

    @IgnoreIf({ isSharded() || !serverVersionAtLeast([2, 6, 0]) })
    def 'should throw execution timeout exception from execute'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document())
        def operation = new ListDatabasesOperation(codec).maxTime(1000, MILLISECONDS)

        enableMaxTimeFailPoint()

        when:
        operation.execute(getBinding())

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    @Category(Async)
    @IgnoreIf({ isSharded() || !serverVersionAtLeast([2, 6, 0]) })
    def 'should throw execution timeout exception from executeAsync'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document())
        def operation = new ListDatabasesOperation(codec).maxTime(1000, MILLISECONDS)

        enableMaxTimeFailPoint()

        when:
        executeAsync(operation);

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    def 'should use the ReadBindings readPreference to set slaveOK'() {
        given:
        def decoder = Stub(Decoder)
        def readBinding = Mock(ReadBinding)
        def readPreference = Mock(ReadPreference)
        def connectionSource = Mock(ConnectionSource)
        def connection = Mock(Connection)
        def connectionDescription = Mock(ConnectionDescription)
        def commandResult = new BsonDocument('ok', new BsonDouble(1.0)).append('databases', new BsonArrayWrapper([]))
        def operation = new ListDatabasesOperation(decoder)

        when:
        operation.execute(readBinding)

        then:
        1 * readBinding.getReadConnectionSource() >> connectionSource
        1 * readBinding.getReadPreference() >> readPreference
        1 * connectionSource.getConnection() >> connection
        2 * connection.getDescription() >> connectionDescription
        1 * readPreference.slaveOk >> slaveOk
        1 * connection.command(_, _, slaveOk, _, _) >> commandResult
        1 * connectionSource.retain()
        1 * connectionDescription.getServerType()
        1 * connectionDescription.getServerAddress()
        1 * connection.release()
        1 * connectionSource.release()

        where:
        slaveOk << [true, false]
    }

    def 'should use the AsyncReadBindings readPreference to set slaveOK'() {
        given:
        def decoder = Stub(Decoder)
        def readBinding = Mock(AsyncReadBinding)
        def readPreference = Mock(ReadPreference)
        def connectionSource = Mock(AsyncConnectionSource)
        def connection = Mock(AsyncConnection)
        def connectionDescription = Mock(ConnectionDescription)
        def commandResult = new BsonDocument('ok', new BsonDouble(1.0)).append('databases', new BsonArrayWrapper([]))
        def operation = new ListDatabasesOperation(decoder)

        when:
        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        1 * readBinding.getReadPreference() >> readPreference
        1 * readBinding.getReadConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        1 * connectionSource.getConnection(_) >> { it[0].onResult(connection, null) }
        1 * readPreference.slaveOk >> slaveOk
        1 * connection.getDescription() >> connectionDescription
        1 * connectionDescription.getServerType()
        1 * connection.commandAsync(_, _, slaveOk, _, _, _) >> { it[6].onResult(commandResult, _) }

        where:
        slaveOk << [true, false]
    }

}
