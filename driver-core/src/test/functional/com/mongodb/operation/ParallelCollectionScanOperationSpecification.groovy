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
import category.Slow
import com.mongodb.Block
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
import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.Decoder
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import java.util.concurrent.ConcurrentHashMap

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.loopCursor
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.Arrays.asList
import static org.junit.Assert.assertTrue

@IgnoreIf({ isSharded() || !serverVersionAtLeast(asList(2, 6, 0)) })
@Category(Slow)
class ParallelCollectionScanOperationSpecification extends OperationFunctionalSpecification {
    Map<Integer, Boolean> ids = [] as ConcurrentHashMap

    def 'setup'() {
        (1..2000).each {
            ids.put(it, true)
            getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('_id', it))
        }
    }

    def 'should visit all documents'() {
        when:
        def cursors = new ParallelCollectionScanOperation<Document>(getNamespace(), 3, new DocumentCodec())
                .batchSize(500).execute(getBinding())

        then:
        cursors.size() <= 3

        when:
        cursors.each { batchCursor -> batchCursor.each { cursor -> cursor.each { doc -> ids.remove(doc.getInteger('_id')) } } }

        then:
        ids.isEmpty()
    }

    @Category(Async)
    def 'should visit all documents asynchronously'() {
        when:
        def cursors = executeAsync(new ParallelCollectionScanOperation<Document>(getNamespace(), 3, new DocumentCodec()).batchSize(500))

        then:
        cursors.size() <= 3

        when:
        loopCursor(cursors, new Block<Document>() {
            @Override
            void apply(final Document document) {
                assertTrue(ids.remove((Integer) document.get('_id')))
            }
        })

        then:
        ids.isEmpty()
    }

    def 'should use the ReadBindings readPreference to set slaveOK'() {
        given:
        def dbName = 'db'
        def collectionName = 'coll'
        def namespace = new MongoNamespace(dbName, collectionName)
        def decoder = Stub(Decoder)
        def readBinding = Mock(ReadBinding)
        def readPreference = Mock(ReadPreference)
        def connectionSource = Mock(ConnectionSource)
        def connection = Mock(Connection)
        def connectionDescription = Mock(ConnectionDescription)
        def commandResult = BsonDocument.parse('{ok: 1.0, cursors: []}')
        def operation = new ParallelCollectionScanOperation<Document>(namespace, 2, decoder)

        when:
        operation.execute(readBinding)

        then:
        1 * readBinding.getReadConnectionSource() >> connectionSource
        1 * readBinding.getReadPreference() >> readPreference
        1 * connectionSource.getConnection() >> connection
        1 * connection.getDescription() >> connectionDescription
        1 * connectionDescription.getServerType()
        1 * readPreference.slaveOk >> slaveOk
        1 * connection.command(_, _, slaveOk, _, _) >> commandResult
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
        def decoder = Stub(Decoder)
        def readBinding = Mock(AsyncReadBinding)
        def readPreference = Mock(ReadPreference)
        def connectionSource = Mock(AsyncConnectionSource)
        def connection = Mock(AsyncConnection)
        def connectionDescription = Mock(ConnectionDescription)
        def commandResult = BsonDocument.parse('{ok: 1.0, cursors: []}')
        def operation = new ParallelCollectionScanOperation<Document>(namespace, 2, decoder)

        when:
        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        1 * readBinding.getReadPreference() >> readPreference
        1 * readBinding.getReadConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        1 * connectionSource.getConnection(_) >> { it[0].onResult(connection, null) }
        1 * connection.getDescription() >> connectionDescription
        1 * connectionDescription.getServerType()
        1 * readPreference.slaveOk >> slaveOk
        1 * connection.commandAsync(_, _, slaveOk, _, _, _) >> { it[6].onResult(commandResult, _) }

        where:
        slaveOk << [true, false]
    }
}
