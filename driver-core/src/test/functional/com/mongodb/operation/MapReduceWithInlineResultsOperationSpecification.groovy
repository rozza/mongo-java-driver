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
import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadPreference
import com.mongodb.async.SingleResultCallback
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.binding.AsyncReadBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.ReadBinding
import com.mongodb.client.test.CollectionHelper
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import org.bson.BsonDocument
import org.bson.BsonJavaScript
import org.bson.Document
import org.bson.codecs.Decoder
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category

import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.loopCursor

class MapReduceWithInlineResultsOperationSpecification extends OperationFunctionalSpecification {
    private final documentCodec = new DocumentCodec()
    def mapReduceOperation = new MapReduceWithInlineResultsOperation<Document>(
            getNamespace(),
            new BsonJavaScript('function(){ emit( this.name , 1 ); }'),
            new BsonJavaScript('function(key, values){ return values.length; }'),
            documentCodec)

    def expectedResults = [['_id': 'Pete', 'value': 2.0] as Document,
                           ['_id': 'Sam', 'value': 1.0] as Document]

    def setup() {
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        helper.insertDocuments(new DocumentCodec(), pete, sam, pete2)
    }


    def 'should return the correct results'() {
        when:
        MapReduceBatchCursor<Document> results = mapReduceOperation.execute(getBinding())

        then:
        results.iterator().next() == expectedResults
    }

    @Category(Async)
    def 'should return the correct results asynchronously'() {
        when:
        List<Document> docList = []
        loopCursor(mapReduceOperation, new Block<Document>() {
            @Override
            void apply(final Document value) {
                if (value != null) {
                    docList += value
                }
            }
        });

        then:
        docList.iterator().toList() == expectedResults
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
        def commandResult = BsonDocument.parse('{ok: 1.0, counts: {input: 1, emit: 1, output: 1}, timeMillis: 1}')
                .append('results', new BsonArrayWrapper([]))
        def operation = new MapReduceWithInlineResultsOperation<Document>( namespace, new BsonJavaScript('function(){ }'),
                new BsonJavaScript('function(key, values){ }'), decoder)

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
        def dbName = 'db'
        def collectionName = 'coll'
        def namespace = new MongoNamespace(dbName, collectionName)
        def decoder = Stub(Decoder)
        def readBinding = Mock(AsyncReadBinding)
        def readPreference = Mock(ReadPreference)
        def connectionSource = Mock(AsyncConnectionSource)
        def connection = Mock(AsyncConnection)
        def connectionDescription = Mock(ConnectionDescription)
        def commandResult = BsonDocument.parse('{ok: 1.0, counts: {input: 1, emit: 1, output: 1}, timeMillis: 1}')
                .append('results', new BsonArrayWrapper([]))
        def operation = new MapReduceWithInlineResultsOperation<Document>( namespace, new BsonJavaScript('function(){ }'),
                new BsonJavaScript('function(key, values){ }'), decoder)

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
