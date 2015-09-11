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
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonJavaScript
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category

import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.loopCursor

class GroupOperationSpecification extends OperationFunctionalSpecification {

    def 'should be able to group by inferring from the reduce function'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        def result = new GroupOperation(getNamespace(),
                                        new BsonJavaScript('function ( curr, result ) { if (result.name.indexOf(curr.name) == -1) { ' +
                                                           'result.name.push(curr.name); }}'),
                                        new BsonDocument('name': new BsonArray()), new DocumentCodec())
                .execute(getBinding());

        then:
        result.next()[0].name == ['Pete', 'Sam']
    }

    def 'should be able to group by name'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        def result = new GroupOperation(getNamespace(),
                                        new BsonJavaScript('function ( curr, result ) {}'),
                                        new BsonDocument(), new DocumentCodec())
                .key(new BsonDocument('name', new BsonInt32(1)))
                .execute(getBinding());

        then:
        List<String> results = result.iterator().next()*.getString('name')
        results.containsAll(['Pete', 'Sam'])
    }

    def 'should be able to group by key function'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        def result = new GroupOperation(getNamespace(),
                                        new BsonJavaScript('function ( curr, result ) { }'),
                                        new BsonDocument(), new DocumentCodec())
                .keyFunction(new BsonJavaScript('function(doc){ return {name: doc.name}; }'))
                .execute(getBinding());

        then:
        List<String> results = result.iterator().next()*.getString('name')
        results.containsAll(['Pete', 'Sam'])
    }


    def 'should be able to group with filter'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        def result = new GroupOperation(getNamespace(),
                                        new BsonJavaScript('function ( curr, result ) { }'),
                                        new BsonDocument(), new DocumentCodec())
                .key(new BsonDocument('name', new BsonInt32(1)))
                .filter(new BsonDocument('name': new BsonString('Pete')))
                .execute(getBinding());

        then:
        List<String> results = result.iterator().next()*.getString('name')
        results == ['Pete']
    }

    @Category(Async)
    def 'should be able to group by name asynchronously'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam, pete2)

        when:
        def operation = new GroupOperation(getNamespace(),
                                        new BsonJavaScript('function ( curr, result ) {}'),
                                        new BsonDocument(), new DocumentCodec())
                .key(new BsonDocument('name', new BsonInt32(1)))

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
        docList.iterator()*.getString('name') containsAll(['Pete', 'Sam'])
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
        def commmandResult = BsonDocument.parse('{ok: 1.0}').append('retval', new BsonArrayWrapper([]))
        def operation = new GroupOperation(namespace, new BsonJavaScript('function ( curr, result ) { }'), new BsonDocument(),
                new DocumentCodec()).key(BsonDocument.parse('{name: 1}'))

        when:
        operation.execute(readBinding)

        then:
        1 * readBinding.getReadConnectionSource() >> connectionSource
        1 * readBinding.getReadPreference() >> readPreference
        1 * connectionSource.getConnection() >> connection
        1 * connection.getDescription() >> connectionDescription
        1 * readPreference.slaveOk >> slaveOk
        1 * connection.command(dbName, _, slaveOk, _, _) >> commmandResult
        1 * connection.getDescription() >> connectionDescription
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
        def commmandResult = BsonDocument.parse('{ok: 1.0}').append('retval', new BsonArrayWrapper([]))
        def operation = new GroupOperation(namespace, new BsonJavaScript('function ( curr, result ) { }'), new BsonDocument(),
                new DocumentCodec()).key(BsonDocument.parse('{name: 1}'))

        when:
        operation.executeAsync(readBinding, Stub(SingleResultCallback))

        then:
        1 * readBinding.getReadPreference() >> readPreference
        2 * connection.getDescription() >> connectionDescription
        1 * readBinding.getReadConnectionSource(_) >> { it[0].onResult(connectionSource, null) }
        1 * connectionSource.getConnection(_) >> { it[0].onResult(connection, null) }
        1 * readPreference.slaveOk >> slaveOk
        1 * connection.commandAsync(dbName, _, slaveOk, _, _, _)  >> { it[5].onResult(commmandResult, null) }
        1 * connection.release()
        1 * connectionSource.release()

        where:
        slaveOk << [true, false]
    }
}
