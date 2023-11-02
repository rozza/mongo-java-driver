/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.operation

import com.mongodb.MongoCommandException
import com.mongodb.MongoException
import com.mongodb.MongoNamespace
import com.mongodb.ServerAddress
import com.mongodb.ServerCursor
import com.mongodb.async.FutureResultCallback
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerConnectionState
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerType
import com.mongodb.connection.ServerVersion
import com.mongodb.internal.async.SingleResultCallback
import com.mongodb.internal.binding.AsyncConnectionSource
import com.mongodb.internal.connection.AsyncConnection
import com.mongodb.internal.connection.QueryResult
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodec
import spock.lang.Specification

import static OperationUnitSpecification.getMaxWireVersionForServerVersion
import static com.mongodb.ReadPreference.primary

class AsyncQueryBatchCursorSpecification extends Specification {

    def 'should generate expected command with batchSize and maxTimeMS'() {
        given:
        def connection = referenceCountedAsyncConnection()
        def connectionSource = getAsyncConnectionSource(connection)

        def firstBatch = new QueryResult(NAMESPACE, [], 42, SERVER_ADDRESS)
        def cursor = new AsyncQueryBatchCursor<Document>(firstBatch, 0, batchSize, maxTimeMS, CODEC, null, connectionSource,
                connection)
        def expectedCommand = new BsonDocument('getMore': new BsonInt64(CURSOR_ID))
                .append('collection', new BsonString(NAMESPACE.getCollectionName()))
        if (batchSize != 0) {
            expectedCommand.append('batchSize', new BsonInt32(batchSize))
        }
        if (expectedMaxTimeFieldValue != null) {
            expectedCommand.append('maxTimeMS', new BsonInt64(expectedMaxTimeFieldValue))
        }

        def reply =  documentResponse([], 0)

        when:
        def batch = nextBatch(cursor)

        then:
        1 * connection.commandAsync(NAMESPACE.getDatabaseName(), expectedCommand, *_) >> {
            it.last().onResult(reply, null)
        }
        batch == null

        then:
        !cursor.isClosed()

        then:
        cursor.close()

        then:
        connection.getCount() == 0
        connectionSource.getCount() == 0

        where:
        batchSize | maxTimeMS | expectedMaxTimeFieldValue
        0         | 0         | null
        2         | 0         | null
        0         | 100       | 100
    }

    def 'should close the cursor'() {
        given:
        def serverVersion = new ServerVersion([3, 6, 0])
        def connection = referenceCountedAsyncConnection(serverVersion)
        def connectionSource = getAsyncConnectionSource(connection)

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(firstBatch, 0, 0, 0, CODEC, null, connectionSource, connection)
        cursor.close()

        then:
        if (firstBatch.getCursor() != null) {
            1 * connection.commandAsync(NAMESPACE.databaseName, createKillCursorsDocument(firstBatch.cursor), _, primary(), *_) >> {
                it.last().onResult(null, null)
            }
        }

        then:
        connection.getCount() == 0
        connectionSource.getCount() == 0

        where:
        firstBatch << [queryResult(), queryResult(FIRST_BATCH, 0)]
    }

    def 'should return the expected results from next'() {
        given:
        def connectionSource = getAsyncConnectionSource(referenceCountedAsyncConnection())

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(queryResult(FIRST_BATCH, 0), 0, 0, 0, CODEC, null, connectionSource, null)

        then:
        nextBatch(cursor) == FIRST_BATCH

        then:
        connectionSource.getCount() == 0

        then:
        cursor.isClosed()

        when:
        nextBatch(cursor)

        then:
        def exception = thrown(MongoException)
        exception.getMessage() == 'next() called after the cursor was closed.'
    }

    def 'should respect the limit'() {
        given:
        def serverVersion = new ServerVersion([3, 6, 0])
        def connectionA = referenceCountedAsyncConnection(serverVersion)
        def connectionB = referenceCountedAsyncConnection(serverVersion)
        def connectionSource = getAsyncConnectionSource(connectionA, connectionB)

        def firstBatch = [new Document('_id', 1), new Document('_id', 2), new Document('_id', 3)]
        def secondBatch = [new Document('_id', 4), new Document('_id', 5)]
        def thirdBatch = [new Document('_id', 6)]

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(queryResult(firstBatch), 6, 2, 0, CODEC, null, connectionSource,
                connectionA)
        def batch = nextBatch(cursor)

        then:
        batch == firstBatch

        when:
        batch = nextBatch(cursor)

        then:
        1 * connectionA.commandAsync(*_) >> { it.last().onResult(documentResponse(secondBatch), null) }

        then:
        batch == secondBatch
        connectionA.getCount() == 0
        connectionSource.getCount() == 1

        when:
        batch = nextBatch(cursor)

        then:
        1 * connectionB.commandAsync(*_) >> {
            connectionB.getCount() == 1
            connectionSource.getCount() == 1
                it.last().onResult(documentResponse(thirdBatch, 0), null)
        }

        then:
        batch == thirdBatch
        connectionB.getCount() == 0
        connectionSource.getCount() == 0

        when:
        batch = nextBatch(cursor)

        then:
        batch == null
        connectionSource.getCount() == 0
    }


    def 'should close the cursor immediately if the limit has been reached'() {
        given:
        def serverVersion = new ServerVersion([3, 6, 0])
        def connection = referenceCountedAsyncConnection(serverVersion)
        def connectionSource = getAsyncConnectionSource(connection)
        def queryResult = queryResult()

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(queryResult, 1, 0, 0, CODEC, null, connectionSource, connection)

        then:
        1 * connection.commandAsync(NAMESPACE.databaseName, createKillCursorsDocument(queryResult.cursor), _, primary(),
                    *_) >> {
                it.last().onResult(null, null)
        }

        when:
        cursor.close()

        then:
        0 * connection.commandAsync(_, _, _, _, _)

        then:
        connection.getCount() == 0
        connectionSource.getCount() == 0
    }

    def 'should handle getMore when there are empty results but there is a cursor'() {
        given:
        def connection = referenceCountedAsyncConnection(serverVersion)
        def connectionSource = getAsyncConnectionSource(connection)

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(queryResult([], 42), 3, 0, 0, CODEC, null, connectionSource, connection)
        def batch = nextBatch(cursor)

        then:
        1 * connection.commandAsync(*_) >> {
            connection.getCount() == 1
            connectionSource.getCount() == 1
                it.last().onResult(response, null)
        }

        1 * connection.commandAsync(*_) >> {
            connection.getCount() == 1
            connectionSource.getCount() == 1
                it.last().onResult(response2, null)
        }

        then:
        batch == SECOND_BATCH

        then:
        connection.getCount() == 0
        connectionSource.getCount() == 0

        when:
        cursor.close()

        then:
        0 * connection._
        connectionSource.getCount() == 0

        where:
        serverVersion                | response              | response2
        new ServerVersion([3, 6, 0]) | documentResponse([])  | documentResponse(SECOND_BATCH, 0)
    }

    def 'should kill the cursor in the getMore if limit is reached'() {
        given:
        def connection = referenceCountedAsyncConnection(serverVersion)
        def connectionSource = getAsyncConnectionSource(connection)
        def initialResult = queryResult()

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(initialResult, 3, 0, 0, CODEC, null, connectionSource, connection)
        def batch = nextBatch(cursor)

        then:
        batch == FIRST_BATCH

        when:
        nextBatch(cursor)

        then:
        1 * connection.commandAsync(*_) >> {
            it.last().onResult(response, null)
        }
        1 * connection.commandAsync(NAMESPACE.databaseName, createKillCursorsDocument(initialResult.cursor), _, primary(), _,
                connectionSource.operationContext, *_) >> {
                it.last().onResult(null, null)
        }

        then:
        connection.getCount() == 0
        connectionSource.getCount() == 0

        when:
        cursor.close()

        then:
        0 * connection.commandAsync(*_)
        connectionSource.getCount() == 0

        where:
        serverVersion                | response
        new ServerVersion([3, 2, 0]) | documentResponse(SECOND_BATCH)
    }

    def 'should close cursor after getMore finishes if cursor was closed while getMore was in progress and getMore returns a response'() {
        given:
        def serverVersion =  new ServerVersion([3, 6, 0])
        def connectionA = referenceCountedAsyncConnection(serverVersion)
        def connectionB = referenceCountedAsyncConnection(serverVersion)
        def connectionSource = getAsyncConnectionSource(serverType, connectionA, connectionB)
        def initialResult = queryResult()

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(initialResult, 0, 0, 0, CODEC, null, connectionSource, connectionA)
        def batch = nextBatch(cursor)

        then:
        batch == FIRST_BATCH

        when:
        nextBatch(cursor)

        then:
        numberOfInvocations * connectionA.commandAsync(*_) >> {
            // Simulate the user calling close while the getMore is in flight
            cursor.close()
                ((SingleResultCallback<?>) it.last()).onResult(response, null)
        } >> {
                // `killCursors` command
                ((SingleResultCallback<?>) it.last()).onResult(response2, null)
        }

        then:
        noExceptionThrown()

        then:
        connectionA.getCount() == 0
        cursor.isClosed()

        where:
        response                | response2                | getMoreResponseHasCursor | serverType                | numberOfInvocations
        documentResponse([])    | documentResponse([], 0)  | true                     | ServerType.LOAD_BALANCER  | 2
        documentResponse([], 0) | null                     | false                    | ServerType.LOAD_BALANCER  | 1
        documentResponse([])    | documentResponse([], 0)  | true                     | ServerType.STANDALONE     | 1
        documentResponse([], 0) | null                     | false                    | ServerType.STANDALONE     | 1
    }

    def 'should close cursor after getMore finishes if cursor was closed while getMore was in progress and getMore throws exception'() {
        given:
        def serverVersion = new ServerVersion([3, 2, 0])
        def connectionA = referenceCountedAsyncConnection(serverVersion)
        def connectionB = referenceCountedAsyncConnection(serverVersion)
        def connectionSource = getAsyncConnectionSource(serverType, connectionA, connectionB)
        def initialResult = queryResult()

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(initialResult, 0, 0, 0, CODEC, null, connectionSource, connectionA)
        def batch = nextBatch(cursor)

        then:
        batch == FIRST_BATCH

        when:
        nextBatch(cursor)

        then:
        numberOfInvocations * connectionA.commandAsync(*_) >> {
            // Simulate the user calling close while the getMore is throwing a MongoException
            cursor.close()
                ((SingleResultCallback<?>) it.last()).onResult(null, MONGO_EXCEPTION)
            } >> {
                // `killCursors` command
                ((SingleResultCallback<?>) it.last()).onResult(null, null)
        }

        then:
        thrown(MongoException)

        then:
        connectionA.getCount() == 0
        cursor.isClosed()

        where:
        serverType               | numberOfInvocations
        ServerType.LOAD_BALANCER | 2
        ServerType.STANDALONE    | 1
    }

    def 'should handle errors when calling close'() {
        given:
        def connection = referenceCountedAsyncConnection()
        def connectionSource = getAsyncConnectionSourceWithResult(ServerType.STANDALONE) { [null, MONGO_EXCEPTION] }
        def cursor = new AsyncQueryBatchCursor<Document>(queryResult(), 0, 0, 0, CODEC, null, connectionSource, connection)

        when:
        cursor.close()
        nextBatch(cursor)

        then:
        def exception = thrown(MongoException)
        exception.getMessage() == 'next() called after the cursor was closed.'

        then:
        cursor.isClosed()
        connectionSource.getCount() == 0
    }


    def 'should handle errors when getting a connection for getMore'() {
        given:
        def connection = referenceCountedAsyncConnection()
        def connectionSource = getAsyncConnectionSourceWithResult(ServerType.STANDALONE) { [null, MONGO_EXCEPTION] }

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(queryResult(), 0, 0, 0, CODEC, null, connectionSource, connection)

        then:
        nextBatch(cursor)

        when:
        nextBatch(cursor)

        then:
        thrown(MongoException)

        then:
        connectionSource.getCount() == 1

        when:
        cursor.close()

        then:
        connectionSource.getCount() == 0
    }

    def 'should handle errors when calling getMore'() {
        given:
        def serverVersion = new ServerVersion([3, 6, 0])
        def connectionA = referenceCountedAsyncConnection(serverVersion)
        def connectionB = referenceCountedAsyncConnection(serverVersion)
        def connectionSource = getAsyncConnectionSource(connectionA, connectionB)

        when:
        def cursor = new AsyncQueryBatchCursor<Document>(queryResult([]), 0, 0, 0, CODEC, null, connectionSource,
                connectionA)

        then:
        connectionSource.getCount() == 1

        when:
        nextBatch(cursor)

        then:
        1 * connectionA.commandAsync(*_) >> {
            connectionA.getCount() == 1
            connectionSource.getCount() == 1
                it.last().onResult(null, exception)
        }

        then:
        thrown(MongoException)

        then:
        connectionA.getCount() == 0
        connectionSource.getCount() == 1

        when:
        cursor.close()

        then:
        connectionSource.getCount() == 1

        where:
        exception << [COMMAND_EXCEPTION, MONGO_EXCEPTION]
    }

    List<Document> nextBatch(AsyncQueryBatchCursor cursor) {
        def futureResultCallback = new FutureResultCallback()
        cursor.next(futureResultCallback)
        futureResultCallback.get()
    }

    private static final MongoNamespace NAMESPACE = new MongoNamespace('db', 'coll')
    private static final ServerAddress SERVER_ADDRESS = new ServerAddress()
    private static final CURSOR_ID = 42
    private static final FIRST_BATCH = [new Document('_id', 1), new Document('_id', 2)]
    private static final SECOND_BATCH = [new Document('_id', 3), new Document('_id', 4)]
    private static final CODEC = new DocumentCodec()
    private static final MONGO_EXCEPTION = new MongoException('error')
    private static final COMMAND_EXCEPTION = new MongoCommandException(BsonDocument.parse('{"ok": false, "errmsg": "error"}'),
            SERVER_ADDRESS)

    private static BsonDocument documentResponse(results, cursorId = 42) {
        new BsonDocument('ok', new BsonInt32(1)).append('cursor',
                new BsonDocument('id', new BsonInt64(cursorId)).append('ns',
                        new BsonString(NAMESPACE.getFullName()))
                        .append('nextBatch', new BsonArrayWrapper(results)))
    }

    private static QueryResult<?> queryResult(results = FIRST_BATCH, cursorId = 42) {
        new QueryResult(NAMESPACE, results, cursorId, SERVER_ADDRESS)
    }

    def referenceCountedAsyncConnection() {
        referenceCountedAsyncConnection(new ServerVersion([3, 2, 0]))
    }

    def referenceCountedAsyncConnection(ServerVersion serverVersion) {
        def released = false
        def counter = 0
        def mock = Mock(AsyncConnection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> getMaxWireVersionForServerVersion(serverVersion.getVersionList())
            }
        }
        mock.retain() >> {
            if (released) {
                throw new IllegalStateException('Tried to retain AsyncConnection when already released')
            } else {
                counter += 1
            }
            mock
        }
        mock.release() >> {
            counter -= 1
            if (counter == 0) {
                released = true
            } else if (counter < 0) {
                throw new IllegalStateException('Tried to release AsyncConnection below 0')
            }
            counter
        }
        mock.getCount() >> { counter }
        mock
    }

    AsyncConnectionSource getAsyncConnectionSource(AsyncConnection... connections) {
        getAsyncConnectionSource(ServerType.STANDALONE, connections)
    }

    AsyncConnectionSource getAsyncConnectionSource(ServerType serverType, AsyncConnection... connections) {
        def index = -1
        getAsyncConnectionSourceWithResult(serverType) { index += 1; [connections.toList().get(index).retain(), null] }
    }

    def getAsyncConnectionSourceWithResult(ServerType serverType, Closure<?> connectionCallbackResults) {
        def released = false
        int counter = 0
        def mock = Mock(AsyncConnectionSource)
        mock.getServerDescription() >> {
            ServerDescription.builder()
                    .address(new ServerAddress())
                    .type(serverType)
                    .state(ServerConnectionState.CONNECTED)
                    .build()
        }
        mock.getConnection(_) >> {
            if (counter == 0) {
                throw new IllegalStateException('Tried to use released AsyncConnectionSource')
            }
            def (result, error) = connectionCallbackResults()
            it[0].onResult(result, error)
        }
        mock.retain() >> {
            if (released) {
                throw new IllegalStateException('Tried to retain AsyncConnectionSource when already released')
            } else {
                counter += 1
            }
            mock
        }
        mock.release() >> {
            counter -= 1
            if (counter == 0) {
                released = true
            } else if (counter < 0) {
                throw new IllegalStateException('Tried to release AsyncConnectionSource below 0')
            }
            counter
        }
        mock.getCount() >> { counter }
        mock
    }

    BsonDocument createKillCursorsDocument(ServerCursor serverCursor) {
        new BsonDocument('killCursors', new BsonString(NAMESPACE.getCollectionName()))
                .append('cursors', new BsonArray(Collections.singletonList(new BsonInt64(serverCursor.id))))
    }

}
