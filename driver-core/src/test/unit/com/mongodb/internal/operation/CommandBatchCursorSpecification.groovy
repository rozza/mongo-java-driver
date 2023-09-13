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

import com.mongodb.MongoException
import com.mongodb.MongoNamespace
import com.mongodb.MongoSocketException
import com.mongodb.MongoSocketOpenException
import com.mongodb.ServerAddress
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerConnectionState
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerType
import com.mongodb.connection.ServerVersion
import com.mongodb.internal.binding.ConnectionSource
import com.mongodb.internal.connection.Connection
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodec
import spock.lang.Specification

import static com.mongodb.internal.operation.OperationUnitSpecification.getMaxWireVersionForServerVersion

class CommandBatchCursorSpecification extends Specification {
    private static final MongoNamespace NAMESPACE = new MongoNamespace('db', 'coll')
    private static final ServerAddress SERVER_ADDRESS = new ServerAddress()

    def 'should generate expected command with batchSize and maxTimeMS'() {
        given:
        def connection = Mock(Connection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> 4
            }
        }
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> { connection }
            getServerApi() >> null
        }
        connectionSource.retain() >> connectionSource

        def cursorId = 42
        def initialResults = createCommandResult([], cursorId)
        def cursor = new CommandBatchCursor<Document>(SERVER_ADDRESS, initialResults, 0, batchSize, maxTimeMS, new DocumentCodec(),
                null, connectionSource, connection)
        def expectedCommand = new BsonDocument('getMore': new BsonInt64(cursorId))
                .append('collection', new BsonString(NAMESPACE.getCollectionName()))
        if (batchSize != 0) {
            expectedCommand.append('batchSize', new BsonInt32(batchSize))
        }
        if (expectedMaxTimeFieldValue != null) {
            expectedCommand.append('maxTimeMS', new BsonInt64(expectedMaxTimeFieldValue))
        }

        def reply = getMoreResponse([], 0)

        when:
        cursor.hasNext()

        then:
        1 * connection.command(NAMESPACE.getDatabaseName(), expectedCommand, _, _, _, connectionSource) >> {
            reply
        }
        1 * connection.release()

        where:
        batchSize  | maxTimeMS  | expectedMaxTimeFieldValue
        0          | 0          | null
        2          | 0          | null
        0          | 100        | 100
    }

    def 'should handle exceptions when closing'() {
        given:
        def connection = Mock(Connection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> 4
            }
            _ * command(*_) >> { throw new MongoSocketException('No MongoD', SERVER_ADDRESS) }
        }
        def connectionSource = Stub(ConnectionSource) {
            getServerApi() >> null
            getConnection() >> { connection }
        }
        connectionSource.retain() >> connectionSource

        def initialResults = createCommandResult([])
        def cursor = new CommandBatchCursor<Document>(SERVER_ADDRESS, initialResults, 0, 2, 100, new DocumentCodec(),
                null, connectionSource, connection)

        when:
        cursor.close()

        then:
        notThrown(MongoSocketException)

        when:
        cursor.close()

        then:
        notThrown(Exception)
    }

    def 'should handle exceptions when killing cursor and a connection can not be obtained'() {
        given:
        def connection = Mock(Connection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> 4
            }
        }
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> { throw new MongoSocketOpenException("can't open socket", SERVER_ADDRESS, new IOException()) }
            getServerApi() >> null
        }
        connectionSource.retain() >> connectionSource

        def initialResults = createCommandResult([])
        def cursor = new CommandBatchCursor<Document>(SERVER_ADDRESS, initialResults, 0, 2, 100, new DocumentCodec(),
                null, connectionSource, connection)

        when:
        cursor.close()

        then:
        notThrown(MongoSocketException)

        when:
        cursor.close()

        then:
        notThrown(Exception)
    }

    def 'should close cursor after getMore finishes if cursor was closed while getMore was in progress and getMore returns a response'() {
        given:
        def serverVersion =  new ServerVersion([3, 6, 0])
        Connection connection = mockConnection(serverVersion)
        ConnectionSource connectionSource
        if (serverType == ServerType.LOAD_BALANCER) {
            connectionSource = mockConnectionSource(SERVER_ADDRESS, serverType, connection)
        } else {
            connectionSource = mockConnectionSource(SERVER_ADDRESS, serverType, connection, mockConnection(serverVersion))
        }
        List<Document> firstBatch = [new Document()]
        def initialResults = createCommandResult(firstBatch)
        when:
        CommandBatchCursor<Document> cursor = new CommandBatchCursor<>(SERVER_ADDRESS, initialResults, 0, 0, 0, new DocumentCodec(),
                null, connectionSource, connection)
        List<Document> batch = cursor.next()

        then:
        batch == firstBatch

        when:
        cursor.next()

        then:
        // simulate the user calling `close` while `getMore` is in flight
        // in LB mode the same connection is used to execute both `getMore` and `killCursors`
        numberOfInvocations * connection.command(*_) >> {
            // `getMore` command
            cursor.close()
            response
        } >> {
            // `killCursors` command
            response2
        }

        then:
        IllegalStateException e = thrown()
        e.getMessage() == 'Cursor has been closed'

        then:
        connection.getCount() == 1
        connectionSource.getCount() == 1

        where:
        response               | response2              | getMoreResponseHasCursor | serverType               | numberOfInvocations
        getMoreResponse([])    | getMoreResponse([], 0) | true                     | ServerType.LOAD_BALANCER | 2
        getMoreResponse([], 0) | null                   | false                    | ServerType.LOAD_BALANCER | 1
        getMoreResponse([])    | getMoreResponse([], 0) | true                     | ServerType.STANDALONE    | 1
        getMoreResponse([], 0) | null                   | false                    | ServerType.STANDALONE    | 1
    }

    def 'should close cursor after getMore finishes if cursor was closed while getMore was in progress and getMore throws exception'() {
        given:
        Connection connection = mockConnection(serverVersion)
        ConnectionSource connectionSource
        if (serverType == ServerType.LOAD_BALANCER) {
            connectionSource = mockConnectionSource(SERVER_ADDRESS, serverType)
        } else {
            connectionSource = mockConnectionSource(SERVER_ADDRESS, serverType, connection, mockConnection(serverVersion))
        }
        List<Document> firstBatch = [new Document()]
        def initialResults = createCommandResult(firstBatch)
        String exceptionMessage = 'test'

        when:
        CommandBatchCursor<Document> cursor = new CommandBatchCursor<>(SERVER_ADDRESS, initialResults, 0, 0, 0, new DocumentCodec(),
                null, connectionSource, connection)
        List<Document> batch = cursor.next()

        then:
        batch == firstBatch

        when:
        cursor.next()

        then:
        // simulate the user calling `close` while `getMore` is in flight
        if (useCommand) {
            // in LB mode the same connection is used to execute both `getMore` and `killCursors`
            int numberOfInvocations = serverType == ServerType.LOAD_BALANCER ? 2 : 1
            numberOfInvocations * connection.command(*_) >> {
                // `getMore` command
                cursor.close()
                throw new MongoException(exceptionMessage)
            } >> {
                // `killCursors` command
                null
            }
        } else {
            1 * connection.getMore(*_) >> {
                cursor.close()
                throw new MongoException(exceptionMessage)
            }
        }

        then:
        MongoException e = thrown()
        e.getMessage() == exceptionMessage

        then:
        connection.getCount() == 1
        connectionSource.getCount() == 1

        where:
        serverVersion                | useCommand | serverType
        new ServerVersion([5, 0, 0]) | true       | ServerType.LOAD_BALANCER
        new ServerVersion([3, 2, 0]) | true       | ServerType.STANDALONE
    }

    /**
     * Creates a {@link Connection} with {@link Connection#getCount()} returning 1.
     */
    private Connection mockConnection(ServerVersion serverVersion) {
        int refCounter = 1
        Connection mockConn = Mock(Connection) {
            getDescription() >> Stub(ConnectionDescription) {
                getMaxWireVersion() >> getMaxWireVersionForServerVersion(serverVersion.getVersionList())
            }
        }
        mockConn.retain() >> {
            if (refCounter == 0) {
                throw new IllegalStateException('Tried to retain Connection when already released')
            } else {
                refCounter += 1
            }
            mockConn
        }
        mockConn.release() >> {
            refCounter -= 1
            if (refCounter < 0) {
                throw new IllegalStateException('Tried to release Connection below 0')
            }
            refCounter
        }
        mockConn.getCount() >> { refCounter }
        mockConn
    }

    private ConnectionSource mockConnectionSource(ServerAddress serverAddress, ServerType serverType, Connection... connections) {
        int connIdx = 0
        int refCounter = 1
        ConnectionSource mockConnectionSource = Mock(ConnectionSource)
        mockConnectionSource.getServerDescription() >> {
            ServerDescription.builder()
                    .address(serverAddress)
                    .type(serverType)
                    .state(ServerConnectionState.CONNECTED)
                    .build()
        }
        mockConnectionSource.retain() >> {
            if (refCounter == 0) {
                throw new IllegalStateException('Tried to retain ConnectionSource when already released')
            } else {
                refCounter += 1
            }
            mockConnectionSource
        }
        mockConnectionSource.release() >> {
            refCounter -= 1
            if (refCounter < 0) {
                throw new IllegalStateException('Tried to release ConnectionSource below 0')
            }
            refCounter
        }
        mockConnectionSource.getCount() >> { refCounter }
        mockConnectionSource.getConnection() >> {
            if (refCounter == 0) {
                throw new IllegalStateException('Tried to use released ConnectionSource')
            }
            Connection conn
            if (connIdx < connections.length) {
                conn = connections[connIdx]
            } else {
                throw new IllegalStateException('Requested more than maxConnections=' + maxConnections)
            }
            connIdx++
            conn.retain()
        }
        mockConnectionSource
    }

    private static BsonDocument getMoreResponse(results, cursorId = 42) {
        createCommandResult(results, cursorId, "nextBatch")
    }

    private static BsonDocument createCommandResult(List<?> results, Long cursorId = 42, String fieldNameContainingBatch = "firstBatch") {
        new BsonDocument("ok", new BsonInt32(1))
                        .append('cursor', new BsonDocument('id', new BsonInt64(cursorId))
                        .append("ns", new BsonString(NAMESPACE.fullName))
                        .append("id", new BsonInt64(cursorId))
                        .append(fieldNameContainingBatch, new BsonArrayWrapper(results)))
    }

}
