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
import org.bson.codecs.Codec
import org.bson.codecs.DocumentCodec
import spock.lang.Specification

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT
import static com.mongodb.internal.operation.OperationUnitSpecification.getMaxWireVersionForServerVersion

class CommandBatchCursorSpecification extends Specification {
    private static final MongoNamespace NAMESPACE = new MongoNamespace('db', 'coll')
    private static final ServerAddress SERVER_ADDRESS = new ServerAddress()
    private static final Codec<Document> CODEC = new DocumentCodec()

    def 'should generate expected command with batchSize and maxTimeMS'() {
        given:
        def connection = Mock(Connection) {
            _ * getDescription() >> Stub(ConnectionDescription) {
                getServerAddress() >> SERVER_ADDRESS
                getMaxWireVersion() >> 4
            }
        }
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> { connection }
            getOperationContext() >> OPERATION_CONTEXT
        }
        connectionSource.retain() >> connectionSource

        def cursorId = 42

        def result = createCommandResult([], cursorId)
        def cursor = new CommandBatchCursor<Document>(result, 0, batchSize, maxTimeMS, CODEC,
                null, connectionSource, connection)
        def expectedCommand = new BsonDocument('getMore': new BsonInt64(cursorId))
                .append('collection', new BsonString(NAMESPACE.getCollectionName()))
        if (batchSize != 0) {
            expectedCommand.append('batchSize', new BsonInt32(batchSize))
        }
        if (expectedMaxTimeFieldValue != null) {
            expectedCommand.append('maxTimeMS', new BsonInt64(expectedMaxTimeFieldValue))
        }

        def reply = createCommandResult([], 0, 'nextBatch')

        when:
        cursor.hasNext()

        then:
        1 * connection.command(NAMESPACE.getDatabaseName(), expectedCommand, _, _, _, OPERATION_CONTEXT) >> {
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
                getServerAddress() >> SERVER_ADDRESS
                getMaxWireVersion() >> 4
            }
            _ * command(*_) >> { throw new MongoSocketException('No MongoD', SERVER_ADDRESS) }
        }
        def connectionSource = Stub(ConnectionSource) {
            getOperationContext() >> OPERATION_CONTEXT
            getConnection() >> { connection }
        }
        connectionSource.retain() >> connectionSource

        def firstBatch = createCommandResult([], 42)
        def cursor = new CommandBatchCursor<Document>(firstBatch, 0, 2, 100, CODEC,
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
                getServerAddress() >> SERVER_ADDRESS
                getMaxWireVersion() >> 4
            }
        }
        def connectionSource = Stub(ConnectionSource) {
            getConnection() >> { throw new MongoSocketOpenException("can't open socket", SERVER_ADDRESS, new IOException()) }
            getOperationContext() >> OPERATION_CONTEXT
        }
        connectionSource.retain() >> connectionSource

        def firstBatch = createCommandResult([], 42)
        def cursor = new CommandBatchCursor<Document>(firstBatch, 0, 2, 100, CODEC,
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
        Connection conn = mockConnection(serverVersion)
        ConnectionSource connSource
        if (serverType == ServerType.LOAD_BALANCER) {
            connSource = mockConnectionSource(SERVER_ADDRESS, serverType)
        } else {
            connSource = mockConnectionSource(SERVER_ADDRESS, serverType, conn, mockConnection(serverVersion))
        }
        List<Document> firstBatch = [new Document()]
        BsonDocument commandResult = createCommandResult(firstBatch, 1)
        Object getMoreResponse = emptyGetMoreCommandResponse(getMoreResponseHasCursor ? 42 : 0)

        when:
        CommandBatchCursor<Document> cursor = new CommandBatchCursor<>(commandResult, 0, 0, 0, CODEC,
                null, connSource, conn)
        List<Document> batch = cursor.next()

        then:
        batch == firstBatch

        when:
        cursor.next()

        then:
        // simulate the user calling `close` while `getMore` is in flight
        // in LB mode the same connection is used to execute both `getMore` and `killCursors`
        numberOfInvocations * conn.command(*_) >> {
            // `getMore` command
            cursor.close()
            getMoreResponse
        } >> {
            // `killCursors` command
            null
        }

        then:
        IllegalStateException e = thrown()
        e.getMessage() == 'Cursor has been closed'

        then:
        conn.getCount() == 1
        connSource.getCount() == 1

        where:
        serverVersion                | getMoreResponseHasCursor | serverType                | numberOfInvocations
        new ServerVersion([5, 0, 0]) | true                     | ServerType.LOAD_BALANCER  | 2
        new ServerVersion([5, 0, 0]) | false                    | ServerType.LOAD_BALANCER  | 1
        new ServerVersion([3, 2, 0]) | true                     | ServerType.STANDALONE     | 2
        new ServerVersion([3, 2, 0]) | false                    | ServerType.STANDALONE     | 1
    }

    def 'should close cursor after getMore finishes if cursor was closed while getMore was in progress and getMore throws exception'() {
        given:
        Connection conn = mockConnection(serverVersion)
        ConnectionSource connSource
        if (serverType == ServerType.LOAD_BALANCER) {
            connSource = mockConnectionSource(SERVER_ADDRESS, serverType)
        } else {
            connSource = mockConnectionSource(SERVER_ADDRESS, serverType, conn, mockConnection(serverVersion))
        }
        List<Document> firstBatch = [new Document()]
        BsonDocument initialResult = createCommandResult(firstBatch,  1)
        String exceptionMessage = 'test'

        when:
        CommandBatchCursor<Document> cursor = new CommandBatchCursor<>(initialResult, 0, 0, 0, CODEC,
                null, connSource, conn)
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
            numberOfInvocations * conn.command(*_) >> {
                // `getMore` command
                cursor.close()
                throw new MongoException(exceptionMessage)
            } >> {
                // `killCursors` command
                null
            }
        } else {
            1 * conn.getMore(*_) >> {
                cursor.close()
                throw new MongoException(exceptionMessage)
            }
        }

        then:
        MongoException e = thrown()
        e.getMessage() == exceptionMessage

        then:
        conn.getCount() == 1
        connSource.getCount() == 1

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
                getServerAddress() >> SERVER_ADDRESS
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
                throw new IllegalStateException('Requested more than maxConnections=' + connections.length)
            }
            connIdx++
            conn.retain()
        }
        mockConnectionSource
    }

    private static BsonDocument emptyGetMoreCommandResponse(long cursorId) {
        createCommandResult([], cursorId, "nextBatch")
    }

    private static CommandCursorResult createCommandCursorResult(final List<?> results, final Long cursorId,
            final String fieldNameContainingBatch = "firstBatch", final ServerAddress serverAddress = SERVER_ADDRESS) {
        new CommandCursorResult(serverAddress, fieldNameContainingBatch, createCommandResult(results, cursorId, fieldNameContainingBatch))
    }

    private static BsonDocument createCommandResult(final List<?> results, final Long cursorId,
            final String fieldNameContainingBatch = "firstBatch") {
        new BsonDocument("ok", new BsonInt32(1))
                .append("cursor",
                        new BsonDocument("ns", new BsonString(NAMESPACE.fullName))
                                .append("id", new BsonInt64(cursorId))
                                .append(fieldNameContainingBatch, new BsonArrayWrapper(results)))
    }

}
