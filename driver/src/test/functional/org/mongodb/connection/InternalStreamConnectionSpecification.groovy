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


package org.mongodb.connection

import org.bson.BsonDocument
import org.bson.BsonInt32
import org.mongodb.MongoNamespace
import org.mongodb.ServerCursor
import org.mongodb.event.ConnectionListener
import org.mongodb.operation.QueryFlag
import org.mongodb.protocol.KillCursor
import org.mongodb.protocol.message.CommandMessage
import org.mongodb.protocol.message.KillCursorsMessage
import org.mongodb.protocol.message.MessageSettings
import spock.lang.Specification

import static org.mongodb.Fixture.getPrimary
import static org.mongodb.Fixture.getSSLSettings
import static org.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME

class InternalStreamConnectionSpecification extends Specification {
    private static final String CLUSTER_ID = '1'
    def streamFactory = new SocketStreamFactory(SocketSettings.builder().build(), getSSLSettings())
    def stream = streamFactory.create(getPrimary())

    def cleanup() {
        stream.close();
    }

    def 'should fire connection opened event'() {
        given:
        def listener = Mock(ConnectionListener)

        when:
        new InternalStreamConnection(CLUSTER_ID, stream, [], listener)

        then:
        1 * listener.connectionOpened(_)
    }

    def 'should fire connection closed event'() {
        given:
        def listener = Mock(ConnectionListener)
        def connection = new InternalStreamConnection(CLUSTER_ID, stream, [], listener)

        when:
        connection.close()

        then:
        1 * listener.connectionClosed(_)
    }

    def 'should fire messages sent event'() {
        given:
        def listener = Mock(ConnectionListener)
        def connection = new InternalStreamConnection(CLUSTER_ID, stream, [], listener)
        def buffer = new ByteBufferOutputBuffer(connection);
        def message = new KillCursorsMessage(new KillCursor(new ServerCursor(1, getPrimary())));
        message.encode(buffer);

        when:
        connection.sendMessage(buffer.getByteBuffers(), message.getId())

        then:
        1 * listener.messagesSent(_)
    }

    def 'should fire message received event'() {
        given:
        def listener = Mock(ConnectionListener)
        def connection = new InternalStreamConnection(CLUSTER_ID, stream, [], listener)
        def buffer = new ByteBufferOutputBuffer(connection)
        def message = new CommandMessage(new MongoNamespace('admin', COMMAND_COLLECTION_NAME).fullName,
                                         new BsonDocument('ismaster', new BsonInt32(1)), EnumSet.noneOf(QueryFlag),
                                         MessageSettings.builder().build());
        message.encode(buffer);

        when:
        connection.sendMessage(buffer.getByteBuffers(), message.getId())
        connection.receiveMessage(message.getId())

        then:
        1 * listener.messageReceived(_)
    }
}
