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

package com.mongodb.connection

import category.Async
import category.Slow
import com.mongodb.MongoException
import com.mongodb.MongoNamespace
import com.mongodb.ServerCursor
import com.mongodb.codecs.DocumentCodec
import com.mongodb.event.ConnectionListener
import com.mongodb.operation.QueryFlag
import com.mongodb.operation.SingleResultFuture
import com.mongodb.protocol.KillCursor
import com.mongodb.protocol.message.CommandMessage
import com.mongodb.protocol.message.KillCursorsMessage
import com.mongodb.protocol.message.MessageSettings
import org.bson.BsonBinaryWriter
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.ByteBuf
import org.bson.ByteBufNIO
import org.bson.codecs.EncoderContext
import org.bson.io.BasicOutputBuffer
import org.bson.io.OutputBuffer
import org.junit.experimental.categories.Category
import org.mongodb.Document
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.SecureRandom
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import static MongoNamespace.COMMAND_COLLECTION_NAME
import static com.mongodb.ClusterFixture.getAsyncStreamFactory
import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.ClusterFixture.getSSLSettings

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

    @Category(Async)
    def 'should fire message sent event async'() {
        def latch = new CountDownLatch(1);
        def listener = Mock(ConnectionListener)
        def stream = getAsyncStreamFactory().create(getPrimary())
        def connection = new InternalStreamConnection(CLUSTER_ID, stream, [], listener)
        def buffer = new ByteBufferOutputBuffer(connection)
        def message = new CommandMessage(new MongoNamespace('admin', COMMAND_COLLECTION_NAME).fullName,
                                         new BsonDocument('ismaster', new BsonInt32(1)), EnumSet.noneOf(QueryFlag),
                                         MessageSettings.builder().build());
        message.encode(buffer);

        when:
        connection.sendMessageAsync(buffer.getByteBuffers(), message.getId(), new SingleResultCallback<Void>() {
            @Override
            void onResult(final Void result, final MongoException e) {
                latch.countDown();
            }
        })
        latch.await()

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

    @Category(Async)
    def 'should fire message received event async'() {
        def latch = new CountDownLatch(1);
        def listener = Mock(ConnectionListener)
        def stream = getAsyncStreamFactory().create(getPrimary())
        def connection = new InternalStreamConnection(CLUSTER_ID, stream, [], listener)
        def buffer = new ByteBufferOutputBuffer(connection)
        def message = new CommandMessage(new MongoNamespace('admin', COMMAND_COLLECTION_NAME).fullName,
                                         new BsonDocument('ismaster', new BsonInt32(1)), EnumSet.noneOf(QueryFlag),
                                         MessageSettings.builder().build());
        message.encode(buffer);

        when:
        connection.sendMessageAsync(buffer.getByteBuffers(), message.getId(), new SingleResultCallback<Void>() {
            @Override
            void onResult(final Void result, final MongoException e) {
            }
        })
        connection.receiveMessageAsync(message.getId(), new SingleResultCallback<ResponseBuffers>() {
            @Override
            void onResult(final ResponseBuffers result, final MongoException e) {
                latch.countDown();
            }
        })
        latch.await()

        then:
        1 * listener.messageReceived(_)
    }

    def 'should handle out of order messages on the stream'() {
        // Connect then: Send(1), Send(2), Send(3), Receive(3), Receive(2), Receive(1)
        given:
        def listener = Mock(ConnectionListener)
        def helper = new StreamHelper()
        def stream = Stub(Stream)
        stream.getBuffer(_) >> { args -> helper.getBuffer(args.get(0)) }
        stream.read(_) >>> helper.read(1) + helper.read(3, ordered)  // Setup costs a read

        def connection = new InternalStreamConnection(CLUSTER_ID, stream, [], listener)
        def (buffers1, messageId1) = helper.isMaster()
        def (buffers2, messageId2) = helper.isMaster()
        def (buffers3, messageId3) = helper.isMaster()

        when:
        connection.sendMessage(buffers1, messageId1)
        connection.sendMessage(buffers2, messageId2)
        connection.sendMessage(buffers3, messageId3)

        then:
        connection.receiveMessage(messageId3).replyHeader.responseTo == messageId3
        connection.receiveMessage(messageId2).replyHeader.responseTo == messageId2
        connection.receiveMessage(messageId1).replyHeader.responseTo == messageId1

        where:
        ordered << [true, false]
    }

    def 'should handle out of order messages on the stream async'() {
        // Connect then: SendAsync(1), SendAsync(2), SendAsync(3), ReceiveAsync(3), ReceiveAsync(2), ReceiveAsync(1)
        given:
        def listener = Mock(ConnectionListener)
        def helper = new StreamHelper()
        def stream = Stub(Stream)
        stream.getBuffer(_) >> { args -> helper.getBuffer(args.get(0)) }
        stream.read(_) >>> helper.read(1)  // Setup costs a read
        stream.writeAsync(_, _) >> { List<ByteBuf> buffers, AsyncCompletionHandler<Void> callback ->
            callback.completed(null)
        }
        def generateHeaders = {
            List<ByteBuf> headers = (1..3).collect { helper.header() }.reverse()
            if (!ordered) {
                Collections.shuffle(headers, new SecureRandom())
            }
            headers
        }
        List<ByteBuf> headers = generateHeaders()
        stream.readAsync(36, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(headers.pop())
        }
        stream.readAsync(74, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.body())
        }

        def connection = new InternalStreamConnection(CLUSTER_ID, stream, [], listener)
        def sendLatch = new CountDownLatch(3)
        def rcvdLatch = new CountDownLatch(3)
        def (buffers1, messageId1, sendCallback1, rcvdCallback1, fResponseBuffers1) = helper.isMasterAsync(sendLatch, rcvdLatch)
        def (buffers2, messageId2, sendCallback2, rcvdCallback2, fResponseBuffers2) = helper.isMasterAsync(sendLatch, rcvdLatch)
        def (buffers3, messageId3, sendCallback3, rcvdCallback3, fResponseBuffers3) = helper.isMasterAsync(sendLatch, rcvdLatch)

        when:
        connection.sendMessageAsync(buffers1, messageId1, sendCallback1)
        connection.sendMessageAsync(buffers2, messageId2, sendCallback2)
        connection.sendMessageAsync(buffers3, messageId3, sendCallback3)

        then:
        sendLatch.await()

        when:
        connection.receiveMessageAsync(messageId3, rcvdCallback3)
        connection.receiveMessageAsync(messageId2, rcvdCallback2)
        connection.receiveMessageAsync(messageId1, rcvdCallback1)
        rcvdLatch.await()

        then:
        fResponseBuffers1.get().replyHeader.responseTo == messageId1
        fResponseBuffers2.get().replyHeader.responseTo == messageId2
        fResponseBuffers3.get().replyHeader.responseTo == messageId3

        where:
        ordered << [true, false]
    }

    @SuppressWarnings(['UnusedVariable'])
    def 'should handle out of order messages on the stream mixed synchronicity'() {
        // Connect then: Send(1), SendAsync(2), Send(3), ReceiveAsync(3), Receive(2), ReceiveAsync(1)
        given:
        def listener = Mock(ConnectionListener)
        def helper = new StreamHelper()
        def stream = Stub(Stream)
        stream.getBuffer(_) >> { args -> helper.getBuffer(args.get(0)) }
        stream.read(36) >> helper.header()
        stream.read(74) >> helper.body()
        stream.writeAsync(_, _) >> { List<ByteBuf> buffers, AsyncCompletionHandler<Void> callback ->
            callback.completed(null)
        }
        stream.readAsync(36, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.header())
        }
        stream.readAsync(74, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.body())
        }

        def connection = new InternalStreamConnection(CLUSTER_ID, stream, [], listener)
        def sendLatch = new CountDownLatch(1)
        def rcvdLatch = new CountDownLatch(2)
        def (buffers1, messageId1, sendCallback1, rcvdCallback1, fResponseBuffers1) = helper.isMasterAsync(sendLatch, rcvdLatch)
        def (buffers2, messageId2, sendCallback2, rcvdCallback2, fResponseBuffers2) = helper.isMasterAsync(sendLatch, rcvdLatch)
        def (buffers3, messageId3, sendCallback3, rcvdCallback3, fResponseBuffers3) = helper.isMasterAsync(sendLatch, rcvdLatch)

        when:
        connection.sendMessage(buffers1, messageId1)
        connection.sendMessageAsync(buffers2, messageId2, sendCallback2)
        connection.sendMessage(buffers3, messageId3)

        then:
        sendLatch.await()

        when:
        connection.receiveMessageAsync(messageId3, rcvdCallback3)
        ResponseBuffers responseBuffers2 = connection.receiveMessage(messageId2)
        connection.receiveMessageAsync(messageId1, rcvdCallback1)
        rcvdLatch.await()

        then:
        fResponseBuffers1.get().replyHeader.responseTo == messageId1
        responseBuffers2.replyHeader.responseTo == messageId2
        fResponseBuffers3.get().replyHeader.responseTo == messageId3
    }

    @Category(Slow)
    def 'should the connection pipeling should be threadsafe'() {
        given:
        int threads = 10
        ExecutorService pool = Executors.newFixedThreadPool(threads)
        def listener = Mock(ConnectionListener)
        def helper = new StreamHelper()
        def stream = Stub(Stream)
        stream.getBuffer(_) >> { args -> helper.getBuffer(args.get(0)) }
        stream.read(36) >> { helper.header() }
        stream.read(74) >> { helper.body() }

        when:
        def connection = new InternalStreamConnection(CLUSTER_ID, stream, [], listener)

        then:
        (1..100000).each { n ->
            def (buffers, messageId) = helper.isMaster()
            pool.submit( { connection.sendMessage(buffers, messageId) } as Runnable )
            pool.submit( { connection.receiveMessage(messageId).replyHeader.responseTo == messageId } as Runnable )
        }

        cleanup:
        pool.shutdown()
    }

    @Category(Slow)
    def 'should the connection pipeling should be threadsafe async'() {
        given:
        int threads = 10
        ExecutorService pool = Executors.newFixedThreadPool(threads)
        def listener = Mock(ConnectionListener)
        def helper = new StreamHelper()
        def stream = Stub(Stream)
        stream.getBuffer(_) >> { args -> helper.getBuffer(args.get(0)) }
        stream.read(36) >> { helper.header() }
        stream.read(74) >> { helper.body() }
        stream.writeAsync(_, _) >> { List<ByteBuf> buffers, AsyncCompletionHandler<Void> callback ->
            callback.completed(null)
        }
        stream.readAsync(36, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.header())
        }
        stream.readAsync(74, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.body())
        }

        when:
        def connection = new InternalStreamConnection(CLUSTER_ID, stream, [], listener)

        then:
        (1..100000).each { n ->
            def sendLatch = new CountDownLatch(1)
            def rcvdLatch = new CountDownLatch(1)
            def (buffers, messageId, sendCallback, rcvdCallback, fResponseBuffers) = helper.isMasterAsync(sendLatch, rcvdLatch)

            pool.submit( { connection.sendMessageAsync(buffers, messageId, sendCallback) } as Runnable )
            pool.submit( { connection.receiveMessageAsync(messageId, rcvdCallback) } as Runnable )

            fResponseBuffers.get().replyHeader.responseTo == messageId
        }

        cleanup:
        pool.shutdown()
    }


    class StreamHelper {
        int nextMessageId = isMaster().get(1) + 1 // Generates a message then adds one to the id
        PowerOfTwoBufferPool bufferProvider = new PowerOfTwoBufferPool()

        def read(int count) {
            read(count, false)
        }

        def read(int count, boolean ordered) {
            List<ByteBuf> headers = (1..count).collect { header() }
            List<ByteBuf> bodies = (1..count).collect { body() }
            if (!ordered) {
                Collections.shuffle(headers, new SecureRandom())
            }
            [headers, bodies].transpose().flatten()
        }

        def header() {
            ByteBuffer headerByteBuffer = ByteBuffer.allocate(36).with {
                order(ByteOrder.LITTLE_ENDIAN);
                putInt(110);           // messageLength
                putInt(4);             // requestId
                putInt(nextMessageId); // responseTo
                putInt(1);             // opCode
                putInt(0);             // responseFlags
                putLong(0);            // cursorId
                putInt(0);             // starting from
                putInt(1);             // number returned
            }
            headerByteBuffer.flip()
            nextMessageId++
            new ByteBufNIO(headerByteBuffer)
        }

        def body() {
            def okResponse = ['connectionId': 1, 'n': 0, 'syncMillis': 0, 'writtenTo': null, 'err': null, 'ok': 1] as Document
            def binaryResponse = new BsonBinaryWriter(new BasicOutputBuffer(), false)
            new DocumentCodec().encode(binaryResponse, okResponse, EncoderContext.builder().build())
            binaryResponse.buffer.byteBuffers.get(0)
        }

        def getBuffer(size) { bufferProvider.getBuffer(size) }

        def isMaster() {
            def command = new CommandMessage(new MongoNamespace('admin', COMMAND_COLLECTION_NAME).getFullName(),
                                             new BsonDocument('ismaster', new BsonInt32(1)),
                                             EnumSet.noneOf(QueryFlag),
                                             MessageSettings.builder().build());
            OutputBuffer buffer = new BasicOutputBuffer();
            command.encode(buffer);
            [buffer.byteBuffers, command.getId()]
        }

        def isMasterAsync(CountDownLatch sendLatch, CountDownLatch rcvdLatch) {
            SingleResultFuture<ResponseBuffers> futureResponseBuffers = new SingleResultFuture<ResponseBuffers>()
            isMaster() + [new SingleResultCallback<Void>() {
                @Override
                void onResult(final Void result, final MongoException e) {
                    sendLatch.countDown()
                }
            }, new SingleResultCallback<ResponseBuffers>() {
                @Override
                void onResult(final ResponseBuffers result, final MongoException e) {
                    rcvdLatch.countDown()
                    futureResponseBuffers.init(result, e)
                }
            }, futureResponseBuffers]

        }
    }
}
