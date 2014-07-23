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

package org.mongodb.connection;

import category.Async;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.io.OutputBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mongodb.MongoCredential;
import org.mongodb.MongoException;
import org.mongodb.MongoNamespace;
import org.mongodb.ServerCursor;
import org.mongodb.event.ConnectionEvent;
import org.mongodb.event.ConnectionListener;
import org.mongodb.event.ConnectionMessageReceivedEvent;
import org.mongodb.event.ConnectionMessagesSentEvent;
import org.mongodb.operation.QueryFlag;
import org.mongodb.protocol.KillCursor;
import org.mongodb.protocol.message.CommandMessage;
import org.mongodb.protocol.message.KillCursorsMessage;
import org.mongodb.protocol.message.MessageSettings;
import org.mongodb.protocol.message.RequestMessage;

import java.util.Collections;
import java.util.EnumSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.mongodb.Fixture.getAsyncStreamFactory;
import static org.mongodb.Fixture.getPrimary;
import static org.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;

// This is a Java test so that we can use categories.
@Category(Async.class)
public class InternalStreamConnectionTest {
    private static final String CLUSTER_ID = "1";
    private StreamFactory factory = getAsyncStreamFactory();
    private Stream stream;

    @Before
    public void setUp() throws InterruptedException {
        stream = factory.create(getPrimary());
    }

    @After
    public void tearDown() {
        stream.close();
    }

    @Test
    public void shouldFireMessagesSentEventAsync() throws InterruptedException {
        // given
        TestConnectionListener listener = new TestConnectionListener();
        InternalStreamConnection connection = new InternalStreamConnection(CLUSTER_ID, stream, Collections.<MongoCredential>emptyList(),
                                                                           listener);
        OutputBuffer buffer = new ByteBufferOutputBuffer(connection);
        RequestMessage message = new KillCursorsMessage(new KillCursor(new ServerCursor(1, getPrimary())));
        message.encode(buffer);

        // when
        listener.reset();
        final CountDownLatch latch = new CountDownLatch(1);
        connection.sendMessageAsync(buffer.getByteBuffers(), message.getId(), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                latch.countDown();
            }
        }, false);
        latch.await();

        // then
        assertEquals(1, listener.messagesSentCount());
    }

    @Test
    public void shouldFireMessageReceiveEventAsync() throws InterruptedException {
        // given
        final TestConnectionListener listener = new TestConnectionListener();
        final InternalStreamConnection connection = new InternalStreamConnection(CLUSTER_ID, stream,
                                                                                 Collections.<MongoCredential>emptyList(), listener);
        final OutputBuffer buffer = new ByteBufferOutputBuffer(connection);
        final RequestMessage message = createIsMasterMessage();
        message.encode(buffer);

        // when
        listener.reset();
        final CountDownLatch latch = new CountDownLatch(1);
        connection.sendMessageAsync(buffer.getByteBuffers(), message.getId(), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                connection.receiveMessageAsync(message.getId(), new SingleResultCallback<ResponseBuffers>() {
                    @Override
                    public void onResult(final ResponseBuffers result, final MongoException e) {
                        latch.countDown();
                    }
                });
            }
        }, true);
        latch.await();

        // then
        assertEquals(1, listener.messageReceivedCount());
    }

    @Test
    public void shouldBeAbleToPipeline() throws InterruptedException {
        // Simulated: Send(1), Send(2), Send(3), Rcv(3), Rcv(1)

        // given
        final ConcurrentLinkedQueue<Integer> receivedIds = new ConcurrentLinkedQueue<Integer>();
        final TestConnectionListener listener = new TestConnectionListener();
        final InternalStreamConnection connection = new InternalStreamConnection(CLUSTER_ID, stream,
                                                                                 Collections.<MongoCredential>emptyList(), listener);
        final OutputBuffer buffer1 = new ByteBufferOutputBuffer(connection);
        final RequestMessage message1 = createIsMasterMessage();
        message1.encode(buffer1);

        final OutputBuffer buffer2 = new ByteBufferOutputBuffer(connection);
        final RequestMessage message2 = new KillCursorsMessage(new KillCursor(new ServerCursor(1, getPrimary())));
        message2.encode(buffer2);

        final OutputBuffer buffer3 = new ByteBufferOutputBuffer(connection);
        final RequestMessage message3 = createIsMasterMessage();
        message3.encode(buffer3);

        final CountDownLatch message3Requested = new CountDownLatch(1);

        // when
        listener.reset();
        final CountDownLatch latch = new CountDownLatch(3);

        // Send(1) Waits for Rcv(3) to be called before it asks to Rcv(1)
        connection.sendMessageAsync(buffer1.getByteBuffers(), message1.getId(), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                try {
                    message3Requested.await();
                } catch (InterruptedException e1) {
                    // Pass
                }
                receivedIds.add(message1.getId());
                connection.receiveMessageAsync(message1.getId(), new SingleResultCallback<ResponseBuffers>() {
                    @Override
                    public void onResult(final ResponseBuffers result, final MongoException e) {
                        latch.countDown();
                    }
                });
            }
        }, true);

        // Send(2) - Unacknowledged message
        connection.sendMessageAsync(buffer2.getByteBuffers(), message2.getId(), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                latch.countDown();
            }
        }, false);

        // Send(3) -> Rcv(3)
        connection.sendMessageAsync(buffer3.getByteBuffers(), message3.getId(), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                receivedIds.add(message3.getId());
                connection.receiveMessageAsync(message3.getId(), new SingleResultCallback<ResponseBuffers>() {
                    @Override
                    public void onResult(final ResponseBuffers result, final MongoException e) {
                        latch.countDown();
                    }
                });
                message3Requested.countDown();
            }
        }, true);
        latch.await();

        // then
        assertEquals(2, listener.messageReceivedCount());
        assertEquals(new Integer(message3.getId()), receivedIds.poll());
        assertEquals(new Integer(message1.getId()), receivedIds.poll());
    }

    private CommandMessage createIsMasterMessage() {
        return new CommandMessage(new MongoNamespace("admin", COMMAND_COLLECTION_NAME).getFullName(),
                           new BsonDocument("ismaster", new BsonInt32(1)),
                           EnumSet.noneOf(QueryFlag.class),
                           MessageSettings.builder().build());
    }


    private static final class TestConnectionListener implements ConnectionListener {
        private int messagesSentCount;
        private int messageReceivedCount;

        @Override
        public void connectionOpened(final ConnectionEvent event) {
        }

        @Override
        public void connectionClosed(final ConnectionEvent event) {
        }

        @Override
        public void messagesSent(final ConnectionMessagesSentEvent event) {
            messagesSentCount++;
        }

        @Override
        public void messageReceived(final ConnectionMessageReceivedEvent event) {
            messageReceivedCount++;
        }

        public void reset() {
            messagesSentCount = 0;
            messageReceivedCount = 0;
        }

        public int messagesSentCount() {
            return messagesSentCount;
        }

        public int messageReceivedCount() {
            return messageReceivedCount;
        }
    }
}
