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

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.ByteBuf;
import org.bson.io.BasicInputBuffer;
import org.mongodb.CommandResult;
import org.mongodb.MongoCommandFailureException;
import org.mongodb.MongoCredential;
import org.mongodb.MongoException;
import org.mongodb.MongoInternalException;
import org.mongodb.MongoInterruptedException;
import org.mongodb.event.ConnectionEvent;
import org.mongodb.event.ConnectionListener;
import org.mongodb.event.ConnectionMessageReceivedEvent;
import org.mongodb.event.ConnectionMessagesSentEvent;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.connection.ReplyHeader.REPLY_HEADER_LENGTH;

class InternalStreamConnection implements InternalConnection {

    private final AtomicInteger incrementingId = new AtomicInteger();
    private final String clusterId;
    private final Stream stream;
    private final ConnectionListener eventPublisher;
    private final List<MongoCredential> credentialList;
    private final Pipeline pipeline = new Pipeline();
    private volatile boolean isClosed;
    private String id;

    InternalStreamConnection(final String clusterId, final Stream stream, final List<MongoCredential> credentialList,
                             final ConnectionListener connectionListener) {
        this.clusterId = notNull("clusterId", clusterId);
        this.stream = notNull("stream", stream);
        this.eventPublisher = notNull("connectionListener", connectionListener);
        notNull("credentialList", credentialList);
        this.credentialList = new ArrayList<MongoCredential>(credentialList);
        initialize();
        connectionListener.connectionOpened(new ConnectionEvent(clusterId, stream.getAddress(), getId()));
    }

    @Override
    public void close() {
        isClosed = true;
        stream.close();
        eventPublisher.connectionClosed(new ConnectionEvent(clusterId, stream.getAddress(), getId()));
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    public ServerAddress getServerAddress() {
        return stream.getAddress();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return stream.getBuffer(size);
    }

    public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
        isTrue("open", !isClosed());
        pipeline.sendMessage(byteBuffers, lastRequestId);
    }

    @Override
    public ResponseBuffers receiveMessage(final int responseTo) {
        isTrue("open", !isClosed());
        final Response response = pipeline.receiveMessage(responseTo);
        if (response.hasError()) {
            throw response.getError();
        }
        return response.getResult();
    }

    @Override
    public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
        isTrue("open", !isClosed());
        pipeline.sendMessageAsync(byteBuffers, lastRequestId, callback);
    }

    @Override
    public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("open", !isClosed());
        pipeline.receiveMessageAsync(responseTo, callback);
    }

    private void fillAndFlipBuffer(final int numBytes, final SingleResultCallback<ByteBuf> callback) {
        stream.readAsync(numBytes, new AsyncCompletionHandler<ByteBuf>() {
            @Override
            public void completed(final ByteBuf buffer) {
                callback.onResult(buffer, null);
            }

            @Override
            public void failed(final Throwable t) {
                if (t instanceof MongoException) {
                    callback.onResult(null, (MongoException) t);
                } else if (t instanceof IOException) {
                    callback.onResult(null, new MongoSocketReadException("Exception writing to stream", getServerAddress(), t));
                } else {
                    callback.onResult(null, new MongoInternalException("Unexpected exception", t));
                }
            }
        });
    }

    private MongoException translateReadException(final IOException e) {
        close();
        if (e instanceof SocketTimeoutException) {
            throw new MongoSocketReadTimeoutException("Timeout while receiving message", getServerAddress(), e);
        } else if (e instanceof InterruptedIOException || e instanceof ClosedByInterruptException) {
            throw new MongoInterruptedException("Interrupted while receiving message", e);
        } else {
            throw new MongoSocketReadException("Exception receiving message", getServerAddress(), e);
        }
    }

    private ResponseBuffers readResponseBuffers(final long start) throws IOException {
        ByteBuf headerByteBuffer = stream.read(REPLY_HEADER_LENGTH);
        ReplyHeader replyHeader;
        BasicInputBuffer headerInputBuffer = new BasicInputBuffer(headerByteBuffer);
        try {
            replyHeader = new ReplyHeader(headerInputBuffer);
        } finally {
            headerInputBuffer.close();
        }

        ByteBuf bodyByteBuffer = null;

        if (replyHeader.getNumberReturned() > 0) {
            bodyByteBuffer = stream.read(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH);
        }

        return new ResponseBuffers(replyHeader, bodyByteBuffer, System.nanoTime() - start);
    }

    private void initialize() {
        initializeConnectionId();
        authenticateAll();
        // try again if there was an exception calling getlasterror before authenticating
        if (id.contains("*")) {
            initializeConnectionId();
        }
    }

    private void initializeConnectionId() {
        CommandResult result;
        try {
            result = CommandHelper.executeCommand("admin", new BsonDocument("getlasterror", new BsonInt32(1)), this);

        } catch (MongoCommandFailureException e) {
            result = e.getCommandResult();
        }
        BsonInt32 connectionIdFromServer = (BsonInt32) result.getResponse().get("connectionId");
        id = "conn" + (connectionIdFromServer != null ? connectionIdFromServer.getValue() : "*" + incrementingId.incrementAndGet() + "*");
    }

    private void authenticateAll() {
        for (final MongoCredential cur : credentialList) {
            createAuthenticator(cur).authenticate();
        }
    }

    private Authenticator createAuthenticator(final MongoCredential credential) {
        switch (credential.getMechanism()) {
            case MONGODB_CR:
                return new NativeAuthenticator(credential, this);
            case GSSAPI:
                return new GSSAPIAuthenticator(credential, this);
            case PLAIN:
                return new PlainAuthenticator(credential, this);
            case MONGODB_X509:
                return new X509Authenticator(credential, this);
            default:
                throw new IllegalArgumentException("Unsupported authentication protocol: " + credential.getMechanism());
        }
    }

    private class ResponseHeaderCallback implements SingleResultCallback<ByteBuf> {
        private final long start;
        private final SingleResultCallback<ResponseBuffers> callback;

        public ResponseHeaderCallback(final long start, final SingleResultCallback<ResponseBuffers> callback) {
            this.start = start;
            this.callback = callback;
        }

        @Override
        public void onResult(final ByteBuf result, final MongoException e) {
            if (e != null) {
                callback.onResult(null, e);
            } else {
                ReplyHeader replyHeader;
                BasicInputBuffer headerInputBuffer = new BasicInputBuffer(result);
                try {
                    replyHeader = new ReplyHeader(headerInputBuffer);
                } finally {
                    headerInputBuffer.close();
                }

                if (replyHeader.getMessageLength() == REPLY_HEADER_LENGTH) {
                    onSuccess(new ResponseBuffers(replyHeader, null, System.nanoTime() - start));
                } else {
                    fillAndFlipBuffer(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH,
                                      new ResponseBodyCallback(replyHeader));
                }
            }
        }

        private void onSuccess(final ResponseBuffers responseBuffers) {
            eventPublisher.messageReceived(new ConnectionMessageReceivedEvent(clusterId,
                                                                              stream.getAddress(),
                                                                              getId(),
                                                                              responseBuffers.getReplyHeader().getResponseTo(),
                                                                              responseBuffers.getReplyHeader().getMessageLength()));
            callback.onResult(responseBuffers, null);
        }

        private class ResponseBodyCallback implements SingleResultCallback<ByteBuf> {
            private final ReplyHeader replyHeader;

            public ResponseBodyCallback(final ReplyHeader replyHeader) {
                this.replyHeader = replyHeader;
            }

            @Override
            public void onResult(final ByteBuf result, final MongoException e) {
                if (e != null) {
                    callback.onResult(null, e);
                } else {
                    onSuccess(new ResponseBuffers(replyHeader, result, System.nanoTime() - start));
                }
            }
        }
    }

    private int getTotalRemaining(final List<ByteBuf> byteBuffers) {
        int messageSize = 0;
        for (final ByteBuf cur : byteBuffers) {
            messageSize += cur.remaining();
        }
        return messageSize;
    }

    private class Pipeline {
        private final ConcurrentLinkedQueue<SendMessageAsync> writeQueue = new ConcurrentLinkedQueue<SendMessageAsync>();
        private final ConcurrentHashMap<Integer, SingleResultCallback<ResponseBuffers>> readQueue =
            new ConcurrentHashMap<Integer, SingleResultCallback<ResponseBuffers>>();
        private final ConcurrentMap<Integer, Response> messages = new ConcurrentHashMap<Integer, Response>();

        private final Semaphore writing = new Semaphore(1);
        private final Semaphore reading = new Semaphore(1);

        private synchronized void sendMessage(final List<ByteBuf> byteBuffers, final int messageId) {

            try {
                if (!writing.tryAcquire()) {
                    writing.acquire();
                }
                stream.write(byteBuffers);
                eventPublisher.messagesSent(new ConnectionMessagesSentEvent(clusterId, stream.getAddress(), getId(), messageId,
                                                                            getTotalRemaining(byteBuffers)));
            } catch (IOException e) {
                close();
                throw new MongoSocketWriteException("Exception sending message", getServerAddress(), e);
            } catch (InterruptedException e) {
                close();
                throw new MongoInternalException("Unexpected interrupted exception", e);
            } finally {
                writing.release();
            }
        }

        private synchronized Response receiveMessage(final int messageId) {
            Response response;
            response = checkMessages(messageId);
            if (response != null) {
                return response;
            }

            try {
                if (!reading.tryAcquire()) {
                    reading.acquire();
                }

                response = checkMessages(messageId);
                if (response != null) {
                    return response;
                }

                ResponseBuffers responseBuffers = readResponseBuffers(System.nanoTime());
                eventPublisher.messageReceived(new ConnectionMessageReceivedEvent(clusterId,
                                                                                  stream.getAddress(),
                                                                                  getId(),
                                                                                  responseBuffers.getReplyHeader().getResponseTo(),
                                                                                  responseBuffers.getReplyHeader().getMessageLength()));

                messages.put(responseBuffers.getReplyHeader().getResponseTo(), new Response(responseBuffers, null));
            } catch (IOException e) {
                close();
                return new Response(null, translateReadException(e));
            } catch (MongoException e) {
                close();
                return new Response(null, e);
            } catch (RuntimeException e) {
                close();
                return new Response(null, new MongoInternalException("Unexpected runtime exception", e));
            } catch (InterruptedException e) {
                close();
                return new Response(null, new MongoInternalException("Unexpected runtime exception", e));
            } finally {
                reading.release();
            }

            response = checkMessages(messageId);
            if (response != null) {
                return response;
            } else {
                return receiveMessage(messageId);
            }

        }

        private synchronized void sendMessageAsync(final List<ByteBuf> byteBuffers, final int messageId,
                                                   final SingleResultCallback<Void> callback) {
            writeQueue.add(new SendMessageAsync(byteBuffers, messageId, callback));
            processPending();
        }

        private synchronized void receiveMessageAsync(final int messageId, final SingleResultCallback<ResponseBuffers> callback) {
            readQueue.put(messageId, callback);
            processPending();
        }

        private synchronized Response checkMessages(final int messageId) {
            if (messages.containsKey(messageId)) {
                return messages.remove(messageId);
            } else {
                return null;
            }
        }

        private synchronized void processPending() {
            processPendingResults();
            processPendingReads();
            processPendingWrites();
        }

        private synchronized void processPendingReads() {
            if (!readQueue.isEmpty() && reading.tryAcquire()) {
                fillAndFlipBuffer(REPLY_HEADER_LENGTH,
                                  new ResponseHeaderCallback(System.nanoTime(), new SingleResultCallback<ResponseBuffers>() {
                                      @Override
                                      public void onResult(final ResponseBuffers result, final MongoException e) {
                                          reading.release();
                                          messages.put(result.getReplyHeader().getResponseTo(), new Response(result, e));
                                          processPending();
                                      }
                                  }));
            }
        }

        private synchronized void processPendingWrites() {
            if (!writeQueue.isEmpty() && writing.tryAcquire()) {
                final SendMessageAsync message = writeQueue.poll();
                stream.writeAsync(message.getByteBuffers(), new AsyncCompletionHandler<Void>() {
                    @Override
                    public void completed(final Void t) {
                        writing.release();
                        eventPublisher.messagesSent(new ConnectionMessagesSentEvent(clusterId, stream.getAddress(), getId(),
                                                                                    message.getMessageId(),
                                                                                    getTotalRemaining(message.getByteBuffers())));
                        message.getCallback().onResult(null, null);
                        processPending();
                    }

                    @Override
                    public void failed(final Throwable t) {
                        writing.release();
                        if (t instanceof MongoException) {
                            message.getCallback().onResult(null, (MongoException) t);
                        } else if (t instanceof IOException) {
                            message.getCallback().onResult(null, new MongoSocketWriteException("Exception writing to stream",
                                                                                               getServerAddress(), (IOException) t));
                        } else {
                            message.getCallback().onResult(null, new MongoInternalException("Unexpected exception", t));
                        }
                        processPending();
                    }
                });
            }
        }

        private synchronized void processPendingResults() {
            Iterator<Map.Entry<Integer, Response>> it = messages.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Integer, Response> pairs = it.next();
                final int messageId = pairs.getKey();
                if (readQueue.containsKey(messageId)) {
                    final Response response = pairs.getValue();
                    readQueue.remove(messageId).onResult(response.getResult(), response.getError());
                    it.remove();
                }
            }
        }
    }


    private static class SendMessageAsync {
        private final List<ByteBuf> byteBuffers;
        private final int messageId;
        private final SingleResultCallback<Void> callback;

        SendMessageAsync(final List<ByteBuf> byteBuffers, final int messageId, final SingleResultCallback<Void> callback) {
            this.byteBuffers = byteBuffers;
            this.messageId = messageId;
            this.callback = callback;
        }

        public List<ByteBuf> getByteBuffers() {
            return byteBuffers;
        }

        public int getMessageId() {
            return messageId;
        }

        public SingleResultCallback<Void> getCallback() {
            return callback;
        }
    }


    private static class Response {
        private final ResponseBuffers result;
        private final MongoException error;

        public Response(final ResponseBuffers result, final MongoException error) {
            this.result = result;
            this.error = error;
        }

        public ResponseBuffers getResult() {
            return result;
        }

        public MongoException getError() {
            return error;
        }

        public boolean hasError() {
            return error != null;
        }
    }

}
