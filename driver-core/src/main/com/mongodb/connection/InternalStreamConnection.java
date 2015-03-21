/*
 * Copyright (c) 2014-2015 MongoDB, Inc.
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

package com.mongodb.connection;

import com.mongodb.Block;
import com.mongodb.Function;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSocketClosedException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.ConnectionEvent;
import com.mongodb.event.ConnectionListener;
import com.mongodb.event.ConnectionMessageReceivedEvent;
import com.mongodb.event.ConnectionMessagesSentEvent;
import org.bson.ByteBuf;
import org.bson.io.ByteBufferBsonInput;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ReplyHeader.REPLY_HEADER_LENGTH;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static java.lang.String.format;

final class InternalStreamConnection implements InternalConnection {
    private final ServerId serverId;
    private final StreamFactory streamFactory;
    private final InternalConnectionInitializer connectionInitializer;
    private final ConnectionListener connectionListener;

    private final InternalStreamWriteQueue writeQueue;
    private final InternalStreamReadQueue readQueue;

    private final AtomicBoolean isClosed = new AtomicBoolean();
    private final AtomicBoolean opened = new AtomicBoolean();

    private volatile ConnectionDescription description;
    private volatile Stream stream;

    static final Logger LOGGER = Loggers.getLogger("connection");

    InternalStreamConnection(final ServerId serverId, final StreamFactory streamFactory,
                             final InternalConnectionInitializer connectionInitializer,
                             final ConnectionListener connectionListener) {
        this.serverId = notNull("serverId", serverId);
        this.streamFactory = notNull("streamFactory", streamFactory);
        this.connectionInitializer = notNull("connectionInitializer", connectionInitializer);
        this.connectionListener = notNull("connectionListener", connectionListener);
        description = new ConnectionDescription(serverId);
        readQueue = new InternalStreamReadQueue(serverId, description);
        writeQueue = new InternalStreamWriteQueue(serverId, description);
    }

    @Override
    public ConnectionDescription getDescription() {
        return description;
    }

    @Override
    public void open() {
        isTrue("Open already called", stream == null);
        stream = streamFactory.create(serverId.getAddress());
        try {
            stream.open();
            setDescription(connectionInitializer.initialize(this));

            opened.set(true);

            try {
                connectionListener.connectionOpened(new ConnectionEvent(getId()));
            } catch (Throwable t) {
                LOGGER.warn("Exception when trying to signal connectionOpened to the connectionListener", t);
            }
            LOGGER.info(format("Opened connection [%s] to %s", getId(), serverId.getAddress()));
        } catch (Throwable t) {
            close();
            if (t instanceof MongoException) {
                throw (MongoException) t;
            } else {
                throw new MongoException(t.toString(), t);
            }
        }
    }

    @Override
    public void openAsync(final SingleResultCallback<Void> callback) {
        isTrue("Open already called", stream == null, callback);
        try {
            stream = streamFactory.create(serverId.getAddress());
        } catch (Throwable t) {
            callback.onResult(null, t);
            return;
        }
        stream.openAsync(new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void aVoid) {
                connectionInitializer.initializeAsync(InternalStreamConnection.this, new SingleResultCallback<ConnectionDescription>() {
                    @Override
                    public void onResult(final ConnectionDescription result, final Throwable t) {
                        if (t != null) {
                            close();
                            callback.onResult(null, t);
                        } else {
                            description = result;
                            opened.set(true);
                            try {
                                connectionListener.connectionOpened(new ConnectionEvent(getId()));
                            } catch (Throwable tr) {
                                LOGGER.warn("Exception when trying to signal connectionOpened to the connectionListener", tr);
                            }
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info(format("Opened connection [%s] to %s", getId(), serverId.getAddress()));
                            }
                            callback.onResult(null, null);
                        }
                    }
                });
            }

            @Override
            public void failed(final Throwable t) {
                callback.onResult(null, t);
            }
        });
    }

    @Override
    public void close() {
        if (stream != null) {
            stream.close();
        }
        isClosed.set(true);
        try {
            connectionListener.connectionClosed(new ConnectionEvent(getId()));
        } catch (Throwable t) {
            LOGGER.warn("Exception when trying to signal connectionClosed to the connectionListener", t);
        }
    }

    @Override
    public boolean opened() {
        return opened.get();
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
        notNull("open", stream);
        if (isClosed()) {
            throw new MongoSocketClosedException("Cannot write to a closed stream", getServerAddress());
        } else {
            try {
                writeQueue.acquire();
                stream.write(byteBuffers);
                try {
                    connectionListener.messagesSent(new ConnectionMessagesSentEvent(getId(),
                                                                                    lastRequestId,
                                                                                    getTotalRemaining(byteBuffers)));
                } catch (Throwable t) {
                    LOGGER.warn("Exception when trying to signal messagesSent to the connectionListener", t);
                }
            } catch (Exception e) {
                close();
                throw translateWriteException(e);
            } finally {
                writeQueue.release();
            }
        }
    }

    @Override
    public ResponseBuffers receiveMessage(final int responseTo) {
        notNull("open", stream);
        if (isClosed()) {
            throw new MongoSocketClosedException("Cannot read from a closed stream", getServerAddress());
        } else {
            readQueue.acquire(new Block<Throwable>() {
                @Override
                public void apply(final Throwable t) {
                    if (t != null) {
                        throw MongoException.fromThrowable(t);
                    } else {
                        while (!readQueue.containsKeyResponse(responseTo)) {
                            try {
                                ResponseBuffers responseBuffers = receiveResponseBuffers();
                                try {
                                    connectionListener.messageReceived(new ConnectionMessageReceivedEvent(getId(),
                                            responseBuffers.getReplyHeader().getResponseTo(),
                                            responseBuffers.getReplyHeader().getMessageLength()));
                                } catch (Throwable t1) {
                                    LOGGER.warn("Exception when trying to signal messageReceived to the connectionListener", t1);
                                }
                                readQueue.putResponse(responseBuffers.getReplyHeader().getResponseTo(),
                                        new InternalStreamResponse(responseBuffers, null));
                            } catch (IOException e) {
                                readQueue.putResponse(responseTo, new InternalStreamResponse(null, translateReadException(e)));
                                close();
                                break;
                            }
                        }
                    }
                }
            });

            InternalStreamResponse response = readQueue.removeResponse(responseTo);
            if (response.hasError()) {
                Throwable t = response.getError();
                if (t instanceof MongoException) {
                    throw (MongoException) t;
                } else {
                    throw MongoException.fromThrowable(t);
                }
            }
            return response.getResult();
        }
    }

    @Override
    public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
        notNull("open", stream, callback);
        writeQueue.add(new InternalStreamSendMessageAsync(byteBuffers, lastRequestId, errorHandlingCallback(callback, LOGGER)));
        processPendingWrites();
    }

    @Override
    public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
        notNull("open", stream, callback);
        readQueue.put(responseTo, errorHandlingCallback(callback, LOGGER));
        processPendingReads();
    }

    private void setDescription(final ConnectionDescription description) {
        this.description = description;
        writeQueue.setDescription(description);
        readQueue.setDescription(description);

    }

    private ConnectionId getId() {
        return description.getConnectionId();
    }

    private ServerAddress getServerAddress() {
        return description.getServerAddress();
    }

    private void fillAndFlipBuffer(final int numBytes, final SingleResultCallback<ByteBuf> callback) {
        notNull("open", stream, callback);
        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Cannot read from a closed stream", getServerAddress()));
        } else {
            stream.readAsync(numBytes, new AsyncCompletionHandler<ByteBuf>() {
                @Override
                public void completed(final ByteBuf buffer) {
                    callback.onResult(buffer, null);
                }

                @Override
                public void failed(final Throwable t) {
                    close();
                    callback.onResult(null, translateReadException(t));
                }
            });
        }
    }

    private MongoException translateWriteException(final Throwable e) {
        if (e instanceof MongoException) {
            return (MongoException) e;
        } else if (e instanceof IOException) {
            return new MongoSocketWriteException("Exception sending message", getServerAddress(), e);
        } else if (e instanceof InterruptedException) {
            return new MongoInternalException("Thread interrupted exception", e);
        } else {
            return new MongoInternalException("Unexpected exception", e);
        }
    }

    private MongoException translateReadException(final Throwable e) {
        if (e instanceof MongoException) {
            return (MongoException) e;
        } else if (e instanceof SocketTimeoutException) {
            return new MongoSocketReadTimeoutException("Timeout while receiving message", getServerAddress(), e);
        } else if (e instanceof InterruptedIOException) {
            return new MongoInterruptedException("Interrupted while receiving message", (InterruptedIOException) e);
        } else if (e instanceof ClosedByInterruptException) {
            return new MongoInterruptedException("Interrupted while receiving message", (ClosedByInterruptException) e);
        } else if (e instanceof IOException) {
            return new MongoSocketReadException("Exception receiving message", getServerAddress(), e);
        } else if (e instanceof RuntimeException) {
            return new MongoInternalException("Unexpected runtime exception", e);
        } else if (e instanceof InterruptedException) {
            return new MongoInternalException("Interrupted exception", e);
        } else {
            return new MongoInternalException("Unexpected exception", e);
        }
    }

    private ResponseBuffers receiveResponseBuffers() throws IOException {
        ByteBuf headerByteBuffer = stream.read(REPLY_HEADER_LENGTH);
        ReplyHeader replyHeader;
        ByteBufferBsonInput headerInputBuffer = new ByteBufferBsonInput(headerByteBuffer);
        try {
            replyHeader = new ReplyHeader(headerInputBuffer);
        } finally {
            headerInputBuffer.close();
        }

        ByteBuf bodyByteBuffer = null;

        if (replyHeader.getNumberReturned() > 0) {
            bodyByteBuffer = stream.read(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH);
        }
        return new ResponseBuffers(replyHeader, bodyByteBuffer);
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        notNull("open", stream);
        return stream.getBuffer(size);
    }

    private class ResponseHeaderCallback implements SingleResultCallback<ByteBuf> {
        private final SingleResultCallback<ResponseBuffers> callback;

        public ResponseHeaderCallback(final SingleResultCallback<ResponseBuffers> callback) {
            this.callback = callback;
        }

        @Override
        public void onResult(final ByteBuf result, final Throwable t) {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                ReplyHeader replyHeader;
                ByteBufferBsonInput headerInputBuffer = new ByteBufferBsonInput(result);
                try {
                    replyHeader = new ReplyHeader(headerInputBuffer);
                } finally {
                    headerInputBuffer.close();
                }

                if (replyHeader.getMessageLength() == REPLY_HEADER_LENGTH) {
                    onSuccess(new ResponseBuffers(replyHeader, null));
                } else {
                    fillAndFlipBuffer(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH,
                                      new ResponseBodyCallback(replyHeader));
                }
            }
        }

        private void onSuccess(final ResponseBuffers responseBuffers) {

            if (responseBuffers == null) {
                callback.onResult(null, new MongoException("Unexpected empty response buffers"));
                return;
            }

            try {
                connectionListener.messageReceived(new ConnectionMessageReceivedEvent(getId(),
                                                                                      responseBuffers.getReplyHeader().getResponseTo(),
                                                                                      responseBuffers.getReplyHeader().getMessageLength()));
            } catch (Throwable t) {
                LOGGER.warn("Exception when trying to signal messageReceived to the connectionListener", t);
            }

            try {
                callback.onResult(responseBuffers, null);
            } catch (Throwable t) {
                LOGGER.warn("Exception calling callback", t);
            }
        }

        private class ResponseBodyCallback implements SingleResultCallback<ByteBuf> {
            private final ReplyHeader replyHeader;

            public ResponseBodyCallback(final ReplyHeader replyHeader) {
                this.replyHeader = replyHeader;
            }

            @Override
            public void onResult(final ByteBuf result, final Throwable t) {
                if (t != null) {
                    try {
                        callback.onResult(new ResponseBuffers(replyHeader, result), t);
                    } catch (Throwable tr) {
                        LOGGER.warn("Exception calling callback", tr);
                    }
                } else {
                    onSuccess(new ResponseBuffers(replyHeader, result));
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

    private void processPendingReads() {

        processPendingResults();

        if (readQueue.isEmpty()) {
            return;
        } else if (isClosed()) {
            readQueue.forEach(new Block<Map.Entry<Integer, SingleResultCallback<ResponseBuffers>>>() {
                @Override
                public void apply(final Map.Entry<Integer, SingleResultCallback<ResponseBuffers>> entry) {
                    SingleResultCallback<ResponseBuffers> callback = entry.getValue();
                    try {
                        callback.onResult(null, new MongoSocketClosedException("Cannot read from a closed stream", getServerAddress()));
                    } catch (Throwable t) {
                        LOGGER.warn("Exception calling callback", t);
                    }
                }
            });
        }

        if (readQueue.tryAcquire()) {
            fillAndFlipBuffer(REPLY_HEADER_LENGTH,
                    errorHandlingCallback(new ResponseHeaderCallback(new SingleResultCallback<ResponseBuffers>() {
                        @Override
                        public void onResult(final ResponseBuffers result, final Throwable t) {
                            readQueue.release();
                            if (result == null) {
                                close();
                                readQueue.forEach(new Block<Map.Entry<Integer, SingleResultCallback<ResponseBuffers>>>() {
                                    @Override
                                    public void apply(final Map.Entry<Integer, SingleResultCallback<ResponseBuffers>> entry) {
                                        if (LOGGER.isTraceEnabled()) {
                                            LOGGER.trace(format("Processing unknown failed message: %s. ([%s] %s)", entry.getKey(),
                                                    getId(), serverId));
                                        }
                                        SingleResultCallback<ResponseBuffers> callback = entry.getValue();
                                        try {
                                            callback.onResult(null, t);
                                        } catch (Throwable tr) {
                                            LOGGER.warn("Exception calling callback", tr);
                                        }
                                    }
                                });
                            } else {
                                readQueue.putResponse(result.getReplyHeader().getResponseTo(), new InternalStreamResponse(result, t));
                            }
                            processPendingReads();
                        }
                    }), LOGGER));
        }
    }

    private void processPendingWrites() {
        if (writeQueue.isEmpty()) {
            return;
        } else if (isClosed()) {
            writeQueue.closeAll();
        }

        if (writeQueue.tryAcquire()) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Writing locked ([%s] %s)", getId(), serverId));
            }
            final InternalStreamSendMessageAsync message = writeQueue.poll();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Sending Message: %s. ([%s] %s)", message.getMessageId(), getId(), serverId));
            }
            stream.writeAsync(message.getByteBuffers(), new AsyncCompletionHandler<Void>() {
                @Override
                public void completed(final Void v) {
                    writeQueue.release();
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace(format("Writing released (completed) ([%s] %s)", getId(), serverId));
                    }
                    try {
                        connectionListener.messagesSent(new ConnectionMessagesSentEvent(getId(),
                                                                                        message.getMessageId(),
                                                                                        getTotalRemaining(message.getByteBuffers())));
                    } catch (Throwable t) {
                        LOGGER.warn("Exception when trying to signal messagesSent to the connectionListener", t);
                    }
                    errorHandlingCallback(message.getCallback(), LOGGER).onResult(null, null);
                    processPendingWrites();
                }

                @Override
                public void failed(final Throwable t) {
                    writeQueue.release();
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace(format("Writing released (failed) ([%s] %s)", getId(), serverId));
                    }
                    close();
                    errorHandlingCallback(message.getCallback(), LOGGER).onResult(null, translateWriteException(t));
                    processPendingWrites();
                }
            });
        } else {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Writing lock unavailable or queue empty ([%s] %s)", getId(), serverId));
            }
        }
    }

    private void processPendingResults() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Processing results started ([%s] %s)", getId(), serverId));
        }
        readQueue.forEachResponse(new Function< Map.Entry<Integer, InternalStreamResponse>, Boolean>() {
            @Override
            public Boolean apply(final Map.Entry<Integer, InternalStreamResponse> entry) {
                int messageId = entry.getKey();
                InternalStreamResponse response = entry.getValue();
                SingleResultCallback<ResponseBuffers> callback = readQueue.remove(messageId);
                if (callback != null) {
                    if (response.hasError()) {
                        try {
                            callback.onResult(null, response.getError());
                        } catch (Throwable t) {
                            LOGGER.warn("Exception calling callback", t);
                        }
                    } else {
                        try {
                            callback.onResult(response.getResult(), null);
                        } catch (Throwable t) {
                            LOGGER.warn("Exception calling callback", t);
                        }
                    }
                    return true;
                } else {
                    return false;
                }
            }
        });
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Processing results ended ([%s] %s)", getId(), serverId));
        }
    }

}
