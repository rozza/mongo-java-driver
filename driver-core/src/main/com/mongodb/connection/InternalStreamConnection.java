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

package com.mongodb.connection;

import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.diagnostics.Loggers;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.event.ConnectionEvent;
import com.mongodb.event.ConnectionListener;
import org.bson.ByteBuf;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

class InternalStreamConnection implements InternalConnection {

    private final String clusterId;
    private final Stream stream;
    private final ConnectionListener connectionListener;
    private final StreamPipeline streamPipeline;
    private final ConnectionInitializer connectionInitializer;
    private volatile boolean initializeCalled;
    private volatile boolean isClosed;

    static final Logger LOGGER = Loggers.getLogger("InternalStreamConnection");

    InternalStreamConnection(final String clusterId, final Stream stream, final ConnectionInitializer connectionInitializer,
                             final ConnectionListener connectionListener) {
        this.clusterId = notNull("clusterId", clusterId);
        this.stream = notNull("stream", stream);
        this.connectionListener = notNull("connectionListener", connectionListener);
        this.connectionInitializer = connectionInitializer;
        this.streamPipeline = new StreamPipeline(clusterId, stream, connectionListener, this);
        isClosed = false;
        initializeCalled = false;
    }

    @Override
    public void close() {
        isClosed = true;
        stream.close();
        connectionListener.connectionClosed(new ConnectionEvent(clusterId, stream.getAddress(), getId()));
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public ServerAddress getServerAddress() {
        return stream.getAddress();
    }

    @Override
    public String getId() {
        return connectionInitializer.getId();
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return stream.getBuffer(size);
    }

    public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
        if (!initializeCalled) {
            connectionInitializer.initialize();
            streamPipeline.initialized();
            initializeCalled = true;
        }
        streamPipeline.sendMessage(byteBuffers, lastRequestId);
    }

    @Override
    public ResponseBuffers receiveMessage(final int responseTo) {
        return streamPipeline.receiveMessage(responseTo);
    }

    @Override
    public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
        if (!initializeCalled) {
            connectionInitializer.initialize(new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final MongoException e) {
                    streamPipeline.initialized();
                    try {
                        connectionListener.connectionOpened(new ConnectionEvent(clusterId, stream.getAddress(), getId()));
                    } catch (Throwable t) {
                        LOGGER.warn("Exception when trying to signal messagesSent to the connectionListener", t);
                    }
                }
            });
            initializeCalled = true;
        }
        streamPipeline.sendMessageAsync(byteBuffers, lastRequestId, callback);
    }

    @Override
    public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
        streamPipeline.receiveMessageAsync(responseTo, callback);
    }
}
