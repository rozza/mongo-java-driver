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

import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
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
    private volatile boolean isClosed;

    InternalStreamConnection(final String clusterId, final Stream stream, final List<MongoCredential> credentialList,
                             final ConnectionListener connectionListener) {
        this.clusterId = notNull("clusterId", clusterId);
        this.stream = notNull("stream", stream);
        this.connectionListener = notNull("connectionListener", connectionListener);
        this.connectionInitializer = new ConnectionInitializer(clusterId, stream, credentialList, connectionListener, this);
        this.streamPipeline = new StreamPipeline(clusterId, stream, connectionListener, this);
        connectionInitializer.initialize();
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
        streamPipeline.sendMessage(byteBuffers, lastRequestId);
    }

    @Override
    public ResponseBuffers receiveMessage(final int responseTo) {
        return streamPipeline.receiveMessage(responseTo);
    }

    @Override
    public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
        streamPipeline.sendMessageAsync(byteBuffers, lastRequestId, callback);
    }

    @Override
    public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
        streamPipeline.receiveMessageAsync(responseTo, callback);
    }

}
