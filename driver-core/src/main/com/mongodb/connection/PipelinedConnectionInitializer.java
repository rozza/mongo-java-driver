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
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import com.mongodb.diagnostics.Loggers;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.event.ConnectionEvent;
import com.mongodb.event.ConnectionListener;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.assertions.Assertions.notNull;

class PipelinedConnectionInitializer implements ConnectionInitializer, InternalConnection {
    private static final AtomicInteger INCREMENTING_ID = new AtomicInteger();
    private final String clusterId;
    private final Stream stream;
    private final ConnectionListener connectionListener;
    private final List<MongoCredential> credentialList;
    private final StreamPipeline streamPipeline;
    private volatile boolean isClosed;

    static final Logger LOGGER = Loggers.getLogger("ConnectionInitializer");

    private String id;

    PipelinedConnectionInitializer(final String clusterId, final Stream stream, final List<MongoCredential> credentialList,
                                   final ConnectionListener connectionListener) {
        this.clusterId = notNull("clusterId", clusterId);
        this.stream = notNull("stream", stream);
        this.connectionListener = notNull("connectionListener", connectionListener);
        notNull("credentialList", credentialList);
        this.credentialList = new ArrayList<MongoCredential>(credentialList);
        this.streamPipeline = new StreamPipeline(clusterId, stream, connectionListener, this, true);
    }

    @Override
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

    @Override
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
    public void initialize() {
        initializeConnectionId();
        authenticateAll();

        // try again if there was an exception calling getlasterror before authenticating
        if (id.contains("*")) {
            initializeConnectionId();
        }
    }

    @Override
    public void initialize(final SingleResultCallback<Void> initializationFuture) {
        try {
            initialize();
        } catch (Throwable t) {
            LOGGER.warn("Exception initializing the connection", t);
            initializationFuture.onResult(null, new MongoException(t.toString(), t));
            close();
            return;
        }
        initializationFuture.onResult(null, null);
    }

    private void initializeConnectionId() throws MongoInternalException {
        BsonDocument response = CommandHelper.executeCommandWithoutCheckingForFailure("admin",
                                                                                      new BsonDocument("getlasterror", new BsonInt32(1)),
                                                                                      this);
        id = "conn" + (response.containsKey("connectionId")
                       ? response.getNumber("connectionId").intValue()
                       : "*" + INCREMENTING_ID.incrementAndGet() + "*");
    }

    private void authenticateAll() throws MongoSecurityException, IllegalArgumentException {
        for (final MongoCredential cur : credentialList) {
            createAuthenticator(cur).authenticate();
        }
    }

    private Authenticator createAuthenticator(final MongoCredential credential) throws MongoSecurityException, IllegalArgumentException {
        switch (credential.getAuthenticationMechanism()) {
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

}
