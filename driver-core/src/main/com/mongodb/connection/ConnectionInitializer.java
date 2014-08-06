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
import com.mongodb.event.ConnectionEvent;
import com.mongodb.event.ConnectionListener;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.assertions.Assertions.notNull;

class ConnectionInitializer {
    private final AtomicInteger incrementingId = new AtomicInteger();
    private final String clusterId;
    private final Stream stream;
    private final ConnectionListener connectionListener;
    private final List<MongoCredential> credentialList;
    private final InternalConnection connection;

    private String id;
    private AtomicBoolean initialized;

    ConnectionInitializer(final String clusterId, final Stream stream, final List<MongoCredential> credentialList,
                          final ConnectionListener connectionListener, final InternalConnection connection) {
        this.clusterId = notNull("clusterId", clusterId);
        this.stream = notNull("stream", stream);
        this.connectionListener = notNull("connectionListener", connectionListener);
        this.connection = notNull("connection", connection);
        notNull("credentialList", credentialList);
        this.credentialList = new ArrayList<MongoCredential>(credentialList);
        initialized = new AtomicBoolean(false);
    }

    public String getId() {
        return id;
    }

    public boolean initialized() {
        return initialized.get();
    }

    public void initialize() {
        // TODO - Asynchronize
        if (!initialized()) {
            initialized.set(true);
            initializeConnectionId();
            authenticateAll();

            // try again if there was an exception calling getlasterror before authenticating
            if (id.contains("*")) {
                initializeConnectionId();
            }
            connectionListener.connectionOpened(new ConnectionEvent(clusterId, stream.getAddress(), getId()));
        }
    }

    private void initializeConnectionId() {
        BsonDocument response = CommandHelper.executeCommandWithoutCheckingForFailure("admin",
                                                                                      new BsonDocument("getlasterror", new BsonInt32(1)),
                                                                                      connection);
        id = "conn" + (response.containsKey("connectionId")
                       ? response.getNumber("connectionId").intValue()
                       : "*" + incrementingId.incrementAndGet() + "*");
    }

    private void authenticateAll() {
        for (final MongoCredential cur : credentialList) {
            createAuthenticator(cur).authenticate();
        }
    }

    private Authenticator createAuthenticator(final MongoCredential credential) {
        switch (credential.getAuthenticationMechanism()) {
            case MONGODB_CR:
                return new NativeAuthenticator(credential, connection);
            case GSSAPI:
                return new GSSAPIAuthenticator(credential, connection);
            case PLAIN:
                return new PlainAuthenticator(credential, connection);
            case MONGODB_X509:
                return new X509Authenticator(credential, connection);
            default:
                throw new IllegalArgumentException("Unsupported authentication protocol: " + credential.getMechanism());
        }
    }
}
