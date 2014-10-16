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
import com.mongodb.event.ConnectionListener;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

class InternalStreamConnectionFactory implements InternalConnectionFactory {
    private final String clusterId;
    private final StreamFactory streamFactory;
    private final List<MongoCredential> credentialList;
    private final ConnectionListener connectionListener;

    public InternalStreamConnectionFactory(final String clusterId, final StreamFactory streamFactory,
                                           final List<MongoCredential> credentialList, final ConnectionListener connectionListener) {
        this.clusterId = notNull("clusterId", clusterId);
        this.streamFactory = notNull("streamFactory", streamFactory);
        this.credentialList = notNull("credentialList", credentialList);
        this.connectionListener = notNull("connectionListener", connectionListener);
    }

    @Override
    public InternalConnection create(final ServerAddress serverAddress) {
        Stream stream = streamFactory.create(serverAddress);
        return new InternalStreamConnection(clusterId,
                                            stream,
                                            connectionListener,
                                            new DefaultConnectionInitializerFactory(serverAddress, credentialList));
    }

}