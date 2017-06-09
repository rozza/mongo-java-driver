/*
 * Copyright 2008-2016 MongoDB, Inc.
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
import com.mongodb.client.MongoDriverInformation;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ConnectionListener;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.EventListenerSettings;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerMonitorListener;

import java.util.List;

import static com.mongodb.internal.event.EventListenerSettingsHelper.createCommandListener;
import static com.mongodb.internal.event.EventListenerSettingsHelper.createConnectionListener;
import static com.mongodb.internal.event.EventListenerSettingsHelper.createConnectionPoolListener;
import static com.mongodb.internal.event.EventListenerSettingsHelper.createServerListener;
import static com.mongodb.internal.event.EventListenerSettingsHelper.createServerMonitorListener;

class DefaultClusterableServerFactory implements ClusterableServerFactory {
    private final ClusterId clusterId;
    private final ClusterSettings clusterSettings;
    private final ServerSettings serverSettings;
    private final ConnectionPoolSettings connectionPoolSettings;
    private final EventListenerSettings eventListenerSettings;
    private final StreamFactory streamFactory;
    private final List<MongoCredential> credentialList;
    private final StreamFactory heartbeatStreamFactory;
    private final String applicationName;
    private final MongoDriverInformation mongoDriverInformation;

    DefaultClusterableServerFactory(final ClusterId clusterId, final ClusterSettings clusterSettings, final ServerSettings serverSettings,
                                    final ConnectionPoolSettings connectionPoolSettings, final EventListenerSettings eventListenerSettings,
                                    final StreamFactory streamFactory, final StreamFactory heartbeatStreamFactory,
                                    final List<MongoCredential> credentialList,
                                    final String applicationName, final MongoDriverInformation mongoDriverInformation) {
        this.clusterId = clusterId;
        this.clusterSettings = clusterSettings;
        this.serverSettings = serverSettings;
        this.connectionPoolSettings = connectionPoolSettings;
        this.streamFactory = streamFactory;
        this.credentialList = credentialList;
        this.eventListenerSettings = eventListenerSettings;
        this.heartbeatStreamFactory = heartbeatStreamFactory;
        this.applicationName = applicationName;
        this.mongoDriverInformation = mongoDriverInformation;
    }

    @Override
    public ClusterableServer create(final ServerAddress serverAddress, final ServerListener initialServerListener) {
        ConnectionListener connectionListener = createConnectionListener(eventListenerSettings);
        ConnectionPoolListener connectionPoolListener = createConnectionPoolListener(eventListenerSettings);
        CommandListener commandListener = createCommandListener(eventListenerSettings);
        ServerListener serverListener = createServerListener(eventListenerSettings, serverSettings, initialServerListener);
        ServerMonitorListener serverMonitorListener = createServerMonitorListener(eventListenerSettings, serverSettings);

        ConnectionPool connectionPool = new DefaultConnectionPool(new ServerId(clusterId, serverAddress),
                new InternalStreamConnectionFactory(streamFactory, credentialList, connectionListener,
                        applicationName, mongoDriverInformation), connectionPoolSettings, connectionPoolListener);

        ServerMonitorFactory serverMonitorFactory =
            new DefaultServerMonitorFactory(new ServerId(clusterId, serverAddress), serverSettings, serverMonitorListener,
                    new InternalStreamConnectionFactory(heartbeatStreamFactory, credentialList, connectionListener, applicationName,
                            mongoDriverInformation), connectionPool);

        return new DefaultServer(new ServerId(clusterId, serverAddress), clusterSettings.getMode(), connectionPool,
                new DefaultConnectionFactory(), serverMonitorFactory, serverListener, commandListener);
    }

    @Override
    public ServerSettings getSettings() {
        return serverSettings;
    }

}
