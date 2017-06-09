/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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
import com.mongodb.client.MongoDriverInformation;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ConnectionListener;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.EventListenerSettings;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.event.EventListenerSettingsHelper.createClusterListener;

/**
 * The default factory for cluster implementations.
 *
 * @since 3.0
 */
public final class DefaultClusterFactory implements ClusterFactory {

    @Override
    public Cluster create(final ClusterSettings settings, final ServerSettings serverSettings,
                          final ConnectionPoolSettings connectionPoolSettings, final StreamFactory streamFactory,
                          final StreamFactory heartbeatStreamFactory,
                          final List<MongoCredential> credentialList,
                          final ClusterListener clusterListener, final ConnectionPoolListener connectionPoolListener,
                          final ConnectionListener connectionListener) {
        return create(settings, serverSettings, connectionPoolSettings, streamFactory, heartbeatStreamFactory, credentialList,
                      clusterListener, connectionPoolListener, connectionListener, null);
    }

    /**
     * Creates a cluster with the given settings.  The cluster mode will be based on the mode from the settings.
     *
     * @param settings               the cluster settings
     * @param serverSettings         the server settings
     * @param connectionPoolSettings the connection pool settings
     * @param streamFactory          the stream factory
     * @param heartbeatStreamFactory the heartbeat stream factory
     * @param credentialList         the credential list
     * @param clusterListener        an optional listener for cluster-related events
     * @param connectionPoolListener an optional listener for connection pool-related events
     * @param connectionListener     an optional listener for connection-related events
     * @param commandListener        an optional listener for command-related events
     * @return the cluster
     * @since 3.1
     */
    public Cluster create(final ClusterSettings settings, final ServerSettings serverSettings,
                          final ConnectionPoolSettings connectionPoolSettings, final StreamFactory streamFactory,
                          final StreamFactory heartbeatStreamFactory, final List<MongoCredential> credentialList,
                          final ClusterListener clusterListener, final ConnectionPoolListener connectionPoolListener,
                          final ConnectionListener connectionListener, final CommandListener commandListener) {
        return create(settings, serverSettings, connectionPoolSettings, streamFactory, heartbeatStreamFactory, credentialList,
                clusterListener, connectionPoolListener, connectionListener, commandListener, null, null);
    }

    /**
     * Creates a cluster with the given settings.  The cluster mode will be based on the mode from the settings.
     *
     * @param clusterSettings               the cluster settings
     * @param serverSettings         the server settings
     * @param connectionPoolSettings the connection pool settings
     * @param streamFactory          the stream factory
     * @param heartbeatStreamFactory the heartbeat stream factory
     * @param credentialList         the credential list
     * @param clusterListener        an optional listener for cluster-related events
     * @param connectionPoolListener an optional listener for connection pool-related events
     * @param connectionListener     an optional listener for connection-related events
     * @param commandListener        an optional listener for command-related events
     * @param applicationName        an optional application name to associate with connections to the servers in this cluster
     * @param mongoDriverInformation the optional driver information associate with connections to the servers in this cluster
     * @return the cluster
     *
     * @since 3.4
     */
    public Cluster create(final ClusterSettings clusterSettings, final ServerSettings serverSettings,
                          final ConnectionPoolSettings connectionPoolSettings, final StreamFactory streamFactory,
                          final StreamFactory heartbeatStreamFactory,
                          final List<MongoCredential> credentialList,
                          final ClusterListener clusterListener, final ConnectionPoolListener connectionPoolListener,
                          final ConnectionListener connectionListener,
                          final CommandListener commandListener,
                          final String applicationName,
                          final MongoDriverInformation mongoDriverInformation) {
        if (clusterListener != null) {
            throw new IllegalArgumentException("Add cluster listener to ClusterSettings");
        }
        EventListenerSettings.Builder eventListenerSettingsBuilder = EventListenerSettings.builder();
        eventListenerSettingsBuilder.addCommandListener(commandListener);
        eventListenerSettingsBuilder.addConnectionListener(connectionListener);
        eventListenerSettingsBuilder.addConnectionPoolListener(connectionPoolListener);

        return create(clusterSettings, serverSettings, connectionPoolSettings, eventListenerSettingsBuilder.build(), streamFactory,
                heartbeatStreamFactory, credentialList, applicationName, mongoDriverInformation);
    }

    /**
     * Creates a cluster with the given settings.  The cluster mode will be based on the mode from the settings.
     *
     * @param clusterSettings        the cluster settings
     * @param serverSettings         the server settings
     * @param connectionPoolSettings the connection pool settings
     * @param eventListenerSettings  the event listener settings
     * @param streamFactory          the stream factory
     * @param heartbeatStreamFactory the heartbeat stream factory
     * @param credentialList         the credential list
     * @param applicationName        an optional application name to associate with connections to the servers in this cluster
     * @param mongoDriverInformation the optional driver information associate with connections to the servers in this cluster
     * @return the cluster
     *
     * @since 3.5
     */
    public Cluster create(final ClusterSettings clusterSettings, final ServerSettings serverSettings,
                          final ConnectionPoolSettings connectionPoolSettings, final EventListenerSettings eventListenerSettings,
                          final StreamFactory streamFactory, final StreamFactory heartbeatStreamFactory,
                          final List<MongoCredential> credentialList, final String applicationName,
                          final MongoDriverInformation mongoDriverInformation) {
        notNull("clusterSettings", clusterSettings);
        notNull("serverSettings", serverSettings);
        notNull("connectionPoolSettings", connectionPoolSettings);
        notNull("eventListenerSettings", eventListenerSettings);
        notNull("streamFactory", streamFactory);
        notNull("heartbeatStreamFactory", heartbeatStreamFactory);
        notNull("credentialList", credentialList);

        ClusterId clusterId = new ClusterId(clusterSettings.getDescription());

        ClusterableServerFactory serverFactory = new DefaultClusterableServerFactory(clusterId, clusterSettings, serverSettings,
                connectionPoolSettings, eventListenerSettings, streamFactory, heartbeatStreamFactory, credentialList, applicationName,
                mongoDriverInformation != null ? mongoDriverInformation : MongoDriverInformation.builder().build());

        ClusterListener clusterListener = createClusterListener(eventListenerSettings, clusterSettings);
        if (clusterSettings.getMode() == ClusterConnectionMode.SINGLE) {
            return new SingleServerCluster(clusterId, clusterSettings, serverFactory, clusterListener);
        } else if (clusterSettings.getMode() == ClusterConnectionMode.MULTIPLE) {
            return new MultiServerCluster(clusterId, clusterSettings, serverFactory, clusterListener);
        } else {
            throw new UnsupportedOperationException("Unsupported cluster mode: " + clusterSettings.getMode());
        }
    }
}
