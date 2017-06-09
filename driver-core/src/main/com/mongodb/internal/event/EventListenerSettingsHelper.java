/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.event;

import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.event.ClusterEventMulticaster;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterListenerAdapter;
import com.mongodb.event.CommandEventMulticaster;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ConnectionEventMulticaster;
import com.mongodb.event.ConnectionListener;
import com.mongodb.event.ConnectionListenerAdapter;
import com.mongodb.event.ConnectionPoolEventMulticaster;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.EventListenerSettings;
import com.mongodb.event.ServerEventMulticaster;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerListenerAdapter;
import com.mongodb.event.ServerMonitorEventMulticaster;
import com.mongodb.event.ServerMonitorListener;
import com.mongodb.event.ServerMonitorListenerAdapter;
import com.mongodb.management.JMXConnectionPoolListener;

import java.util.ArrayList;
import java.util.List;

public final class EventListenerSettingsHelper {

    public static void addAllClusterListeners(final EventListenerSettings.Builder builder, final List<ClusterListener> clusterListeners) {
        for (ClusterListener clusterListener : clusterListeners) {
            builder.addClusterListener(clusterListener);
        }
    }

    public static void addAllCommandListeners(final EventListenerSettings.Builder builder, final List<CommandListener> listeners) {
        for (CommandListener listener : listeners) {
            builder.addCommandListener(listener);
        }
    }

    public static void addAllConnectionListeners(final EventListenerSettings.Builder builder, final List<ConnectionListener> listeners) {
        for (ConnectionListener listener : listeners) {
            builder.addConnectionListener(listener);
        }
    }

    public static void addAllConnectionPoolListeners(final EventListenerSettings.Builder builder,
                                                     final List<ConnectionPoolListener> listeners) {
        for (ConnectionPoolListener listener : listeners) {
            builder.addConnectionPoolListener(listener);
        }
    }

    public static void addAllServerListeners(final EventListenerSettings.Builder builder, final List<ServerListener> listeners) {
        for (ServerListener listener : listeners) {
            builder.addServerListener(listener);
        }
    }

    public static void addAllServerMonitorListeners(final EventListenerSettings.Builder builder,
                                                    final List<ServerMonitorListener> listeners) {
        for (ServerMonitorListener listener : listeners) {
            builder.addServerMonitorListener(listener);
        }
    }

    @SuppressWarnings("deprecation")
    public static ClusterListener createClusterListener(final EventListenerSettings eventListenerSettings,
                                                        final ClusterSettings clusterSettings) {
        List<ClusterListener> clusterListeners = new ArrayList<ClusterListener>();
        clusterListeners.addAll(clusterSettings.getClusterListeners());
        clusterListeners.addAll(eventListenerSettings.getClusterListeners());

        switch (clusterListeners.size()) {
            case 0:
                return new NoOpClusterListener();
            case 1:
                return clusterListeners.get(0);
            default:
                return new ClusterEventMulticaster(clusterListeners);
        }
    }

    public static CommandListener createCommandListener(final EventListenerSettings eventListenerSettings) {
        switch (eventListenerSettings.getCommandListeners().size()) {
            case 0:
                return null;
            case 1:
                return eventListenerSettings.getCommandListeners().get(0);
            default:
                return new CommandEventMulticaster(eventListenerSettings.getCommandListeners());
        }
    }

    public static ConnectionListener createConnectionListener(final EventListenerSettings eventListenerSettings) {
        switch (eventListenerSettings.getConnectionListeners().size()) {
            case 0:
                return new NoOpConnectionListener();
            case 1:
                return eventListenerSettings.getConnectionListeners().get(0);
            default:
                return new ConnectionEventMulticaster(eventListenerSettings.getConnectionListeners());
        }
    }

    public static ConnectionPoolListener createConnectionPoolListener(final EventListenerSettings eventListenerSettings) {
        switch (eventListenerSettings.getConnectionPoolListeners().size()) {
            case 0:
                return new JMXConnectionPoolListener();
            case 1:
                return eventListenerSettings.getConnectionPoolListeners().get(0);
            default:
                return new ConnectionPoolEventMulticaster(eventListenerSettings.getConnectionPoolListeners());
        }
    }

    @SuppressWarnings("deprecation")
    public static ServerMonitorListener createServerMonitorListener(final EventListenerSettings eventListenerSettings,
                                                                    final ServerSettings serverSettings) {
        List<ServerMonitorListener> serverMonitorListeners = new ArrayList<ServerMonitorListener>();
        serverMonitorListeners.addAll(serverSettings.getServerMonitorListeners());
        serverMonitorListeners.addAll(eventListenerSettings.getServerMonitorListeners());

        switch (serverMonitorListeners.size()) {
            case 0:
                return new NoOpServerMonitorListener();
            case 1:
                return serverMonitorListeners.get(0);
            default:
                return new ServerMonitorEventMulticaster(serverMonitorListeners);
        }
    }

    @SuppressWarnings("deprecation")
    public static ServerListener createServerListener(final EventListenerSettings eventListenerSettings,
                                                      final ServerSettings serverSettings,
                                                      final ServerListener initialServerListener) {
        List<ServerListener> serverListeners = new ArrayList<ServerListener>();
        if (initialServerListener != null) {
            serverListeners.add(initialServerListener);
        }
        serverListeners.addAll(eventListenerSettings.getServerListeners());
        serverListeners.addAll(serverSettings.getServerListeners());

        switch (serverListeners.size()) {
            case 0:
                return new NoOpServerListener();
            case 1:
                return serverListeners.get(0);
            default:
                return new ServerEventMulticaster(serverListeners);
        }
    }

    static final class NoOpConnectionListener extends ConnectionListenerAdapter {
    }

    static final class NoOpServerListener extends ServerListenerAdapter {
    }

    static final class NoOpServerMonitorListener extends ServerMonitorListenerAdapter {
    }

    static final class NoOpClusterListener extends ClusterListenerAdapter {
    }

    private EventListenerSettingsHelper() {
    }
}
