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

import com.mongodb.event.ClusterEventMulticaster;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterListenerAdapter;
import com.mongodb.event.CommandEventMulticaster;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ConnectionPoolEventMulticaster;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ConnectionPoolListenerAdapter;
import com.mongodb.event.ServerEventMulticaster;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerListenerAdapter;
import com.mongodb.event.ServerMonitorEventMulticaster;
import com.mongodb.event.ServerMonitorListener;
import com.mongodb.event.ServerMonitorListenerAdapter;
import com.mongodb.management.JMXConnectionPoolListener;

import java.util.ArrayList;
import java.util.List;

public final class EventListenerHelper {

    public static ClusterListener getClusterListener(final List<ClusterListener> clusterListeners) {
        switch (clusterListeners.size()) {
            case 0:
                return NO_OP_CLUSTER_LISTENER;
            case 1:
                return clusterListeners.get(0);
            default:
                return new ClusterEventMulticaster(clusterListeners);
        }
    }

    public static ClusterListener getClusterListenerOrDefault(final ClusterListener clusterListener) {
        return clusterListener != null ? clusterListener : NO_OP_CLUSTER_LISTENER;
    }


    public static CommandListener getCommandListener(final List<CommandListener> commandListeners) {
        switch (commandListeners.size()) {
            case 0:
                return null;
            case 1:
                return commandListeners.get(0);
            default:
                return new CommandEventMulticaster(commandListeners);
        }
    }

    public static ConnectionPoolListener getConnectionPoolListener(final List<ConnectionPoolListener> connectionPoolListeners) {
        switch (connectionPoolListeners.size()) {
            case 0:
                return new JMXConnectionPoolListener();
            case 1:
                return connectionPoolListeners.get(0);
            default:
                return new ConnectionPoolEventMulticaster(connectionPoolListeners);
        }
    }

    public static ConnectionPoolListener getConnectionPoolListenerOrDefault(final ConnectionPoolListener connectionPoolListener) {
        return connectionPoolListener != null ? connectionPoolListener : NO_OP_CONNECTION_POOL_LISTENER;
    }

    public static ServerMonitorListener getServerMonitorListener(final List<ServerMonitorListener> serverMonitorListeners) {
        switch (serverMonitorListeners.size()) {
            case 0:
                return NO_OP_SERVER_MONITOR_LISTENER;
            case 1:
                return serverMonitorListeners.get(0);
            default:
                return new ServerMonitorEventMulticaster(serverMonitorListeners);
        }
    }

    public static ServerMonitorListener getServerMonitorListenerOrDefault(final ServerMonitorListener serverMonitorListener) {
        return serverMonitorListener != null ? serverMonitorListener : NO_OP_SERVER_MONITOR_LISTENER;
    }

    public static ServerListener createServerListener(final List<ServerListener> serverListeners,
                                                      final ServerListener initialServerListener) {
        List<ServerListener> mergedServerListeners = new ArrayList<ServerListener>();
        if (initialServerListener != null) {
            mergedServerListeners.add(initialServerListener);
        }
        mergedServerListeners.addAll(serverListeners);

        switch (mergedServerListeners.size()) {
            case 0:
                return NO_OP_SERVER_LISTENER;
            case 1:
                return mergedServerListeners.get(0);
            default:
                return new ServerEventMulticaster(mergedServerListeners);
        }
    }

    static final ServerListener NO_OP_SERVER_LISTENER = new ServerListenerAdapter() {
    };

    static final ServerMonitorListener NO_OP_SERVER_MONITOR_LISTENER = new ServerMonitorListenerAdapter() {
    };

    static final ClusterListener NO_OP_CLUSTER_LISTENER = new ClusterListenerAdapter() {
    };

    static final ConnectionPoolListener NO_OP_CONNECTION_POOL_LISTENER = new ConnectionPoolListenerAdapter() {
    };

    private EventListenerHelper() {
    }
}
