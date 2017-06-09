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

package com.mongodb.connection;

import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterListenerAdapter;
import com.mongodb.event.ConnectionListener;
import com.mongodb.event.ConnectionListenerAdapter;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ConnectionPoolListenerAdapter;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerListenerAdapter;
import com.mongodb.event.ServerMonitorListener;
import com.mongodb.event.ServerMonitorListenerAdapter;

public final class EventListeners {

    static final ClusterListener NOOP_CLUSTER_LISTENER = new ClusterListenerAdapter() {
    };

    static final ConnectionListener NOOP_CONNECTION_LISTENER = new ConnectionListenerAdapter() {
    };

    static final ConnectionPoolListener NOOP_CONNECTION_POOL_LISTENER = new ConnectionPoolListenerAdapter() {
    };

    static final ServerListener NOOP_SERVER_LISTENER = new ServerListenerAdapter() {
    };

    static final ServerMonitorListener NOOP_SERVER_MONITOR_LISTENER = new ServerMonitorListenerAdapter() {
    };

    private EventListeners() {
    }
}
