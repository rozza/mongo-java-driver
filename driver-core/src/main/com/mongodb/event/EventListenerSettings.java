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

package com.mongodb.event;

import com.mongodb.annotations.NotThreadSafe;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Collections.unmodifiableList;

/**
 * Settings for the various event listeners.
 *
 * @since 3.5
 */
public final class EventListenerSettings {
    private final List<ClusterListener> clusterListeners;
    private final List<CommandListener> commandListeners;
    private final List<ConnectionListener> connectionListeners;
    private final List<ConnectionPoolListener> connectionPoolListeners;
    private final List<ServerListener> serverListeners;
    private final List<ServerMonitorListener> serverMonitorListeners;

    /**
     * Creates a builder instance.
     *
     * @return a builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder instance.
     *
     * @param settings existing EventListenerSettings to default the builder settings on.
     * @return a builder
     */
    public static Builder builder(final EventListenerSettings settings) {
        return new Builder(settings);
    }

    /**
     * Gets the list of added {@code ClusterListener}. The default is an empty list.
     *
     * @return the unmodifiable list of cluster listeners
     */
    public List<ClusterListener> getClusterListeners() {
        return clusterListeners;
    }

    /**
     * Gets the list of added {@code CommandListener}.
     *
     * <p>Default is an empty list.</p>
     *
     * @return the unmodifiable list of command listeners
     */
    public List<CommandListener> getCommandListeners() {
        return commandListeners;
    }

    /**
     * Gets the list of added {@code ConnectionListener}. The default is an empty list.
     *
     * @return the unmodifiable list of connection listeners
     */
    public List<ConnectionListener> getConnectionListeners() {
        return connectionListeners;
    }

    /**
     * Gets the list of added {@code ConnectionPoolListener}. The default is an empty list.
     *
     * @return the unmodifiable list of connection pool listeners
     */
    public List<ConnectionPoolListener> getConnectionPoolListeners() {
        return connectionPoolListeners;
    }

    /**
     * Gets the list of added {@code ServerListener}. The default is an empty list.
     *
     * @return the unmodifiable list of server listeners
     */
    public List<ServerListener> getServerListeners() {
        return serverListeners;
    }

    /**
     * Gets the list of added {@code ServerMonitorListener}. The default is an empty list.
     *
     * @return the unmodifiable list of server monitor listeners
     */
    public List<ServerMonitorListener> getServerMonitorListeners() {
        return serverMonitorListeners;
    }

    /**
     * A builder for the event listener settings.
     */
    @NotThreadSafe
    public static class Builder {
        private final List<ClusterListener> clusterListeners = new ArrayList<ClusterListener>();
        private final List<CommandListener> commandListeners = new ArrayList<CommandListener>();
        private final List<ConnectionListener> connectionListeners = new ArrayList<ConnectionListener>();
        private final List<ConnectionPoolListener> connectionPoolListeners = new ArrayList<ConnectionPoolListener>();
        private final List<ServerListener> serverListeners = new ArrayList<ServerListener>();
        private final List<ServerMonitorListener> serverMonitorListeners = new ArrayList<ServerMonitorListener>();

        /**
         * Creates a Builder.
         */
        Builder() {
        }

        /**
         * Creates a Builder from an existing EventListenerSettings.
         * @param settings create a builder from existing settings
         */
        Builder(final EventListenerSettings settings) {
            clusterListeners.addAll(settings.getClusterListeners());
            commandListeners.addAll(settings.getCommandListeners());
            connectionListeners.addAll(settings.getConnectionListeners());
            connectionPoolListeners.addAll(settings.getConnectionPoolListeners());
            serverListeners.addAll(settings.getServerListeners());
            serverMonitorListeners.addAll(settings.getServerMonitorListeners());
        }

        /**
         * Adds the given cluster listener.
         *
         * @param clusterListener the non-null cluster listener
         * @return this
         */
        public Builder addClusterListener(final ClusterListener clusterListener) {
            notNull("clusterListener", clusterListener);
            clusterListeners.add(clusterListener);
            return this;
        }

        /**
         * Adds the given command listener.
         *
         * @param commandListener the non-null command listener
         * @return this
         * @since 3.1
         */
        public Builder addCommandListener(final CommandListener commandListener) {
            notNull("commandListener", commandListener);
            commandListeners.add(commandListener);
            return this;
        }

        /**
         * Adds the given connection listener.
         *
         * @param connectionListener the non-null connection listener
         * @return this
         */
        public Builder addConnectionListener(final ConnectionListener connectionListener) {
            notNull("connectionListener", connectionListener);
            connectionListeners.add(connectionListener);
            return this;
        }

        /**
         * Adds the given connection pool listener.
         *
         * @param connectionPoolListener the non-null connection pool listener
         * @return this
         */
        public Builder addConnectionPoolListener(final ConnectionPoolListener connectionPoolListener) {
            notNull("connectionPoolListener", connectionPoolListener);
            connectionPoolListeners.add(connectionPoolListener);
            return this;
        }

        /**
         * Adds the given server listener.
         *
         * @param serverListener the non-null server listener
         * @return this
         */
        public Builder addServerListener(final ServerListener serverListener) {
            notNull("serverListener", serverListener);
            serverListeners.add(serverListener);
            return this;
        }

        /**
         * Adds the given server monitor listener.
         *
         * @param serverMonitorListener the non-null server monitor listener
         * @return this
         */
        public Builder addServerMonitorListener(final ServerMonitorListener serverMonitorListener) {
            notNull("serverMonitorListener", serverMonitorListener);
            serverMonitorListeners.add(serverMonitorListener);
            return this;
        }

        /**
         * Build an instance of EventListenerSettings.
         *
         * @return the settings from this builder
         */
        public EventListenerSettings build() {
            return new EventListenerSettings(this);
        }
    }

    @Override
    public String toString() {
        return "EventListenerSettings{"
                + "clusterListeners=" + clusterListeners
                + ", commandListeners=" + commandListeners
                + ", connectionListeners=" + connectionListeners
                + ", connectionPoolListeners=" + connectionPoolListeners
                + ", serverListeners=" + serverListeners
                + ", serverMonitorListeners=" + serverMonitorListeners
                + "}";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EventListenerSettings that = (EventListenerSettings) o;

        if (!getClusterListeners().equals(that.getClusterListeners())) {
            return false;
        }
        if (!getCommandListeners().equals(that.getCommandListeners())) {
            return false;
        }
        if (!getConnectionListeners().equals(that.getConnectionListeners())) {
            return false;
        }
        if (!getConnectionPoolListeners().equals(that.getConnectionPoolListeners())) {
            return false;
        }
        if (!getServerListeners().equals(that.getServerListeners())) {
            return false;
        }
        if (!getServerMonitorListeners().equals(that.getServerMonitorListeners())) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getClusterListeners().hashCode();
        result = 31 * result + getCommandListeners().hashCode();
        result = 31 * result + getConnectionListeners().hashCode();
        result = 31 * result + getConnectionPoolListeners().hashCode();
        result = 31 * result + getServerListeners().hashCode();
        result = 31 * result + getServerMonitorListeners().hashCode();
        return result;
    }

    private EventListenerSettings(final Builder builder) {
         clusterListeners = unmodifiableList(builder.clusterListeners);
         commandListeners = unmodifiableList(builder.commandListeners);
         connectionListeners = unmodifiableList(builder.connectionListeners);
         connectionPoolListeners = unmodifiableList(builder.connectionPoolListeners);
         serverListeners = unmodifiableList(builder.serverListeners);
         serverMonitorListeners = unmodifiableList(builder.serverMonitorListeners);
    }
}
