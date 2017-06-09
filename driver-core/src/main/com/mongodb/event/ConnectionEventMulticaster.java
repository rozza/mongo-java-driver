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

package com.mongodb.event;

import com.mongodb.annotations.Immutable;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;

/**
 * A multicaster for connection events. Any exception thrown by one of the listeners will be caught and not re-thrown, but may be
 * logged.
 *
 * @since 3.5
 */
@Immutable
public final class ConnectionEventMulticaster implements ConnectionListener {
    private static final Logger LOGGER = Loggers.getLogger("protocol.event");

    private final List<ConnectionListener> connectionListeners;

    /**
     * Construct an instance with the given list of connection pool listeners
     *
     * @param connectionListeners the non-null list of connection pool listeners, none of which may be null
     */
    public ConnectionEventMulticaster(final List<ConnectionListener> connectionListeners) {
        notNull("connectionPoolListener", connectionListeners);
        isTrue("All ConnectionListener instances are non-null", !connectionListeners.contains(null));
        this.connectionListeners = new ArrayList<ConnectionListener>(connectionListeners);
    }

    /**
     * Gets the connection pool listeners.
     *
     * @return the unmodifiable set of connection pool listeners
     */
    public List<ConnectionListener> getConnectionListeners() {
        return unmodifiableList(connectionListeners);
    }

    @Override
    public void connectionOpened(final ConnectionOpenedEvent event) {
        for (final ConnectionListener cur : connectionListeners) {
            try {
                cur.connectionOpened(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection opened event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void connectionClosed(final ConnectionClosedEvent event) {
        for (final ConnectionListener cur : connectionListeners) {
            try {
                cur.connectionClosed(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection closed event to listener %s", cur), e);
                }
            }
        }
    }
}
