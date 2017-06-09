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
 * A multicaster for connection pool events. Any exception thrown by one of the listeners will be caught and not re-thrown, but may be
 * logged.
 *
 * @since 3.5
 */
@Immutable
public final class ConnectionPoolEventMulticaster implements ConnectionPoolListener {
    private static final Logger LOGGER = Loggers.getLogger("protocol.event");

    private final List<ConnectionPoolListener> connectionPoolListeners;

    /**
     * Construct an instance with the given list of connection pool listeners
     *
     * @param connectionPoolListeners the non-null list of connection pool listeners, none of which may be null
     */
    public ConnectionPoolEventMulticaster(final List<ConnectionPoolListener> connectionPoolListeners) {
        notNull("connectionPoolListener", connectionPoolListeners);
        isTrue("All ConnectionPoolListener instances are non-null", !connectionPoolListeners.contains(null));
        this.connectionPoolListeners = new ArrayList<ConnectionPoolListener>(connectionPoolListeners);
    }

    /**
     * Gets the connection pool listeners.
     *
     * @return the unmodifiable set of connection pool listeners
     */
    public List<ConnectionPoolListener> getConnectionPoolListeners() {
        return unmodifiableList(connectionPoolListeners);
    }

    @Override
    public void connectionPoolOpened(final ConnectionPoolOpenedEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.connectionPoolOpened(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool opened event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void connectionPoolClosed(final ConnectionPoolClosedEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.connectionPoolClosed(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool closed event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void connectionCheckedOut(final ConnectionCheckedOutEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.connectionCheckedOut(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool checked out event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void connectionCheckedIn(final ConnectionCheckedInEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.connectionCheckedIn(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool checked in event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void waitQueueEntered(final ConnectionPoolWaitQueueEnteredEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.waitQueueEntered(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool wait queue entered event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void waitQueueExited(final ConnectionPoolWaitQueueExitedEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.waitQueueExited(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool wait queue exited event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void connectionAdded(final ConnectionAddedEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.connectionAdded(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool connection added event to listener %s", cur), e);
                }
            }
        }
    }

    @Override
    public void connectionRemoved(final ConnectionRemovedEvent event) {
        for (ConnectionPoolListener cur : connectionPoolListeners) {
            try {
                cur.connectionRemoved(event);
            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Exception thrown raising connection pool connection removed event to listener %s", cur), e);
                }
            }
        }
    }
}
