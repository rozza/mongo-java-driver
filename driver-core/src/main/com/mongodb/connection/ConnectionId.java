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

import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;

/**
 * A description of a connection to a MongoDB server.
 *
 * @since 3.0
 */
public class ConnectionId {
    private static final AtomicInteger INCREMENTING_ID = new AtomicInteger();

    private final int localValue;
    private volatile Integer serverValue;

    ConnectionId() {
        localValue = INCREMENTING_ID.incrementAndGet();
    }

    /**
     * Returns true if the server connection id has been set
     *
     * @return true if the server connection id has been set
     */
    public boolean hasServerValue() {
        return serverValue != null;
    }

    /**
     * Sets the server value for the connection id
     *
     * @param serverValue the server value for the connection id
     */
    public void setServerValue(final int serverValue) {
        this.serverValue = serverValue;
    }

    @Override
    public String toString() {
        return format("connectionId{localValue:%s, serverValue:%s}", localValue, (serverValue != null ? serverValue : "unknown"));
    }
}
