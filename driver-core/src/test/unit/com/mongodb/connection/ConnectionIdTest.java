/*
 * Copyright 2008-present MongoDB, Inc.
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

import com.mongodb.ServerAddress;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectionIdTest {

    private final ServerId serverId = new ServerId(new ClusterId(), new ServerAddress("host1"));

    @Test
    void shouldSetAllProperties() {
        ConnectionId id1 = new ConnectionId(serverId);
        ConnectionId id2 = new ConnectionId(serverId, Long.MAX_VALUE - 1, Long.MAX_VALUE);

        assertEquals(serverId, id1.getServerId());
        assertTrue(id1.getLocalValue() > 0);
        assertNull(id1.getServerValue());

        assertEquals(serverId, id2.getServerId());
        assertEquals(Long.MAX_VALUE - 1, id2.getLocalValue());
        assertEquals(Long.MAX_VALUE, id2.getServerValue());
    }

    @Test
    void shouldIncrementLocalValue() {
        ConnectionId id1 = new ConnectionId(serverId);
        ConnectionId id2 = new ConnectionId(serverId);
        assertEquals(id1.getLocalValue() + 1, id2.getLocalValue());
    }

    @Test
    void withServerValueShouldReturnNewInstanceWithGivenServerValue() {
        ConnectionId id = new ConnectionId(serverId);

        assertNotSame(id.withServerValue(124), id);
        assertEquals(123, id.withServerValue(123).getServerValue());
        assertEquals(id.getLocalValue(), id.withServerValue(123).getLocalValue());
        assertEquals(serverId, id.withServerValue(123).getServerId());
    }

    @Test
    void equivalentIdsShouldBeEqualAndHaveSameHashCode() {
        ConnectionId id1 = new ConnectionId(serverId, 100, 42L);
        ConnectionId id2 = new ConnectionId(serverId, 100, 42L);

        assertEquals(id1, id2);
        assertEquals(id1.hashCode(), id2.hashCode());
    }

    @Test
    void differentIdsShouldNotBeEqualAndHaveDifferentHashCode() {
        ConnectionId id1 = new ConnectionId(serverId, 100, 43L);
        ConnectionId id2 = new ConnectionId(serverId, 100, 42L);

        assertNotEquals(id1, id2);
        assertNotEquals(id1.hashCode(), id2.hashCode());
    }
}
