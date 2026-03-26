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

import com.mongodb.ConnectionString;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerMonitorListener;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerSettingsUnitTest {

    @Test
    void shouldHaveCorrectDefaults() {
        ServerSettings settings = ServerSettings.builder().build();

        assertEquals(10000, settings.getHeartbeatFrequency(MILLISECONDS));
        assertEquals(500, settings.getMinHeartbeatFrequency(MILLISECONDS));
        assertTrue(settings.getServerListeners().isEmpty());
        assertTrue(settings.getServerMonitorListeners().isEmpty());
    }

    @Test
    void shouldApplyBuilderSettings() {
        ServerListener serverListenerOne = new ServerListener() { };
        ServerListener serverListenerTwo = new ServerListener() { };
        ServerListener serverListenerThree = new ServerListener() { };
        ServerMonitorListener serverMonitorListenerOne = new ServerMonitorListener() { };
        ServerMonitorListener serverMonitorListenerTwo = new ServerMonitorListener() { };
        ServerMonitorListener serverMonitorListenerThree = new ServerMonitorListener() { };

        ServerSettings settings = ServerSettings.builder()
                .heartbeatFrequency(4, SECONDS)
                .minHeartbeatFrequency(1, SECONDS)
                .addServerListener(serverListenerOne)
                .addServerListener(serverListenerTwo)
                .addServerMonitorListener(serverMonitorListenerOne)
                .addServerMonitorListener(serverMonitorListenerTwo)
                .build();

        assertEquals(4000, settings.getHeartbeatFrequency(MILLISECONDS));
        assertEquals(1000, settings.getMinHeartbeatFrequency(MILLISECONDS));
        assertEquals(Arrays.asList(serverListenerOne, serverListenerTwo), settings.getServerListeners());
        assertEquals(Arrays.asList(serverMonitorListenerOne, serverMonitorListenerTwo), settings.getServerMonitorListeners());

        settings = ServerSettings.builder()
                .serverListenerList(Collections.singletonList(serverListenerThree))
                .serverMonitorListenerList(Collections.singletonList(serverMonitorListenerThree))
                .build();

        assertEquals(Collections.singletonList(serverListenerThree), settings.getServerListeners());
        assertEquals(Collections.singletonList(serverMonitorListenerThree), settings.getServerMonitorListeners());
    }

    @Test
    void whenConnectionStringIsAppliedToBuilderAllPropertiesShouldBeSet() {
        ServerSettings settings = ServerSettings.builder().applyConnectionString(new ConnectionString("mongodb://example.com:27018/?"
                + "heartbeatFrequencyMS=20000"))
                .build();

        assertEquals(20000, settings.getHeartbeatFrequency(MILLISECONDS));
    }

    @Test
    void shouldApplySettings() {
        ServerListener serverListenerOne = new ServerListener() { };
        ServerMonitorListener serverMonitorListenerOne = new ServerMonitorListener() { };
        ServerSettings defaultSettings = ServerSettings.builder().build();
        ServerSettings customSettings = ServerSettings.builder()
                .heartbeatFrequency(4, SECONDS)
                .minHeartbeatFrequency(1, SECONDS)
                .addServerListener(serverListenerOne)
                .addServerMonitorListener(serverMonitorListenerOne)
                .build();

        assertEquals(customSettings, ServerSettings.builder().applySettings(customSettings).build());
        assertEquals(defaultSettings, ServerSettings.builder(customSettings).applySettings(defaultSettings).build());
    }

    @Test
    void listsOfListenersShouldBeUnmodifiable() {
        ServerSettings settings = ServerSettings.builder().build();

        assertThrows(UnsupportedOperationException.class, () -> settings.getServerListeners().add(new ServerListener() { }));
        assertThrows(UnsupportedOperationException.class, () -> settings.getServerMonitorListeners().add(new ServerMonitorListener() { }));
    }

    @Test
    void listenersShouldNotBeNull() {
        assertThrows(IllegalArgumentException.class, () -> ServerSettings.builder().addServerListener(null));
        assertThrows(IllegalArgumentException.class, () -> ServerSettings.builder().addServerMonitorListener(null));
    }

    @Test
    void identicalSettingsShouldBeEqual() {
        ServerListener serverListenerOne = new ServerListener() { };
        ServerMonitorListener serverMonitorListenerOne = new ServerMonitorListener() { };

        assertEquals(ServerSettings.builder().build(), ServerSettings.builder().build());
        assertEquals(
                ServerSettings.builder()
                        .heartbeatFrequency(4, SECONDS)
                        .minHeartbeatFrequency(1, SECONDS)
                        .addServerListener(serverListenerOne)
                        .addServerMonitorListener(serverMonitorListenerOne)
                        .build(),
                ServerSettings.builder()
                        .heartbeatFrequency(4, SECONDS)
                        .minHeartbeatFrequency(1, SECONDS)
                        .addServerListener(serverListenerOne)
                        .addServerMonitorListener(serverMonitorListenerOne)
                        .build());
    }

    @Test
    void differentSettingsShouldNotBeEqual() {
        assertNotEquals(
                ServerSettings.builder().heartbeatFrequency(4, SECONDS).build(),
                ServerSettings.builder().heartbeatFrequency(3, SECONDS).build());
    }

    @Test
    void identicalSettingsShouldHaveSameHashCode() {
        ServerListener serverListenerOne = new ServerListener() { };
        ServerMonitorListener serverMonitorListenerOne = new ServerMonitorListener() { };

        assertEquals(ServerSettings.builder().build().hashCode(), ServerSettings.builder().build().hashCode());
        assertEquals(
                ServerSettings.builder()
                        .heartbeatFrequency(4, SECONDS)
                        .minHeartbeatFrequency(1, SECONDS)
                        .addServerListener(serverListenerOne)
                        .addServerMonitorListener(serverMonitorListenerOne)
                        .build().hashCode(),
                ServerSettings.builder()
                        .heartbeatFrequency(4, SECONDS)
                        .minHeartbeatFrequency(1, SECONDS)
                        .addServerListener(serverListenerOne)
                        .addServerMonitorListener(serverMonitorListenerOne)
                        .build().hashCode());
    }

    @Test
    void differentSettingsShouldHaveDifferentHashCodes() {
        assertNotEquals(
                ServerSettings.builder().heartbeatFrequency(4, SECONDS).build().hashCode(),
                ServerSettings.builder().heartbeatFrequency(3, SECONDS).build().hashCode());
    }
}
