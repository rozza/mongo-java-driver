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
import com.mongodb.event.ConnectionPoolListener;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class ConnectionPoolSettingsTest {

    static Stream<Object[]> settingsProvider() {
        return Stream.of(
                new Object[]{
                        ConnectionPoolSettings.builder().build(),
                        120000L, 100, 0L, 0L, 0, 0L, 60000L, 2
                },
                new Object[]{
                        ConnectionPoolSettings.builder()
                                .maxWaitTime(5, SECONDS)
                                .maxSize(75)
                                .maxConnectionLifeTime(101, SECONDS)
                                .maxConnectionIdleTime(51, SECONDS)
                                .minSize(1)
                                .maintenanceInitialDelay(5, SECONDS)
                                .maintenanceFrequency(1000, SECONDS)
                                .maxConnecting(1)
                                .build(),
                        5000L, 75, 101000L, 51000L, 1, 5000L, 1000000L, 1
                },
                new Object[]{
                        ConnectionPoolSettings.builder(ConnectionPoolSettings.builder()
                                        .maxWaitTime(5, SECONDS)
                                        .maxSize(75)
                                        .maxConnectionLifeTime(101, SECONDS)
                                        .maxConnectionIdleTime(51, SECONDS)
                                        .minSize(1)
                                        .maintenanceInitialDelay(5, SECONDS)
                                        .maintenanceFrequency(1000, SECONDS)
                                        .maxConnecting(2)
                                        .build())
                                .build(),
                        5000L, 75, 101000L, 51000L, 1, 5000L, 1000000L, 2
                },
                new Object[]{
                        ConnectionPoolSettings.builder(ConnectionPoolSettings.builder().build())
                                .maxWaitTime(5, SECONDS)
                                .maxSize(75)
                                .maxConnectionLifeTime(101, SECONDS)
                                .maxConnectionIdleTime(51, SECONDS)
                                .minSize(1)
                                .maintenanceInitialDelay(5, SECONDS)
                                .maintenanceFrequency(1000, SECONDS)
                                .maxConnecting(1000)
                                .build(),
                        5000L, 75, 101000L, 51000L, 1, 5000L, 1000000L, 1000
                }
        );
    }

    @ParameterizedTest
    @MethodSource("settingsProvider")
    void shouldSetUpConnectionProviderSettingsCorrectly(ConnectionPoolSettings settings, long maxWaitTime, int maxSize,
                                                        long maxConnectionLifeTimeMS, long maxConnectionIdleTimeMS,
                                                        int minSize, long maintenanceInitialDelayMS,
                                                        long maintenanceFrequencyMS, int maxConnecting) {
        assertEquals(maxWaitTime, settings.getMaxWaitTime(MILLISECONDS));
        assertEquals(maxSize, settings.getMaxSize());
        assertEquals(maxConnectionLifeTimeMS, settings.getMaxConnectionLifeTime(MILLISECONDS));
        assertEquals(maxConnectionIdleTimeMS, settings.getMaxConnectionIdleTime(MILLISECONDS));
        assertEquals(minSize, settings.getMinSize());
        assertEquals(maintenanceInitialDelayMS, settings.getMaintenanceInitialDelay(MILLISECONDS));
        assertEquals(maintenanceFrequencyMS, settings.getMaintenanceFrequency(MILLISECONDS));
        assertEquals(maxConnecting, settings.getMaxConnecting());
    }

    @Test
    void shouldThrowExceptionOnInvalidArgument() {
        assertThrows(IllegalStateException.class, () ->
                ConnectionPoolSettings.builder().maxSize(1).maxConnectionLifeTime(-1, SECONDS).build());
        assertThrows(IllegalStateException.class, () ->
                ConnectionPoolSettings.builder().maxSize(1).maxConnectionIdleTime(-1, SECONDS).build());
        assertThrows(IllegalStateException.class, () ->
                ConnectionPoolSettings.builder().maxSize(1).minSize(2).build());
        assertThrows(IllegalStateException.class, () ->
                ConnectionPoolSettings.builder().maxSize(-1).build());
        assertThrows(IllegalStateException.class, () ->
                ConnectionPoolSettings.builder().maintenanceInitialDelay(-1, MILLISECONDS).build());
        assertThrows(IllegalStateException.class, () ->
                ConnectionPoolSettings.builder().maintenanceFrequency(0, MILLISECONDS).build());
        assertThrows(IllegalStateException.class, () ->
                ConnectionPoolSettings.builder().maxConnecting(0).build());
        assertThrows(IllegalStateException.class, () ->
                ConnectionPoolSettings.builder().maxConnecting(-1).build());
    }

    @Test
    void settingsWithSameValuesShouldBeEqual() {
        ConnectionPoolSettings settings1 = ConnectionPoolSettings.builder().maxSize(1).build();
        ConnectionPoolSettings settings2 = ConnectionPoolSettings.builder().maxSize(1).build();

        assertEquals(settings1, settings2);
    }

    @Test
    void settingsWithSameValuesShouldHaveSameHashCode() {
        ConnectionPoolSettings settings1 = ConnectionPoolSettings.builder().maxSize(1).build();
        ConnectionPoolSettings settings2 = ConnectionPoolSettings.builder().maxSize(1).build();

        assertEquals(settings1.hashCode(), settings2.hashCode());
    }

    @Test
    void shouldApplyConnectionString() {
        ConnectionPoolSettings settings = ConnectionPoolSettings.builder().applyConnectionString(
                new ConnectionString("mongodb://localhost/?waitQueueTimeoutMS=100&minPoolSize=5&maxPoolSize=10&"
                        + "maxIdleTimeMS=200&maxLifeTimeMS=300&maxConnecting=1"))
                .build();

        assertEquals(100, settings.getMaxWaitTime(MILLISECONDS));
        assertEquals(10, settings.getMaxSize());
        assertEquals(5, settings.getMinSize());
        assertEquals(200, settings.getMaxConnectionIdleTime(MILLISECONDS));
        assertEquals(300, settings.getMaxConnectionLifeTime(MILLISECONDS));
        assertEquals(1, settings.getMaxConnecting());
    }

    @Test
    void shouldApplySettings() {
        ConnectionPoolListener connectionPoolListener = mock(ConnectionPoolListener.class);
        ConnectionPoolSettings defaultSettings = ConnectionPoolSettings.builder().build();
        ConnectionPoolSettings customSettings = ConnectionPoolSettings.builder()
                .addConnectionPoolListener(mock(ConnectionPoolListener.class))
                .maxWaitTime(5, SECONDS)
                .maxSize(75)
                .maxConnectionLifeTime(101, SECONDS)
                .maxConnectionIdleTime(51, SECONDS)
                .minSize(1)
                .maintenanceInitialDelay(5, SECONDS)
                .maintenanceFrequency(1000, SECONDS)
                .maxConnecting(1)
                .build();

        assertEquals(customSettings, ConnectionPoolSettings.builder().applySettings(customSettings).build());
        assertEquals(defaultSettings, ConnectionPoolSettings.builder(customSettings).applySettings(defaultSettings).build());

        customSettings = ConnectionPoolSettings.builder(customSettings)
                .connectionPoolListenerList(Collections.singletonList(connectionPoolListener)).build();

        assertEquals(Collections.singletonList(connectionPoolListener), customSettings.getConnectionPoolListeners());
    }

    @Test
    void toStringShouldBeOverridden() {
        ConnectionPoolSettings settings = ConnectionPoolSettings.builder().maxSize(1).build();

        assertTrue(settings.toString().startsWith("ConnectionPoolSettings"));
    }

    @Test
    void identicalSettingsShouldBeEqual() {
        assertEquals(ConnectionPoolSettings.builder().build(), ConnectionPoolSettings.builder().build());
        assertEquals(
                ConnectionPoolSettings.builder().maxWaitTime(5, SECONDS).maxSize(75).maxConnectionLifeTime(101, SECONDS)
                        .maxConnectionIdleTime(51, SECONDS).minSize(1).maintenanceInitialDelay(5, SECONDS)
                        .maintenanceFrequency(1000, SECONDS).maxConnecting(1).build(),
                ConnectionPoolSettings.builder().maxWaitTime(5, SECONDS).maxSize(75).maxConnectionLifeTime(101, SECONDS)
                        .maxConnectionIdleTime(51, SECONDS).minSize(1).maintenanceInitialDelay(5, SECONDS)
                        .maintenanceFrequency(1000, SECONDS).maxConnecting(1).build());
    }

    @Test
    void differentSettingsShouldNotBeEqual() {
        assertNotEquals(
                ConnectionPoolSettings.builder().maxWaitTime(5, SECONDS).build(),
                ConnectionPoolSettings.builder().maxWaitTime(2, SECONDS).build());
    }

    @Test
    void identicalSettingsShouldHaveSameHashCode() {
        assertEquals(ConnectionPoolSettings.builder().build().hashCode(), ConnectionPoolSettings.builder().build().hashCode());
        assertEquals(
                ConnectionPoolSettings.builder().maxWaitTime(5, SECONDS).maxSize(75).maxConnectionLifeTime(101, SECONDS)
                        .maxConnectionIdleTime(51, SECONDS).minSize(1).maintenanceInitialDelay(5, SECONDS)
                        .maintenanceFrequency(1000, SECONDS).maxConnecting(1).build().hashCode(),
                ConnectionPoolSettings.builder().maxWaitTime(5, SECONDS).maxSize(75).maxConnectionLifeTime(101, SECONDS)
                        .maxConnectionIdleTime(51, SECONDS).minSize(1).maintenanceInitialDelay(5, SECONDS)
                        .maintenanceFrequency(1000, SECONDS).maxConnecting(1).build().hashCode());
    }

    @Test
    void differentSettingsShouldHaveDifferentHashCodes() {
        assertNotEquals(
                ConnectionPoolSettings.builder().maxWaitTime(5, SECONDS).build().hashCode(),
                ConnectionPoolSettings.builder().maxWaitTime(3, SECONDS).build().hashCode());
    }

    @Test
    void shouldAllowZeroInfiniteMaxSize() {
        assertEquals(0, ConnectionPoolSettings.builder().maxSize(0).build().getMaxSize());
    }
}
