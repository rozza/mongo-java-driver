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
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class SocketSettingsUnitTest {

    @Test
    void shouldHaveCorrectDefaults() {
        SocketSettings settings = SocketSettings.builder().build();

        assertEquals(10000, settings.getConnectTimeout(MILLISECONDS));
        assertEquals(0, settings.getReadTimeout(MILLISECONDS));
        assertEquals(0, settings.getReceiveBufferSize());
        assertEquals(0, settings.getSendBufferSize());
        assertEquals(ProxySettings.builder().build(), settings.getProxySettings());
    }

    @Test
    void shouldSetSettings() {
        SocketSettings settings = SocketSettings.builder()
                .connectTimeout(5000, MILLISECONDS)
                .readTimeout(2000, MILLISECONDS)
                .sendBufferSize(1000)
                .receiveBufferSize(1500)
                .applyToProxySettings(builder -> {
                    builder.host("proxy.com");
                    builder.port(1080);
                    builder.username("username");
                    builder.password("password");
                })
                .build();

        assertEquals(5000, settings.getConnectTimeout(MILLISECONDS));
        assertEquals(2000, settings.getReadTimeout(MILLISECONDS));
        assertEquals(1000, settings.getSendBufferSize());
        assertEquals(1500, settings.getReceiveBufferSize());
        ProxySettings proxySettings = settings.getProxySettings();
        assertEquals("proxy.com", proxySettings.getHost());
        assertEquals(1080, proxySettings.getPort());
        assertEquals("username", proxySettings.getUsername());
        assertEquals("password", proxySettings.getPassword());
    }

    @Test
    void shouldApplyBuilderSettings() {
        SocketSettings original = SocketSettings.builder()
                .connectTimeout(5000, MILLISECONDS)
                .readTimeout(2000, MILLISECONDS)
                .sendBufferSize(1000)
                .receiveBufferSize(1500)
                .applyToProxySettings(builder -> {
                    builder.host("proxy.com");
                    builder.port(1080);
                    builder.username("username");
                    builder.password("password");
                })
                .build();

        SocketSettings settings = SocketSettings.builder(original).build();

        assertEquals(5000, settings.getConnectTimeout(MILLISECONDS));
        assertEquals(2000, settings.getReadTimeout(MILLISECONDS));
        assertEquals(1000, settings.getSendBufferSize());
        assertEquals(1500, settings.getReceiveBufferSize());
        ProxySettings proxySettings = settings.getProxySettings();
        assertEquals("proxy.com", proxySettings.getHost());
        assertEquals(1080, proxySettings.getPort());
        assertEquals("username", proxySettings.getUsername());
        assertEquals("password", proxySettings.getPassword());
    }

    @Test
    void shouldApplyConnectionString() {
        SocketSettings settings = SocketSettings.builder()
                .applyConnectionString(new ConnectionString(
                        "mongodb://localhost/?connectTimeoutMS=5000&socketTimeoutMS=2000"
                                + "&proxyHost=proxy.com"
                                + "&proxyPort=1080"
                                + "&proxyUsername=username"
                                + "&proxyPassword=password"))
                .build();

        assertEquals(5000, settings.getConnectTimeout(MILLISECONDS));
        assertEquals(2000, settings.getReadTimeout(MILLISECONDS));
        assertEquals(0, settings.getSendBufferSize());
        assertEquals(0, settings.getReceiveBufferSize());
        ProxySettings proxySettings = settings.getProxySettings();
        assertEquals("proxy.com", proxySettings.getHost());
        assertEquals(1080, proxySettings.getPort());
        assertEquals("username", proxySettings.getUsername());
        assertEquals("password", proxySettings.getPassword());
    }

    @Test
    void shouldApplySettings() {
        SocketSettings defaultSettings = SocketSettings.builder().build();
        SocketSettings customSettings = SocketSettings.builder()
                .connectTimeout(5000, MILLISECONDS)
                .readTimeout(2000, MILLISECONDS)
                .sendBufferSize(1000)
                .receiveBufferSize(1500)
                .applyToProxySettings(builder -> {
                    builder.host("proxy.com");
                    builder.port(1080);
                    builder.username("username");
                    builder.password("password");
                })
                .build();

        assertEquals(customSettings, SocketSettings.builder().applySettings(customSettings).build());
        assertEquals(defaultSettings, SocketSettings.builder(customSettings).applySettings(defaultSettings).build());
    }

    @Test
    void identicalSettingsShouldBeEqual() {
        assertEquals(SocketSettings.builder().build(), SocketSettings.builder().build());
        assertEquals(
                buildFullSettings(),
                buildFullSettings());
    }

    @Test
    void differentSettingsShouldNotBeEqual() {
        assertNotEquals(SocketSettings.builder().receiveBufferSize(4).build(), SocketSettings.builder().receiveBufferSize(3).build());
    }

    @Test
    void identicalSettingsShouldHaveSameHashCode() {
        assertEquals(SocketSettings.builder().build().hashCode(), SocketSettings.builder().build().hashCode());
        assertEquals(buildFullSettings().hashCode(), buildFullSettings().hashCode());
    }

    @Test
    void differentSettingsShouldHaveDifferentHashCodes() {
        assertNotEquals(
                SocketSettings.builder().sendBufferSize(4).build().hashCode(),
                SocketSettings.builder().sendBufferSize(3).build().hashCode());
    }

    private SocketSettings buildFullSettings() {
        return SocketSettings.builder()
                .connectTimeout(5000, MILLISECONDS)
                .readTimeout(2000, MILLISECONDS)
                .sendBufferSize(1000)
                .receiveBufferSize(1500)
                .applyToProxySettings(builder -> {
                    builder.host("proxy.com");
                    builder.port(1080);
                    builder.username("username");
                    builder.password("password");
                })
                .build();
    }
}
