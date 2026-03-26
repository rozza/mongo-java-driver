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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SslSettingsTest {

    @Test
    void shouldHaveTheExpectedDefaults() {
        SslSettings settings = SslSettings.builder().build();

        assertNull(settings.getContext());
        assertFalse(settings.isEnabled());
        assertFalse(settings.isInvalidHostNameAllowed());
    }

    @Test
    void shouldSetSettings() throws NoSuchAlgorithmException {
        SslSettings settings = SslSettings.builder()
                .context(SSLContext.getDefault())
                .enabled(true)
                .invalidHostNameAllowed(true)
                .build();

        assertEquals(SSLContext.getDefault(), settings.getContext());
        assertTrue(settings.isEnabled());
        assertTrue(settings.isInvalidHostNameAllowed());
    }

    static Stream<Object[]> connectionStringProvider() throws NoSuchAlgorithmException {
        return Stream.of(
                new Object[]{"mongodb://localhost", SslSettings.builder(), SslSettings.builder().build()},
                new Object[]{"mongodb://localhost/?ssl=false", SslSettings.builder(), SslSettings.builder().build()},
                new Object[]{"mongodb://localhost/?ssl=true", SslSettings.builder(), SslSettings.builder().enabled(true).build()},
                new Object[]{"mongodb://localhost/?ssl=true&sslInvalidHostNameAllowed=true", SslSettings.builder(),
                        SslSettings.builder().enabled(true).invalidHostNameAllowed(true).build()},
                new Object[]{"mongodb://localhost/?ssl=true&sslInvalidHostNameAllowed=true",
                        SslSettings.builder().context(SSLContext.getDefault()),
                        SslSettings.builder().enabled(true).context(SSLContext.getDefault()).invalidHostNameAllowed(true).build()}
        );
    }

    @ParameterizedTest
    @MethodSource("connectionStringProvider")
    void shouldApplyConnectionString(String connectionString, SslSettings.Builder builder, SslSettings expected) {
        assertEquals(expected, builder.applyConnectionString(new ConnectionString(connectionString)).build());
    }

    @Test
    void shouldApplySettings() throws NoSuchAlgorithmException {
        SslSettings defaultSettings = SslSettings.builder().build();
        SslSettings customSettings = SslSettings.builder()
                .context(SSLContext.getDefault())
                .enabled(true)
                .invalidHostNameAllowed(true)
                .build();

        assertEquals(customSettings, SslSettings.builder().applySettings(customSettings).build());
        assertEquals(defaultSettings, SslSettings.builder(customSettings).applySettings(defaultSettings).build());
    }

    @Test
    void shouldApplyBuilderSettings() throws NoSuchAlgorithmException {
        SslSettings original = SslSettings.builder().enabled(true)
                .context(SSLContext.getDefault())
                .invalidHostNameAllowed(true).build();

        SslSettings settings = SslSettings.builder(original).build();

        assertEquals(original, settings);
    }

    @Test
    void equivalentSettingsShouldBeEqualAndHaveSameHashCode() throws NoSuchAlgorithmException {
        assertEquals(SslSettings.builder().build(), SslSettings.builder().build());
        assertEquals(SslSettings.builder().build().hashCode(), SslSettings.builder().build().hashCode());

        assertEquals(
                SslSettings.builder().enabled(true).invalidHostNameAllowed(true).build(),
                SslSettings.builder().enabled(true).invalidHostNameAllowed(true).build());
        assertEquals(
                SslSettings.builder().enabled(true).invalidHostNameAllowed(true).build().hashCode(),
                SslSettings.builder().enabled(true).invalidHostNameAllowed(true).build().hashCode());

        assertEquals(
                SslSettings.builder().enabled(true).invalidHostNameAllowed(true).context(SSLContext.getDefault()).build(),
                SslSettings.builder().enabled(true).invalidHostNameAllowed(true).context(SSLContext.getDefault()).build());
        assertEquals(
                SslSettings.builder().enabled(true).invalidHostNameAllowed(true).context(SSLContext.getDefault()).build().hashCode(),
                SslSettings.builder().enabled(true).invalidHostNameAllowed(true).context(SSLContext.getDefault()).build().hashCode());
    }

    @Test
    void unequivalentSettingsShouldNotBeEqual() throws NoSuchAlgorithmException {
        assertNotEquals(SslSettings.builder().build(), SslSettings.builder().enabled(true).build());
        assertNotEquals(SslSettings.builder().build(), SslSettings.builder().invalidHostNameAllowed(true).build());
        assertNotEquals(SslSettings.builder().build(), SslSettings.builder().context(SSLContext.getDefault()).build());
    }
}
