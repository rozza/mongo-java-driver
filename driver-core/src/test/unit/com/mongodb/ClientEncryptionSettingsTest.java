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

package com.mongodb;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClientEncryptionSettingsTest {

    @Test
    @DisplayName("should have return the configured values defaults")
    void shouldReturnConfiguredValuesDefaults() throws NoSuchAlgorithmException {
        MongoClientSettings mongoClientSettings = MongoClientSettings.builder().build();
        String keyVaultNamespace = "keyVaultNamespace";
        Map<String, Map<String, Object>> kmsProvider = new HashMap<>();
        Map<String, Object> providerMap = new HashMap<>();
        providerMap.put("test", "test");
        kmsProvider.put("provider", providerMap);

        Map<String, Supplier<Map<String, Object>>> kmsProviderSupplier = new HashMap<>();
        kmsProviderSupplier.put("provider", () -> {
            Map<String, Object> m = new HashMap<>();
            m.put("test", "test");
            return m;
        });

        Map<String, SSLContext> kmsProviderSslContextMap = new HashMap<>();
        kmsProviderSslContextMap.put("provider", SSLContext.getDefault());

        ClientEncryptionSettings options = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(mongoClientSettings)
                .keyVaultNamespace(keyVaultNamespace)
                .kmsProviders(kmsProvider)
                .build();

        assertEquals(mongoClientSettings, options.getKeyVaultMongoClientSettings());
        assertEquals(keyVaultNamespace, options.getKeyVaultNamespace());
        assertEquals(kmsProvider, options.getKmsProviders());
        assertEquals(Collections.emptyMap(), options.getKmsProviderPropertySuppliers());
        assertEquals(Collections.emptyMap(), options.getKmsProviderSslContextMap());
        assertNull(options.getTimeout(TimeUnit.MILLISECONDS));

        options = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(mongoClientSettings)
                .keyVaultNamespace(keyVaultNamespace)
                .kmsProviders(kmsProvider)
                .kmsProviderPropertySuppliers(kmsProviderSupplier)
                .kmsProviderSslContextMap(kmsProviderSslContextMap)
                .timeout(1_000, TimeUnit.MILLISECONDS)
                .build();

        assertEquals(mongoClientSettings, options.getKeyVaultMongoClientSettings());
        assertEquals(keyVaultNamespace, options.getKeyVaultNamespace());
        assertEquals(kmsProvider, options.getKmsProviders());
        assertEquals(kmsProviderSupplier, options.getKmsProviderPropertySuppliers());
        assertEquals(kmsProviderSslContextMap, options.getKmsProviderSslContextMap());
        assertEquals(Long.valueOf(1_000), options.getTimeout(TimeUnit.MILLISECONDS));
    }

    @Test
    @DisplayName("should throw an exception if the defaultTimeout is set and negative")
    void shouldThrowExceptionForInvalidTimeout() {
        ClientEncryptionSettings.Builder builder = ClientEncryptionSettings.builder();

        assertThrows(IllegalArgumentException.class, () -> builder.timeout(500, TimeUnit.NANOSECONDS));
        assertThrows(IllegalArgumentException.class, () -> builder.timeout(-1, TimeUnit.SECONDS));
    }
}
