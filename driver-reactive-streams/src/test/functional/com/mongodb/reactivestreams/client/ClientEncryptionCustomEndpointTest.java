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

package com.mongodb.reactivestreams.client;

import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.crypt.capi.MongoCryptException;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.vault.ClientEncryption;
import com.mongodb.reactivestreams.client.vault.ClientEncryptions;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import reactor.core.publisher.Mono;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.ClusterFixture.hasEncryptionTestsEnabled;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class ClientEncryptionCustomEndpointTest {

    private ClientEncryption clientEncryption;
    private ClientEncryption invalidClientEncryption;
    private final String provider;
    private final BsonDocument masterKey;
    private final boolean testInvalidClientEncryption;
    private final Class<? extends RuntimeException> exceptionClass;
    private final Class<? extends RuntimeException> wrappedExceptionClass;
    private final String messageContainedInException;

    public ClientEncryptionCustomEndpointTest(@SuppressWarnings("unused") final String name,
                                              final String provider,
                                              final BsonDocument masterKey,
                                              final boolean testInvalidClientEncryption,
                                              @Nullable final Class<? extends RuntimeException> exceptionClass,
                                              @Nullable final Class<? extends RuntimeException> wrappedExceptionClass,
                                              @Nullable final String messageContainedInException) {
        this.provider = provider;
        this.masterKey = masterKey;
        this.testInvalidClientEncryption = testInvalidClientEncryption;
        this.exceptionClass = exceptionClass;
        this.wrappedExceptionClass = wrappedExceptionClass;
        this.messageContainedInException = messageContainedInException;
    }

    @Before
    public void setUp() {
        assumeTrue(serverVersionAtLeast(4, 1));
        assumeTrue("Custom Endpoint tests disables", hasEncryptionTestsEnabled());

        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>() {{
            put("aws",  new HashMap<String, Object>() {{
                put("accessKeyId", System.getProperty("org.mongodb.test.awsAccessKeyId"));
                put("secretAccessKey", System.getProperty("org.mongodb.test.awsSecretAccessKey"));
            }});
            put("azure",  new HashMap<String, Object>() {{
                put("tenantId", System.getProperty("org.mongodb.test.azureTenantId"));
                put("clientId", System.getProperty("org.mongodb.test.azureClientId"));
                put("clientSecret", System.getProperty("org.mongodb.test.azureClientSecret"));
                put("identityPlatformEndpoint", "login.microsoftonline.com:443");
            }});
            put("gcp",  new HashMap<String, Object>() {{
                put("email", System.getProperty("org.mongodb.test.gcpEmail"));
                put("privateKey", System.getProperty("org.mongodb.test.gcpPrivateKey"));
                put("endpoint", "oauth2.googleapis.com:443");
            }});
        }};

        ClientEncryptionSettings.Builder clientEncryptionSettingsBuilder = ClientEncryptionSettings.builder().
                keyVaultMongoClientSettings(getMongoClientSettings())
                .kmsProviders(kmsProviders)
                .keyVaultNamespace("keyvault.datakeys");

        ClientEncryptionSettings clientEncryptionSettings = clientEncryptionSettingsBuilder.build();
        clientEncryption = ClientEncryptions.create(clientEncryptionSettings);

        Map<String, Map<String, Object>> invalidKmsProviders = new HashMap<String, Map<String, Object>>() {{
            put("azure",  new HashMap<String, Object>() {{
                put("tenantId", System.getProperty("org.mongodb.test.azureTenantId"));
                put("clientId", System.getProperty("org.mongodb.test.azureClientId"));
                put("clientSecret", System.getProperty("org.mongodb.test.azureClientSecret"));
                put("identityPlatformEndpoint", "example.com:443");
            }});
            put("gcp",  new HashMap<String, Object>() {{
                put("email", System.getProperty("org.mongodb.test.gcpEmail"));
                put("privateKey", System.getProperty("org.mongodb.test.gcpPrivateKey"));
                put("endpoint", "example.com:443");
            }});
        }};

        invalidClientEncryption = ClientEncryptions.create(ClientEncryptionSettings.builder().
                keyVaultMongoClientSettings(getMongoClientSettings())
                .kmsProviders(invalidKmsProviders)
                .keyVaultNamespace("keyvault.datakeys")
                .build());
    }

    @After
    public void after() {
        if (clientEncryption != null) {
            try {
                clientEncryption.close();
            } catch (Exception e) {
                // ignore
            }
        }

        if (invalidClientEncryption != null) {
            try {
                invalidClientEncryption.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Test
    public void testCustomEndpoint() throws Throwable {
        if (testInvalidClientEncryption) {
            testEndpoint(clientEncryption, null, null, null);
            testEndpoint(invalidClientEncryption, exceptionClass, wrappedExceptionClass, messageContainedInException);
        } else {
            testEndpoint(clientEncryption, exceptionClass, wrappedExceptionClass, messageContainedInException);
        }
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> data = new ArrayList<Object[]>();

        data.add(new Object[]{"1. [aws] valid endpoint",
                "aws",
                BsonDocument.parse("{"
                        + "  region: \"us-east-1\","
                        + "  key: \"arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0\""
                        + "}"),
                false, null, null, null});
        data.add(new Object[]{"2. [aws] valid explicit endpoint",
                "aws",
                BsonDocument.parse("{"
                        + "  region: \"us-east-1\","
                        + "  key: \"arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0\","
                        + "  endpoint: \"kms.us-east-1.amazonaws.com\""
                        + "}"),
                false, null, null, null});
        data.add(new Object[]{"3. [aws] valid explicit endpoint and port",
                "aws",
                BsonDocument.parse("{"
                        + "  region: \"us-east-1\","
                        + "  key: \"arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0\","
                        + "  endpoint: \"kms.us-east-1.amazonaws.com:443\""
                        + "}"),
                false, null, null, null});
        data.add(new Object[]{"4. [aws] invalid amazon region in endpoint",
                "aws",
                BsonDocument.parse("{\n"
                        + "  region: \"us-east-1\",\n"
                        + "  key: \"arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0\",\n"
                        + "  endpoint: \"kms.us-east-1.amazonaws.com:12345\"\n"
                        + "}"),
                false, MongoClientException.class, ConnectException.class, "Connection refused"});
        data.add(new Object[]{"5. [aws] invalid endpoint host",
                "aws",
                BsonDocument.parse("{\n"
                        + "  region: \"us-east-1\",\n"
                        + "  key: \"arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0\",\n"
                        + "  endpoint: \"kms.us-east-2.amazonaws.com\"\n"
                        + "}"),
                false, MongoClientException.class, MongoCryptException.class, "us-east-1"});
        data.add(new Object[]{"6. [aws] invalid endpoint host",
                "aws",
                BsonDocument.parse("{\n"
                        + "  region: \"us-east-1\",\n"
                        + "  key: \"arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0\",\n"
                        + "  endpoint: \"example.com\"\n"
                        + "}"),
                false, MongoClientException.class, MongoCryptException.class, "parse error"});

        data.add(new Object[]{"7. [azure] valid and invalid kms providers test",
                "azure",
                BsonDocument.parse("{\n"
                        + "  \"keyVaultEndpoint\": \"key-vault-csfle.vault.azure.net\",\n"
                        + "  \"keyName\": \"key-name-csfle\"\n"
                        + "}"),
                true, MongoClientException.class, MongoCryptException.class, "parse error"});

        data.add(new Object[]{"8. [gcp] valid and invalid kms providers test",
                "gcp",
                BsonDocument.parse("{\n"
                        + "  \"projectId\": \"devprod-drivers\",\n"
                        + "  \"location\": \"global\",\n"
                        + "  \"keyRing\": \"key-ring-csfle\",\n"
                        + "  \"keyName\": \"key-name-csfle\",\n"
                        + "  \"endpoint\": \"cloudkms.googleapis.com:443\"\n"
                        + "}"),
                true, MongoClientException.class, MongoCryptException.class, "parse error"});

        data.add(new Object[]{"9. [gcp] invalid endpoint",
                "gcp",
                BsonDocument.parse("{\n"
                        + "  \"projectId\": \"csfle-poc\",\n"
                        + "  \"location\": \"global\",\n"
                        + "  \"keyRing\": \"test\",\n"
                        + "  \"keyName\": \"quickstart\",\n"
                        + "  \"endpoint\": \"example.com:443\"\n"
                        + "}"),
                false, MongoClientException.class, MongoCryptException.class, "Invalid KMS response"});
        return data;
    }

    private void testEndpoint(final ClientEncryption clientEncryption,
                              @Nullable final Class<? extends RuntimeException> exceptionClass,
                              @Nullable final Class<? extends RuntimeException> wrappedExceptionClass,
                              @Nullable final String messageContainedInException) {
        try {
            BsonBinary dataKeyId = Mono.from(clientEncryption.createDataKey(provider, new DataKeyOptions().masterKey(masterKey)))
                    .block(TIMEOUT_DURATION);
            assertNull("Expected exception, but encryption succeeded", exceptionClass);

            assertNotNull(dataKeyId);
            Mono.from(clientEncryption.encrypt(new BsonString("test"),
                                               new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic")
                    .keyId(dataKeyId))).block(TIMEOUT_DURATION);
        } catch (Exception e) {
            if (exceptionClass == null) {
                throw e;
            }
            assertEquals(exceptionClass, e.getClass());
            assertEquals(wrappedExceptionClass, e.getCause().getClass());
            if (messageContainedInException != null) {
                assertTrue("Actual Error: " + e.getCause().getMessage(), e.getCause().getMessage().contains(messageContainedInException));
            }
        }
    }
}
