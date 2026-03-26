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

package com.mongodb.client;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClient;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.client.model.Filters.eq;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ClientSideEncryptionExternalKeyVaultSpecTest extends FunctionalSpecification {

    private final MongoNamespace keyVaultNamespace = new MongoNamespace("test.datakeys");
    private final MongoNamespace autoEncryptingCollectionNamespace = new MongoNamespace(getDefaultDatabaseName(),
            "ClientSideEncryptionProseTestSpecification");
    private final MongoCollection<BsonDocument> dataKeyCollection = getMongoClient()
            .getDatabase(keyVaultNamespace.getDatabaseName()).getCollection(keyVaultNamespace.getCollectionName(), BsonDocument.class)
            .withWriteConcern(WriteConcern.MAJORITY);
    private final MongoCollection<BsonDocument> dataCollection = getMongoClient()
            .getDatabase(autoEncryptingCollectionNamespace.getDatabaseName())
            .getCollection(autoEncryptingCollectionNamespace.getCollectionName(), BsonDocument.class);

    private MongoClient autoEncryptingClient;
    private ClientEncryption clientEncryption;
    private MongoCollection<BsonDocument> autoEncryptingDataCollection;
    private TestCommandListener commandListener;

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        assumeTrue(System.getProperty("AWS_ACCESS_KEY_ID") != null
                        && !System.getProperty("AWS_ACCESS_KEY_ID").isEmpty(),
                "Key vault tests disabled");

        dataKeyCollection.drop();
        dataCollection.drop();

        Map<String, Object> localKey = new HashMap<>();
        localKey.put("key", Base64.getDecoder().decode("Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN"
                + "3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk"));

        Map<String, Object> awsKey = new HashMap<>();
        awsKey.put("accessKeyId", System.getProperty("AWS_ACCESS_KEY_ID"));
        awsKey.put("secretAccessKey", System.getProperty("AWS_SECRET_ACCESS_KEY"));

        Map<String, Map<String, Object>> providerProperties = new HashMap<>();
        providerProperties.put("local", localKey);
        providerProperties.put("aws", awsKey);

        autoEncryptingClient = MongoClients.create(getMongoClientSettingsBuilder()
                .autoEncryptionSettings(AutoEncryptionSettings.builder()
                        .keyVaultNamespace(keyVaultNamespace.getFullName())
                        .kmsProviders(providerProperties)
                        .schemaMap(singletonMap(autoEncryptingCollectionNamespace.getFullName(),
                                BsonDocument.parse(
                                        "{"
                                                + "  \"bsonType\": \"object\","
                                                + "  \"properties\": {"
                                                + "    \"encrypted_placeholder\": {"
                                                + "      \"encrypt\": {"
                                                + "        \"keyId\": \"/placeholder\","
                                                + "        \"bsonType\": \"string\","
                                                + "        \"algorithm\": \"AEAD_AES_256_CBC_HMAC_SHA_512-Random\""
                                                + "      }"
                                                + "    }"
                                                + "  }"
                                                + "}")))
                        .build())
                .build());

        autoEncryptingDataCollection = autoEncryptingClient.getDatabase(autoEncryptingCollectionNamespace.getDatabaseName())
                .getCollection(autoEncryptingCollectionNamespace.getCollectionName(), BsonDocument.class);

        commandListener = new TestCommandListener();
        clientEncryption = ClientEncryptions.create(ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettingsBuilder()
                        .addCommandListener(commandListener)
                        .build())
                .keyVaultNamespace(keyVaultNamespace.getFullName())
                .kmsProviders(providerProperties)
                .build());
    }

    @Test
    @DisplayName("test external key vault")
    void testExternalKeyVault() {
        // Create local data key
        BsonBinary localDataKeyId = clientEncryption.createDataKey("local",
                new DataKeyOptions().keyAltNames(Collections.singletonList("local_altname")));

        assertEquals(1, commandListener.getCommandStartedEvents().size());
        CommandStartedEvent event = commandListener.getCommandStartedEvents().get(0);
        assertEquals(true, event.getCommand().containsKey("writeConcern"));
        assertEquals(WriteConcern.MAJORITY.asDocument(), event.getCommand().getDocument("writeConcern"));

        assertNotNull(localDataKeyId);
        assertEquals(BsonBinarySubType.UUID_STANDARD.getValue(), localDataKeyId.getType());
        List<BsonDocument> localKeys = dataKeyCollection.find(eq("masterKey.provider", "local")).into(new ArrayList<>());
        assertEquals(1, localKeys.size());

        // Encrypt with local key
        BsonBinary localEncrypted = clientEncryption.encrypt(new BsonString("hello local"),
                new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic")
                        .keyId(localDataKeyId));

        assertEquals((byte) 6, localEncrypted.asBinary().getType());

        // Insert and find with local encryption
        autoEncryptingDataCollection.insertOne(new BsonDocument("_id", new BsonString("local"))
                .append("value", localEncrypted));

        assertEquals("hello local",
                autoEncryptingDataCollection.find(eq("_id", new BsonString("local"))).first().getString("value").getValue());

        // Encrypt with local alt name
        BsonBinary localEncryptedWithAltName = clientEncryption.encrypt(new BsonString("hello local"),
                new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic")
                        .keyAltName("local_altname"));

        assertEquals(localEncrypted, localEncryptedWithAltName);

        // Create AWS data key
        BsonBinary awsDataKeyId = clientEncryption.createDataKey("aws",
                new DataKeyOptions().keyAltNames(Collections.singletonList("aws_altname"))
                        .masterKey(new BsonDocument("region", new BsonString("us-east-1"))
                                .append("key", new BsonString("arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"))));

        assertNotNull(awsDataKeyId);
        assertEquals(BsonBinarySubType.UUID_STANDARD.getValue(), awsDataKeyId.getType());
        List<BsonDocument> awsKeys = dataKeyCollection.find(eq("masterKey.provider", "aws")).into(new ArrayList<>());
        assertEquals(1, awsKeys.size());

        // Encrypt with AWS key
        BsonBinary awsEncrypted = clientEncryption.encrypt(new BsonString("hello aws"),
                new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic")
                        .keyId(awsDataKeyId));

        assertEquals((byte) 6, awsEncrypted.asBinary().getType());

        // Insert and find with AWS encryption
        autoEncryptingDataCollection.insertOne(new BsonDocument("_id", new BsonString("aws"))
                .append("value", awsEncrypted));

        assertEquals("hello aws",
                autoEncryptingDataCollection.find(eq("_id", new BsonString("aws"))).first().getString("value").getValue());

        // Encrypt with AWS alt name
        BsonBinary awsEncryptedWithAltName = clientEncryption.encrypt(new BsonString("hello aws"),
                new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic")
                        .keyAltName("aws_altname"));

        assertEquals(awsEncrypted, awsEncryptedWithAltName);

        // Insert encrypted_placeholder should throw
        assertThrows(MongoClientException.class, () ->
                autoEncryptingDataCollection.insertOne(new BsonDocument("encrypted_placeholder", localEncrypted)));
    }
}
