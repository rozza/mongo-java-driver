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
import com.mongodb.MongoNamespace;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.ClusterFixture.isClientSideEncryptionTest;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClient;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static util.JsonPoweredTestHelper.getTestDocument;

class ClientSideEncryptionBsonSizeLimitsTest extends FunctionalSpecification {

    private static final String COLL_NAME = "ClientSideEncryptionBsonSizeLimitsSpecification";
    private final MongoNamespace keyVaultNamespace = new MongoNamespace("test.datakeys");
    private final MongoNamespace autoEncryptingCollectionNamespace = new MongoNamespace(getDefaultDatabaseName(),
            COLL_NAME);
    private final MongoCollection<BsonDocument> dataKeyCollection = getMongoClient()
            .getDatabase(keyVaultNamespace.getDatabaseName()).getCollection(keyVaultNamespace.getCollectionName(), BsonDocument.class)
            .withWriteConcern(WriteConcern.MAJORITY);
    private final MongoCollection<BsonDocument> dataCollection = getMongoClient()
            .getDatabase(autoEncryptingCollectionNamespace.getDatabaseName())
            .getCollection(autoEncryptingCollectionNamespace.getCollectionName(), BsonDocument.class);
    private final TestCommandListener commandListener = new TestCommandListener();

    private MongoClient autoEncryptingClient;
    private ClientEncryption clientEncryption;
    private MongoCollection<BsonDocument> autoEncryptingDataCollection;

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        assumeTrue(isClientSideEncryptionTest());
        dataKeyCollection.drop();
        dataCollection.drop();

        dataKeyCollection.insertOne(getTestDocument("client-side-encryption/limits/limits-key.json"));

        Map<String, Object> localKey = new HashMap<>();
        localKey.put("key", Base64.getDecoder().decode("Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN"
                + "3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk"));

        Map<String, Map<String, Object>> providerProperties = new HashMap<>();
        providerProperties.put("local", localKey);

        autoEncryptingClient = MongoClients.create(getMongoClientSettingsBuilder()
                .autoEncryptionSettings(AutoEncryptionSettings.builder()
                        .keyVaultNamespace(keyVaultNamespace.getFullName())
                        .kmsProviders(providerProperties)
                        .schemaMap(singletonMap(autoEncryptingCollectionNamespace.getFullName(),
                                getTestDocument("client-side-encryption/limits/limits-schema.json")))
                        .build())
                .addCommandListener(commandListener)
                .build());

        autoEncryptingDataCollection = autoEncryptingClient.getDatabase(autoEncryptingCollectionNamespace.getDatabaseName())
                .getCollection(autoEncryptingCollectionNamespace.getCollectionName(), BsonDocument.class);

        clientEncryption = ClientEncryptions.create(ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettings())
                .keyVaultNamespace(keyVaultNamespace.getFullName())
                .kmsProviders(providerProperties)
                .build());
    }

    @Test
    @DisplayName("test BSON size limits")
    void testBsonSizeLimits() {
        // over_2mib_under_16mib
        assertDoesNotThrow(() ->
                autoEncryptingDataCollection.insertOne(
                        new BsonDocument("_id", new BsonString("over_2mib_under_16mib"))
                                .append("unencrypted", new BsonString(repeatChar('a', 2097152)))));

        // encryption_exceeds_2mib
        assertDoesNotThrow(() ->
                autoEncryptingDataCollection.insertOne(getTestDocument("client-side-encryption/limits/limits-doc.json")
                        .append("_id", new BsonString("encryption_exceeds_2mib"))
                        .append("unencrypted", new BsonString(repeatChar('a', 2097152 - 2000)))));

        // insertMany over 2mib
        commandListener.reset();
        assertDoesNotThrow(() ->
                autoEncryptingDataCollection.insertMany(Arrays.asList(
                        new BsonDocument("_id", new BsonString("over_2mib_1"))
                                .append("unencrypted", new BsonString(repeatChar('a', 2097152))),
                        new BsonDocument("_id", new BsonString("over_2mib_2"))
                                .append("unencrypted", new BsonString(repeatChar('a', 2097152))))));
        assertEquals(2, countStartedEvents("insert"));

        // insertMany encryption_exceeds_2mib
        commandListener.reset();
        assertDoesNotThrow(() ->
                autoEncryptingDataCollection.insertMany(Arrays.asList(
                        getTestDocument("client-side-encryption/limits/limits-doc.json")
                                .append("_id", new BsonString("encryption_exceeds_2mib_1"))
                                .append("unencrypted", new BsonString(repeatChar('a', 2097152 - 2000))),
                        getTestDocument("client-side-encryption/limits/limits-doc.json")
                                .append("_id", new BsonString("encryption_exceeds_2mib_2"))
                                .append("unencrypted", new BsonString(repeatChar('a', 2097152 - 2000))))));
        assertEquals(2, countStartedEvents("insert"));

        // under_16mib
        assertDoesNotThrow(() ->
                autoEncryptingDataCollection.insertOne(
                        new BsonDocument("_id", new BsonString("under_16mib"))
                                .append("unencrypted", new BsonString(repeatChar('a', 16777216 - 2000)))));

        // encryption_exceeds_16mib should throw
        assertThrows(MongoWriteException.class, () ->
                autoEncryptingDataCollection.insertOne(getTestDocument("client-side-encryption/limits/limits-doc.json")
                        .append("_id", new BsonString("encryption_exceeds_16mib"))
                        .append("unencrypted", new BsonString(repeatChar('a', 16777216 - 2000)))));
    }

    private int countStartedEvents(final String name) {
        int count = 0;
        for (CommandStartedEvent cur : commandListener.getCommandStartedEvents()) {
            if (cur.getCommandName().equals(name)) {
                count++;
            }
        }
        return count;
    }

    private static String repeatChar(final char c, final int count) {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, c);
        return new String(chars);
    }
}
