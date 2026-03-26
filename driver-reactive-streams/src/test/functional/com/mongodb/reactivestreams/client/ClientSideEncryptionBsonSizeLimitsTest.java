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

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.reactivestreams.client.vault.ClientEncryption;
import com.mongodb.reactivestreams.client.vault.ClientEncryptions;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.reactivestreams.client.Fixture.drop;
import static com.mongodb.reactivestreams.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.reactivestreams.client.Fixture.getMongoClientBuilderFromConnectionString;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static util.JsonPoweredTestHelper.getTestDocument;

public class ClientSideEncryptionBsonSizeLimitsTest extends FunctionalSpecification {

    private final MongoNamespace keyVaultNamespace = new MongoNamespace("test.datakeys");
    private final MongoNamespace autoEncryptingCollectionNamespace = new MongoNamespace(getDefaultDatabaseName(),
            "ClientSideEncryptionProseTestSpecification");
    private final TestCommandListener commandListener = new TestCommandListener();

    private MongoClient autoEncryptingClient;
    private ClientEncryption clientEncryption;
    private MongoCollection<BsonDocument> autoEncryptingDataCollection;

    @BeforeEach
    @Override
    public void setUp() {
        super.setUp();
        assumeTrue(!System.getProperty("AWS_ACCESS_KEY_ID", "").isEmpty(),
                "Key vault tests disabled");
        drop(keyVaultNamespace);
        drop(autoEncryptingCollectionNamespace);

        new CollectionHelper<>(new BsonDocumentCodec(), keyVaultNamespace).insertDocuments(
                singletonList(getTestDocument("client-side-encryption/limits/limits-key.json")),
                WriteConcern.MAJORITY);

        Map<String, Map<String, Object>> providerProperties = new HashMap<>();
        Map<String, Object> localProperties = new HashMap<>();
        localProperties.put("key", Base64.getDecoder().decode("Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN"
                + "3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk"));
        providerProperties.put("local", localProperties);

        autoEncryptingClient = MongoClients.create(getMongoClientBuilderFromConnectionString()
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
                .keyVaultMongoClientSettings(getMongoClientBuilderFromConnectionString().build())
                .keyVaultNamespace(keyVaultNamespace.getFullName())
                .kmsProviders(providerProperties)
                .build());
    }

    @AfterEach
    @Override
    public void tearDown() {
        super.tearDown();
        if (autoEncryptingClient != null) {
            try {
                autoEncryptingClient.close();
            } catch (Exception e) {
                // ignore
            }
        }
        if (clientEncryption != null) {
            try {
                clientEncryption.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Test
    public void testBsonSizeLimits() {
        // Insert a document over 2MiB but under 16MiB
        Mono.from(autoEncryptingDataCollection.insertOne(
                new BsonDocument("_id", new BsonString("over_2mib_under_16mib"))
                        .append("unencrypted", new BsonString("a".repeat(2097152))))).block(TIMEOUT_DURATION);

        // Insert encrypted document that exceeds 2MiB
        Mono.from(autoEncryptingDataCollection.insertOne(getTestDocument("client-side-encryption/limits/limits-doc.json")
                .append("_id", new BsonString("encryption_exceeds_2mib"))
                .append("unencrypted", new BsonString("a".repeat(2097152 - 2000))))).block(TIMEOUT_DURATION);

        // Insert many documents over 2MiB - should result in 2 insert commands
        commandListener.reset();
        Mono.from(autoEncryptingDataCollection.insertMany(
                Arrays.asList(
                        new BsonDocument("_id", new BsonString("over_2mib_1"))
                                .append("unencrypted", new BsonString("a".repeat(2097152))),
                        new BsonDocument("_id", new BsonString("over_2mib_2"))
                                .append("unencrypted", new BsonString("a".repeat(2097152)))
                ))).block(TIMEOUT_DURATION);
        assertEquals(2, countStartedEvents("insert"));

        // Insert many encrypted documents that exceed 2MiB - should result in 2 insert commands
        commandListener.reset();
        Mono.from(autoEncryptingDataCollection.insertMany(
                Arrays.asList(
                        getTestDocument("client-side-encryption/limits/limits-doc.json")
                                .append("_id", new BsonString("encryption_exceeds_2mib_1"))
                                .append("unencrypted", new BsonString("a".repeat(2097152 - 2000))),
                        getTestDocument("client-side-encryption/limits/limits-doc.json")
                                .append("_id", new BsonString("encryption_exceeds_2mib_2"))
                                .append("unencrypted", new BsonString("a".repeat(2097152 - 2000)))
                ))).block(TIMEOUT_DURATION);
        assertEquals(2, countStartedEvents("insert"));

        // Insert a document under 16MiB
        Mono.from(autoEncryptingDataCollection.insertOne(
                new BsonDocument("_id", new BsonString("under_16mib"))
                        .append("unencrypted", new BsonString("a".repeat(16777216 - 2000))))).block(TIMEOUT_DURATION);

        // Insert encrypted document that exceeds 16MiB - should throw
        assertThrows(MongoWriteException.class, () ->
                Mono.from(autoEncryptingDataCollection.insertOne(getTestDocument("client-side-encryption/limits/limits-doc.json")
                        .append("_id", new BsonString("encryption_exceeds_16mib"))
                        .append("unencrypted", new BsonString("a".repeat(16777216 - 2000))))).block(TIMEOUT_DURATION));
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
}
