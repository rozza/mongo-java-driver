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

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoClientException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.reactivestreams.client.Fixture.getDefaultDatabase;
import static com.mongodb.reactivestreams.client.Fixture.getMongoClient;
import static com.mongodb.reactivestreams.client.Fixture.getMongoClientBuilderFromConnectionString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class MongoClientSessionTest extends FunctionalSpecification {

    @Test
    void shouldThrowIllegalArgumentExceptionIfOptionsAreNull() {
        assertThrows(IllegalArgumentException.class, () -> getMongoClient().startSession(null));
    }

    @Test
    void shouldCreateSessionWithCorrectDefaults() {
        ClientSessionOptions options = ClientSessionOptions.builder().build();
        ClientSession clientSession = startSession(options);
        try {
            assertNotNull(clientSession);
            assertEquals(getMongoClient(), clientSession.getOriginator());
            assertTrue(clientSession.isCausallyConsistent());
            assertEquals(ClientSessionOptions.builder()
                    .defaultTransactionOptions(TransactionOptions.builder()
                            .readConcern(ReadConcern.DEFAULT)
                            .writeConcern(WriteConcern.ACKNOWLEDGED)
                            .readPreference(ReadPreference.primary())
                            .build())
                    .build(), clientSession.getOptions());
            assertNull(clientSession.getClusterTime());
            assertNull(clientSession.getOperationTime());
            assertNotNull(clientSession.getServerSession());
        } finally {
            clientSession.close();
        }
    }

    @Test
    void clusterTimeShouldAdvance() {
        BsonTimestamp firstOperationTime = new BsonTimestamp(42, 1);
        BsonTimestamp secondOperationTime = new BsonTimestamp(52, 1);
        BsonTimestamp thirdOperationTime = new BsonTimestamp(22, 1);
        BsonDocument firstClusterTime = new BsonDocument("clusterTime", firstOperationTime);
        BsonDocument secondClusterTime = new BsonDocument("clusterTime", secondOperationTime);
        BsonDocument olderClusterTime = new BsonDocument("clusterTime", thirdOperationTime);

        ClientSession clientSession = startSession(ClientSessionOptions.builder().build());
        try {
            assertNull(clientSession.getClusterTime());

            clientSession.advanceClusterTime(null);
            assertNull(clientSession.getClusterTime());

            clientSession.advanceClusterTime(firstClusterTime);
            assertEquals(firstClusterTime, clientSession.getClusterTime());

            clientSession.advanceClusterTime(secondClusterTime);
            assertEquals(secondClusterTime, clientSession.getClusterTime());

            clientSession.advanceClusterTime(olderClusterTime);
            assertEquals(secondClusterTime, clientSession.getClusterTime());
        } finally {
            clientSession.close();
        }
    }

    @Test
    void operationTimeShouldAdvance() {
        BsonTimestamp firstOperationTime = new BsonTimestamp(42, 1);
        BsonTimestamp secondOperationTime = new BsonTimestamp(52, 1);
        BsonTimestamp olderOperationTime = new BsonTimestamp(22, 1);

        ClientSession clientSession = startSession(ClientSessionOptions.builder().build());
        try {
            assertNull(clientSession.getOperationTime());

            clientSession.advanceOperationTime(null);
            assertNull(clientSession.getOperationTime());

            clientSession.advanceOperationTime(firstOperationTime);
            assertEquals(firstOperationTime, clientSession.getOperationTime());

            clientSession.advanceOperationTime(secondOperationTime);
            assertEquals(secondOperationTime, clientSession.getOperationTime());

            clientSession.advanceOperationTime(olderOperationTime);
            assertEquals(secondOperationTime, clientSession.getOperationTime());
        } finally {
            clientSession.close();
        }
    }

    @Test
    void methodsThatUseTheSessionShouldThrowIfTheSessionIsClosed() {
        ClientSessionOptions options = ClientSessionOptions.builder().build();
        ClientSession clientSession = startSession(options);
        clientSession.close();

        assertThrows(IllegalStateException.class, () -> clientSession.getServerSession());
        assertThrows(IllegalStateException.class, () -> clientSession.advanceOperationTime(new BsonTimestamp(42, 0)));
        assertThrows(IllegalStateException.class, () -> clientSession.advanceClusterTime(new BsonDocument()));
    }

    @Test
    void informationalMethodsShouldNotThrowIfTheSessionIsClosed() {
        ClientSessionOptions options = ClientSessionOptions.builder().build();
        ClientSession clientSession = startSession(options);
        clientSession.close();

        clientSession.getOptions();
        clientSession.isCausallyConsistent();
        clientSession.getClusterTime();
        clientSession.getOperationTime();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldApplyCausallyConsistentSessionOptionToClientSession(final boolean causallyConsistent) {
        ClientSession clientSession = startSession(ClientSessionOptions.builder()
                .causallyConsistent(causallyConsistent).build());
        try {
            assertNotNull(clientSession);
            assertEquals(causallyConsistent, clientSession.isCausallyConsistent());
        } finally {
            clientSession.close();
        }
    }

    @Test
    void clientSessionShouldHaveServerSessionWithValidIdentifier() {
        ClientSession clientSession = startSession(ClientSessionOptions.builder().build());
        try {
            BsonDocument identifier = clientSession.getServerSession().getIdentifier();
            assertEquals(1, identifier.size());
            assertTrue(identifier.containsKey("id"));
            assertTrue(identifier.get("id").isBinary());
            assertEquals(BsonBinarySubType.UUID_STANDARD.getValue(), identifier.getBinary("id").getType());
            assertEquals(16, identifier.getBinary("id").getData().length);
        } finally {
            clientSession.close();
        }
    }

    @Test
    void shouldUseADefaultSession() {
        TestCommandListener commandListener = new TestCommandListener();
        MongoClientSettings options = getMongoClientBuilderFromConnectionString()
                .addCommandListener(commandListener).build();
        MongoClient client = MongoClients.create(options);
        try {
            Mono.from(client.getDatabase("admin").runCommand(new BsonDocument("ping", new BsonInt32(1))))
                    .block(TIMEOUT_DURATION);

            assertEquals(2, commandListener.getEvents().size());
            CommandStartedEvent pingCommandStartedEvent = (CommandStartedEvent) commandListener.getEvents().get(0);
            assertTrue(pingCommandStartedEvent.getCommand().containsKey("lsid"));
        } finally {
            client.close();
        }
    }

    @Test
    void shouldThrowExceptionIfUnacknowledgedWriteUsedWithExplicitSession() {
        ClientSession session = Mono.from(getMongoClient().startSession()).block(TIMEOUT_DURATION);
        try {
            assertThrows(MongoClientException.class, () ->
                    Mono.from(getMongoClient().getDatabase(getDatabaseName()).getCollection(getCollectionName())
                            .withWriteConcern(WriteConcern.UNACKNOWLEDGED)
                            .insertOne(session, new Document()))
                            .block(TIMEOUT_DURATION));
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    @Test
    void shouldIgnoreUnacknowledgedWriteConcernWhenInATransaction() {
        assumeTrue(isDiscoverableReplicaSet());

        MongoCollection<Document> col = getMongoClient().getDatabase(getDatabaseName()).getCollection(getCollectionName());
        Mono.from(col.insertOne(new Document())).block(TIMEOUT_DURATION);

        ClientSession session = Mono.from(getMongoClient().startSession()).block(TIMEOUT_DURATION);
        try {
            session.startTransaction();
            Mono.from(col.withWriteConcern(WriteConcern.UNACKNOWLEDGED)
                    .insertOne(session, new Document())).block(TIMEOUT_DURATION);
        } finally {
            session.close();
        }
    }

    // This test attempts to demonstrate that causal consistency works correctly by inserting a document and then
    // immediately searching for that document on a secondary by its _id and failing the test if the document is not
    // found.  Without causal consistency enabled the expectation is that eventually that test would fail since
    // generally the find will execute on the secondary before the secondary has a chance to replicate the document.
    // This test is inherently racy as it's possible that the server _does_ replicate fast enough and therefore the
    // test passes anyway even if causal consistency was not actually in effect.  For that reason the test iterates a
    // number of times in order to increase confidence that it's really causal consistency that is causing the test
    // to succeed.
    @Tag("Slow")
    @ParameterizedTest
    @MethodSource("readConcerns")
    void shouldFindInsertedDocumentOnASecondaryWhenCausalConsistencyIsEnabled(final ReadConcern readConcern) {
        MongoCollection<Document> col = getDefaultDatabase().getCollection(getCollectionName());

        ClientSession clientSession = startSession(ClientSessionOptions.builder().causallyConsistent(true).build());
        try {
            for (int i = 0; i < 16; i++) {
                Document document = new Document("_id", i);
                Mono.from(col.insertOne(clientSession, document)).block(TIMEOUT_DURATION);
                Document foundDocument = Mono.from(col
                        .withReadPreference(ReadPreference.secondaryPreferred())
                        .withReadConcern(readConcern)
                        .find(clientSession, document)
                        .maxTime(30, TimeUnit.SECONDS)
                        .first()
                ).block(TIMEOUT_DURATION);
                if (foundDocument == null) {
                    fail("Should have found recently inserted document on secondary with causal consistency enabled");
                }
            }
        } finally {
            clientSession.close();
        }
    }

    private static Stream<ReadConcern> readConcerns() {
        return Stream.of(ReadConcern.DEFAULT, ReadConcern.LOCAL, ReadConcern.MAJORITY);
    }

    private static ClientSession startSession(final ClientSessionOptions options) {
        return Mono.from(getMongoClient().startSession(options)).block(TIMEOUT_DURATION);
    }
}
