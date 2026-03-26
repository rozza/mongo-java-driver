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

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Filters;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.internal.time.Timeout;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClient;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class MongoClientSessionTest extends FunctionalSpecification {

    @Test
    @DisplayName("should throw IllegalArgumentException if options are null")
    void shouldThrowIfOptionsAreNull() {
        assertThrows(IllegalArgumentException.class, () -> getMongoClient().startSession(null));
    }

    @ParameterizedTest
    @MethodSource("clientSessions")
    @DisplayName("should create session with correct defaults")
    void shouldCreateSessionWithCorrectDefaults(final ClientSession clientSession) {
        try {
            assertEquals(getMongoClient(), clientSession.getOriginator());
            assertTrue(clientSession.isCausallyConsistent());
            assertEquals(ClientSessionOptions.builder()
                            .defaultTransactionOptions(TransactionOptions.builder()
                                    .readConcern(ReadConcern.DEFAULT)
                                    .writeConcern(WriteConcern.ACKNOWLEDGED)
                                    .readPreference(ReadPreference.primary())
                                    .build())
                            .build(),
                    clientSession.getOptions());
            assertNull(clientSession.getClusterTime());
            assertNull(clientSession.getOperationTime());
            assertNotNull(clientSession.getServerSession());
        } finally {
            clientSession.close();
        }
    }

    static Stream<ClientSession> clientSessions() {
        return Stream.of(
                getMongoClient().startSession(),
                getMongoClient().startSession(ClientSessionOptions.builder().build()));
    }

    @Test
    @DisplayName("cluster time should advance")
    void clusterTimeShouldAdvance() {
        BsonTimestamp firstOperationTime = new BsonTimestamp(42, 1);
        BsonTimestamp secondOperationTime = new BsonTimestamp(52, 1);
        BsonTimestamp thirdOperationTime = new BsonTimestamp(22, 1);
        BsonDocument firstClusterTime = new BsonDocument("clusterTime", firstOperationTime);
        BsonDocument secondClusterTime = new BsonDocument("clusterTime", secondOperationTime);
        BsonDocument olderClusterTime = new BsonDocument("clusterTime", thirdOperationTime);

        ClientSession clientSession = getMongoClient().startSession(ClientSessionOptions.builder().build());

        assertNull(clientSession.getClusterTime());

        clientSession.advanceClusterTime(null);
        assertNull(clientSession.getClusterTime());

        clientSession.advanceClusterTime(firstClusterTime);
        assertEquals(firstClusterTime, clientSession.getClusterTime());

        clientSession.advanceClusterTime(secondClusterTime);
        assertEquals(secondClusterTime, clientSession.getClusterTime());

        clientSession.advanceClusterTime(olderClusterTime);
        assertEquals(secondClusterTime, clientSession.getClusterTime());
    }

    @Test
    @DisplayName("operation time should advance")
    void operationTimeShouldAdvance() {
        BsonTimestamp firstOperationTime = new BsonTimestamp(42, 1);
        BsonTimestamp secondOperationTime = new BsonTimestamp(52, 1);
        BsonTimestamp olderOperationTime = new BsonTimestamp(22, 1);

        ClientSession clientSession = getMongoClient().startSession(ClientSessionOptions.builder().build());

        assertNull(clientSession.getOperationTime());

        clientSession.advanceOperationTime(null);
        assertNull(clientSession.getOperationTime());

        clientSession.advanceOperationTime(firstOperationTime);
        assertEquals(firstOperationTime, clientSession.getOperationTime());

        clientSession.advanceOperationTime(secondOperationTime);
        assertEquals(secondOperationTime, clientSession.getOperationTime());

        clientSession.advanceOperationTime(olderOperationTime);
        assertEquals(secondOperationTime, clientSession.getOperationTime());
    }

    @Test
    @DisplayName("methods that use the session should throw if the session is closed")
    void methodsShouldThrowIfSessionIsClosed() {
        ClientSessionOptions options = ClientSessionOptions.builder().build();
        ClientSession clientSession = getMongoClient().startSession(options);
        clientSession.close();

        assertThrows(IllegalStateException.class, () -> clientSession.getServerSession());
        assertThrows(IllegalStateException.class, () -> clientSession.advanceOperationTime(new BsonTimestamp(42, 0)));
        assertThrows(IllegalStateException.class, () -> clientSession.advanceClusterTime(new BsonDocument()));
    }

    @Test
    @DisplayName("informational methods should not throw if the session is closed")
    void informationalMethodsShouldNotThrowIfSessionIsClosed() {
        ClientSessionOptions options = ClientSessionOptions.builder().build();
        ClientSession clientSession = getMongoClient().startSession(options);
        clientSession.close();

        // These should not throw
        clientSession.getOptions();
        clientSession.isCausallyConsistent();
        clientSession.getClusterTime();
        clientSession.getOperationTime();
    }

    @ParameterizedTest
    @MethodSource("causallyConsistentValues")
    @DisplayName("should apply causally consistent session option to client session")
    void shouldApplyCausallyConsistentSessionOption(final boolean causallyConsistent) {
        ClientSession clientSession = getMongoClient().startSession(ClientSessionOptions.builder()
                .causallyConsistent(causallyConsistent)
                .build());
        try {
            assertNotNull(clientSession);
            assertEquals(causallyConsistent, clientSession.isCausallyConsistent());
        } finally {
            clientSession.close();
        }
    }

    static Stream<Boolean> causallyConsistentValues() {
        return Stream.of(true, false);
    }

    @Test
    @DisplayName("client session should have server session with valid identifier")
    void clientSessionShouldHaveServerSessionWithValidIdentifier() {
        ClientSession clientSession = getMongoClient().startSession(ClientSessionOptions.builder().build());

        BsonDocument identifier = clientSession.getServerSession().getIdentifier();

        assertEquals(1, identifier.size());
        assertTrue(identifier.containsKey("id"));
        assertTrue(identifier.get("id").isBinary());
        assertEquals(BsonBinarySubType.UUID_STANDARD.getValue(), identifier.getBinary("id").getType());
        assertEquals(16, identifier.getBinary("id").getData().length);
    }

    @Test
    @DisplayName("should use a default session")
    void shouldUseADefaultSession() {
        TestCommandListener commandListener = new TestCommandListener();
        MongoClientSettings settings = MongoClientSettings.builder(getMongoClientSettings())
                .commandListenerList(Collections.singletonList(commandListener)).build();
        MongoClient client = MongoClients.create(settings);
        try {
            client.getDatabase("admin").runCommand(new BsonDocument("ping", new BsonInt32(1)));

            assertEquals(2, commandListener.getEvents().size());
            CommandStartedEvent pingCommandStartedEvent = (CommandStartedEvent) commandListener.getEvents().get(0);
            assertTrue(pingCommandStartedEvent.getCommand().containsKey("lsid"));
        } finally {
            client.close();
        }
    }

    @Tag("Slow")
    @ParameterizedTest
    @MethodSource("readConcerns")
    @DisplayName("should find inserted document on a secondary when causal consistency is enabled")
    void shouldFindInsertedDocumentOnSecondaryWithCausalConsistency(final ReadConcern readConcern) {
        MongoCollection<Document> coll = getMongoClient().getDatabase(getDefaultDatabaseName()).getCollection(getCollectionName());

        ClientSession clientSession = getMongoClient().startSession(ClientSessionOptions.builder()
                .causallyConsistent(true)
                .build());
        try {
            for (int i = 0; i < 16; i++) {
                Document document = new Document("_id", i);
                coll.insertOne(clientSession, document);
                Document foundDocument = coll
                        .withReadPreference(ReadPreference.secondaryPreferred())
                        .withReadConcern(readConcern)
                        .find(clientSession, document)
                        .maxTime(30, TimeUnit.SECONDS)
                        .first();
                if (foundDocument == null) {
                    fail("Should have found recently inserted document on secondary with causal consistency enabled");
                }
            }
        } finally {
            clientSession.close();
        }
    }

    static Stream<ReadConcern> readConcerns() {
        return Stream.of(ReadConcern.DEFAULT, ReadConcern.LOCAL, ReadConcern.MAJORITY);
    }

    @Test
    @DisplayName("should not use an implicit session for an unacknowledged write")
    void shouldNotUseImplicitSessionForUnacknowledgedWrite() {
        TestCommandListener commandListener = new TestCommandListener();
        MongoClientSettings settings = MongoClientSettings.builder(getMongoClientSettings())
                .commandListenerList(Collections.singletonList(commandListener)).build();
        MongoClient client = MongoClients.create(settings);
        try {
            MongoCollection<Document> coll = client.getDatabase(getDatabaseName()).getCollection(getCollectionName());
            ObjectId id = new ObjectId();

            coll.withWriteConcern(WriteConcern.UNACKNOWLEDGED).insertOne(new Document("_id", id));

            CommandStartedEvent insertEvent = (CommandStartedEvent) commandListener.getEvents().get(0);
            assertTrue(!insertEvent.getCommand().containsKey("lsid"));

            waitForInsertAcknowledgement(coll, id);
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("should throw exception if unacknowledged write used with explicit session")
    void shouldThrowExceptionIfUnacknowledgedWriteUsedWithExplicitSession() {
        ClientSession session = getMongoClient().startSession();
        try {
            assertThrows(MongoClientException.class, () ->
                    getMongoClient().getDatabase(getDatabaseName()).getCollection(getCollectionName())
                            .withWriteConcern(WriteConcern.UNACKNOWLEDGED)
                            .insertOne(session, new Document()));
        } finally {
            session.close();
        }
    }

    @Test
    @DisplayName("should ignore unacknowledged write concern when in a transaction")
    void shouldIgnoreUnacknowledgedWriteConcernInTransaction() {
        assumeTrue(isDiscoverableReplicaSet());

        MongoCollection<Document> coll = getMongoClient().getDatabase(getDatabaseName()).getCollection(getCollectionName());
        coll.insertOne(new Document());

        ClientSession session = getMongoClient().startSession();
        try {
            session.startTransaction();
            // Should not throw
            coll.withWriteConcern(WriteConcern.UNACKNOWLEDGED)
                    .insertOne(session, new Document());
        } finally {
            session.close();
        }
    }

    private void waitForInsertAcknowledgement(final MongoCollection<Document> collection, final ObjectId id) {
        Document document = collection.find(Filters.eq(id)).first();
        Timeout timeout = Timeout.expiresIn(5, TimeUnit.SECONDS, Timeout.ZeroSemantics.ZERO_DURATION_MEANS_INFINITE);
        while (document == null) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            document = collection.find(Filters.eq(id)).first();
            timeout.onExpired(() -> {
                throw new AssertionError("Timed out waiting for insert acknowledgement");
            });
        }
    }
}
