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

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.Fixture.getDefaultDatabaseName;
import static com.mongodb.Fixture.getMongoClient;
import static com.mongodb.Fixture.getMongoClientURI;
import static com.mongodb.Fixture.getOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@SuppressWarnings("deprecation")
class MongoClientSessionTest extends FunctionalSpecification {

    @Test
    @DisplayName("should throw IllegalArgumentException if options are null")
    void shouldThrowIfOptionsAreNull() {
        assertThrows(IllegalArgumentException.class, () -> getMongoClient().startSession(null));
    }

    @Test
    @DisplayName("should create session with correct defaults")
    void shouldCreateSessionWithCorrectDefaults() {
        ClientSession clientSession = getMongoClient().startSession();
        try {
            assertEquals(getMongoClient().getDelegate(), clientSession.getOriginator());
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
    @DisplayName("cluster time should advance")
    void clusterTimeShouldAdvance() {
        BsonTimestamp firstOperationTime = new BsonTimestamp(42, 1);
        BsonTimestamp secondOperationTime = new BsonTimestamp(52, 1);
        BsonTimestamp thirdOperationTime = new BsonTimestamp(22, 1);
        BsonDocument firstClusterTime = new BsonDocument("clusterTime", firstOperationTime);
        BsonDocument secondClusterTime = new BsonDocument("clusterTime", secondOperationTime);
        BsonDocument olderClusterTime = new BsonDocument("clusterTime", thirdOperationTime);

        ClientSession clientSession = getMongoClient().startSession(ClientSessionOptions.builder().build());
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
    @DisplayName("operation time should advance")
    void operationTimeShouldAdvance() {
        BsonTimestamp firstOperationTime = new BsonTimestamp(42, 1);
        BsonTimestamp secondOperationTime = new BsonTimestamp(52, 1);
        BsonTimestamp olderOperationTime = new BsonTimestamp(22, 1);

        ClientSession clientSession = getMongoClient().startSession(ClientSessionOptions.builder().build());
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
    @DisplayName("methods that use the session should throw if the session is closed")
    void shouldThrowIfSessionIsClosed() {
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

    @ParameterizedTest(name = "causallyConsistent={0}")
    @ValueSource(booleans = {true, false})
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

    @Test
    @DisplayName("client session should have server session with valid identifier")
    void clientSessionShouldHaveValidServerSessionIdentifier() {
        ClientSession clientSession = getMongoClient().startSession(ClientSessionOptions.builder().build());
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
    @DisplayName("should use a default session")
    void shouldUseADefaultSession() {
        TestCommandListener commandListener = new TestCommandListener();
        MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder(getOptions())
                .addCommandListener(commandListener);
        MongoClient client = new MongoClient(getMongoClientURI(optionsBuilder));
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
    @ParameterizedTest(name = "readConcern={0}")
    @MethodSource("readConcernsForCausalConsistency")
    @DisplayName("should find inserted document on a secondary when causal consistency is enabled")
    void shouldFindInsertedDocumentOnSecondaryWithCausalConsistency(final ReadConcern readConcern) {
        MongoCollection<Document> coll = getMongoClient().getDatabase(getDefaultDatabaseName())
                .getCollection(getCollectionName());

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
                    Assert.fail("Should have found recently inserted document on secondary with causal consistency enabled");
                }
            }
        } finally {
            clientSession.close();
        }
    }

    static Stream<ReadConcern> readConcernsForCausalConsistency() {
        return Stream.of(ReadConcern.DEFAULT, ReadConcern.LOCAL, ReadConcern.MAJORITY);
    }

    @Test
    @DisplayName("should not use an implicit session for an unacknowledged write")
    void shouldNotUseImplicitSessionForUnacknowledgedWrite() {
        TestCommandListener commandListener = new TestCommandListener();
        MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder(getOptions())
                .addCommandListener(commandListener);
        MongoClientURI mongoClientURI = getMongoClientURI(optionsBuilder);
        MongoClient client = new MongoClient(mongoClientURI);
        MongoCollection<Document> coll = client.getDatabase(getDatabaseName()).getCollection(getCollectionName());
        ObjectId id = new ObjectId();
        try {
            coll.withWriteConcern(WriteConcern.UNACKNOWLEDGED).insertOne(new Document("_id", id));

            CommandStartedEvent insertEvent = (CommandStartedEvent) commandListener.getEvents().get(0);
            assertFalse(insertEvent.getCommand().containsKey("lsid"));
        } finally {
            waitForInsertAcknowledgement(coll, id);
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
            coll.withWriteConcern(WriteConcern.UNACKNOWLEDGED)
                    .insertOne(session, new Document());
            // No exception thrown means test passes
        } finally {
            session.close();
        }
    }

    private void waitForInsertAcknowledgement(final MongoCollection<Document> collection, final ObjectId id) {
        Document document = collection.find(Filters.eq(id)).first();
        while (document == null) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            document = collection.find(Filters.eq(id)).first();
        }
    }
}
