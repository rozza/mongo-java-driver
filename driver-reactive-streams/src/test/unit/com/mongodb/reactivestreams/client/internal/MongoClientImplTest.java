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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.TransactionOptions;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.async.client.OperationExecutor;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListDatabasesPublisher;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import reactor.core.publisher.Mono;

import java.util.List;

import static com.mongodb.reactivestreams.client.internal.PublisherCreator.createChangeStreamPublisher;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.createListDatabasesPublisher;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class MongoClientImplTest extends TestHelper {

    @Mock
    private ClientSession clientSession;

    @Test
    void testListDatabases() {
        MongoClientImpl mongoClient = createMongoClient();

        assertAll("listDatabases",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoClient.listDatabases((Class<?>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoClient.listDatabases((ClientSession) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> mongoClient.listDatabases(clientSession, null))),
                  () -> {
                      ListDatabasesPublisher<Document> expected =
                              createListDatabasesPublisher(null, Document.class, mongoClient.getSettings().getCodecRegistry(),
                                                           mongoClient.getSettings().getReadPreference(), mongoClient.getExecutor(),
                                                           mongoClient.getSettings().getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoClient.listDatabases(), "Default");
                  },
                  () -> {
                      ListDatabasesPublisher<Document> expected =
                              createListDatabasesPublisher(clientSession, Document.class, mongoClient.getSettings().getCodecRegistry(),
                                                           mongoClient.getSettings().getReadPreference(), mongoClient.getExecutor(),
                                                           mongoClient.getSettings().getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoClient.listDatabases(clientSession), "With session");
                  },
                  () -> {
                      ListDatabasesPublisher<BsonDocument> expected =
                              createListDatabasesPublisher(null, BsonDocument.class, mongoClient.getSettings().getCodecRegistry(),
                                                           mongoClient.getSettings().getReadPreference(), mongoClient.getExecutor(),
                                                           mongoClient.getSettings().getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoClient.listDatabases(BsonDocument.class), "Alternative class");
                  },
                  () -> {
                      ListDatabasesPublisher<BsonDocument> expected =
                              createListDatabasesPublisher(clientSession, BsonDocument.class, mongoClient.getSettings().getCodecRegistry(),
                                                           mongoClient.getSettings().getReadPreference(), mongoClient.getExecutor(),
                                                           mongoClient.getSettings().getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoClient.listDatabases(clientSession, BsonDocument.class),
                                                 "Alternative class with session");
                  }
        );
    }

    @Test
    void testListDatabaseNames() {
        MongoClientImpl mongoClient = createMongoClient();

        assertAll("listDatabaseNames",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoClient.listDatabaseNames(null))),
                  () -> {
                      ListDatabasesPublisher<Document> expected =
                              createListDatabasesPublisher(null, Document.class, mongoClient.getSettings().getCodecRegistry(),
                                                           mongoClient.getSettings().getReadPreference(), mongoClient.getExecutor(),
                                                           mongoClient.getSettings().getRetryReads())
                                      .nameOnly(true);

                      assertPublisherIsTheSameAs(expected, mongoClient.listDatabaseNames(), "Default");
                  },
                  () -> {
                      ListDatabasesPublisher<Document> expected =
                              createListDatabasesPublisher(clientSession, Document.class, mongoClient.getSettings().getCodecRegistry(),
                                                           mongoClient.getSettings().getReadPreference(), mongoClient.getExecutor(),
                                                           mongoClient.getSettings().getRetryReads())
                                      .nameOnly(true);

                      assertPublisherIsTheSameAs(expected, mongoClient.listDatabaseNames(clientSession), "With session");
                  }
        );
    }

    @Test
    void testWatch() {
        MongoClientImpl mongoClient = createMongoClient();
        List<Bson> pipeline = singletonList(BsonDocument.parse("{$match: {open: true}}"));

        assertAll("watch",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoClient.watch((Class<?>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoClient.watch((List<Bson>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoClient.watch(pipeline, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoClient.watch((ClientSession) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoClient.watch(null, pipeline)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> mongoClient.watch(null, pipeline, Document.class))
                  ),
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              createChangeStreamPublisher(null, "admin", Document.class,
                                                          mongoClient.getSettings().getCodecRegistry(),
                                                          mongoClient.getSettings().getReadPreference(),
                                                          mongoClient.getSettings().getReadConcern(), mongoClient.getExecutor(),
                                                          emptyList(), ChangeStreamLevel.CLIENT, mongoClient.getSettings().getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoClient.watch(), "Default");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              createChangeStreamPublisher(null, "admin", Document.class,
                                                          mongoClient.getSettings().getCodecRegistry(),
                                                          mongoClient.getSettings().getReadPreference(),
                                                          mongoClient.getSettings().getReadConcern(), mongoClient.getExecutor(), pipeline,
                                                          ChangeStreamLevel.CLIENT, mongoClient.getSettings().getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoClient.watch(pipeline), "With pipeline");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              createChangeStreamPublisher(null, "admin", BsonDocument.class,
                                                          mongoClient.getSettings().getCodecRegistry(),
                                                          mongoClient.getSettings().getReadPreference(),
                                                          mongoClient.getSettings().getReadConcern(), mongoClient.getExecutor(),
                                                          emptyList(), ChangeStreamLevel.CLIENT, mongoClient.getSettings().getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoClient.watch(BsonDocument.class),
                                                 "With result class");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              createChangeStreamPublisher(null, "admin", BsonDocument.class,
                                                          mongoClient.getSettings().getCodecRegistry(),
                                                          mongoClient.getSettings().getReadPreference(),
                                                          mongoClient.getSettings().getReadConcern(), mongoClient.getExecutor(), pipeline,
                                                          ChangeStreamLevel.CLIENT, mongoClient.getSettings().getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoClient.watch(pipeline, BsonDocument.class),
                                                 "With pipeline & result class");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              createChangeStreamPublisher(clientSession, "admin", Document.class,
                                                          mongoClient.getSettings().getCodecRegistry(),
                                                          mongoClient.getSettings().getReadPreference(),
                                                          mongoClient.getSettings().getReadConcern(), mongoClient.getExecutor(),
                                                          emptyList(), ChangeStreamLevel.CLIENT, mongoClient.getSettings().getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoClient.watch(clientSession), "with session");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              createChangeStreamPublisher(clientSession, "admin", Document.class,
                                                          mongoClient.getSettings().getCodecRegistry(),
                                                          mongoClient.getSettings().getReadPreference(),
                                                          mongoClient.getSettings().getReadConcern(), mongoClient.getExecutor(), pipeline,
                                                          ChangeStreamLevel.CLIENT, mongoClient.getSettings().getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoClient.watch(clientSession, pipeline), "With session & pipeline");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              createChangeStreamPublisher(clientSession, "admin", BsonDocument.class,
                                                          mongoClient.getSettings().getCodecRegistry(),
                                                          mongoClient.getSettings().getReadPreference(),
                                                          mongoClient.getSettings().getReadConcern(), mongoClient.getExecutor(),
                                                          emptyList(), ChangeStreamLevel.CLIENT, mongoClient.getSettings().getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoClient.watch(clientSession, BsonDocument.class),
                                                 "With session & resultClass");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              createChangeStreamPublisher(clientSession, "admin", BsonDocument.class,
                                                          mongoClient.getSettings().getCodecRegistry(),
                                                          mongoClient.getSettings().getReadPreference(),
                                                          mongoClient.getSettings().getReadConcern(), mongoClient.getExecutor(), pipeline,
                                                          ChangeStreamLevel.CLIENT, mongoClient.getSettings().getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoClient.watch(clientSession, pipeline, BsonDocument.class),
                                                 "With clientSession, pipeline & result class");
                  }
        );
    }

    @Test
    void testStartSession() {
        ServerDescription serverDescription = ServerDescription.builder()
                .address(new ServerAddress())
                .state(ServerConnectionState.CONNECTED)
                .maxWireVersion(8)
                .build();

        MongoClientImpl mongoClient = createMongoClient();
        Cluster cluster = mongoClient.getCluster();
        when(cluster.getCurrentDescription())
                .thenReturn(new ClusterDescription(ClusterConnectionMode.SINGLE, ClusterType.STANDALONE, singletonList(serverDescription)));

        ServerSessionPool serverSessionPool = mock(ServerSessionPool.class);
        ClientSessionHelper clientSessionHelper = new ClientSessionHelper(mongoClient, serverSessionPool);

        assertAll("Start Session Tests",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoClient.startSession(null))
                  ),
                  () -> {
                      Mono<ClientSession> expected =
                              clientSessionHelper.createClientSessionMono(ClientSessionOptions.builder().build(),
                                                                          mongoClient.getExecutor());
                      assertPublisherIsTheSameAs(expected, mongoClient.startSession(), "Default");
                  },
                  () -> {
                      ClientSessionOptions options = ClientSessionOptions.builder()
                              .causallyConsistent(true)
                              .defaultTransactionOptions(TransactionOptions.builder().readConcern(ReadConcern.LINEARIZABLE).build())
                              .build();
                      Mono<ClientSession> expected =
                              clientSessionHelper.createClientSessionMono(options, mongoClient.getExecutor());
                      assertPublisherIsTheSameAs(expected, mongoClient.startSession(options), "with options");
                  });
    }

    private MongoClientImpl createMongoClient() {
        return new MongoClientImpl(MongoClientSettings.builder().build(), mock(Cluster.class), mock(OperationExecutor.class));
    }
}
