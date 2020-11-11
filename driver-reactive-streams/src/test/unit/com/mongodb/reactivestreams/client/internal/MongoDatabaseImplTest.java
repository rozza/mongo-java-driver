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

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.internal.async.client.OperationExecutor;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListCollectionsPublisher;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import reactor.core.publisher.Mono;

import java.util.List;

import static com.mongodb.reactivestreams.client.internal.PublisherCreator.createAggregatePublisher;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.createChangeStreamPublisher;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.createCreateCollectionMono;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.createDropDatabaseMono;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.createListCollectionsPublisher;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.createRunCommandMono;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.createViewMono;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;


public class MongoDatabaseImplTest extends TestHelper {
    @Mock
    private ClientSession clientSession;

    @Test
    void testAggregate() {
        MongoDatabaseImpl mongoDatabase = createMongoDatabase();
        List<Bson> pipeline = singletonList(BsonDocument.parse("{$match: {open: true}}"));

        assertAll("Aggregate tests",
                  () -> assertAll("check validation",
                                () -> assertThrows(IllegalArgumentException.class, () -> mongoDatabase.aggregate(null)),
                                () -> assertThrows(IllegalArgumentException.class, () -> mongoDatabase.aggregate(clientSession, null))
                  ),
                  () -> {
                      AggregatePublisher<Document> expected =
                              createAggregatePublisher(null, mongoDatabase.getName(), Document.class, Document.class,
                                                       mongoDatabase.getCodecRegistry(), mongoDatabase.getReadPreference(),
                                                       mongoDatabase.getReadConcern(), mongoDatabase.getWriteConcern(),
                                                       mongoDatabase.getExecutor(), pipeline,
                                                       AggregationLevel.DATABASE, mongoDatabase.getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoDatabase.aggregate(pipeline), "Default");
                  },
                  () -> {
                      AggregatePublisher<BsonDocument> expected =
                              createAggregatePublisher(null, mongoDatabase.getName(), Document.class, BsonDocument.class,
                                                       mongoDatabase.getCodecRegistry(), mongoDatabase.getReadPreference(),
                                                       mongoDatabase.getReadConcern(), mongoDatabase.getWriteConcern(),
                                                       mongoDatabase.getExecutor(), pipeline,
                                                       AggregationLevel.DATABASE, mongoDatabase.getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoDatabase.aggregate(pipeline, BsonDocument.class),
                                                 "With result class");
                  },
                  () -> {
                      AggregatePublisher<Document> expected =
                              createAggregatePublisher(clientSession, mongoDatabase.getName(), Document.class, Document.class,
                                                       mongoDatabase.getCodecRegistry(), mongoDatabase.getReadPreference(),
                                                       mongoDatabase.getReadConcern(), mongoDatabase.getWriteConcern(),
                                                       mongoDatabase.getExecutor(), pipeline,
                                                       AggregationLevel.DATABASE, mongoDatabase.getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoDatabase.aggregate(clientSession, pipeline), "With session");
                  },
                  () -> {
                      AggregatePublisher<BsonDocument> expected =
                              createAggregatePublisher(clientSession, mongoDatabase.getName(), Document.class, BsonDocument.class,
                                                       mongoDatabase.getCodecRegistry(), mongoDatabase.getReadPreference(),
                                                       mongoDatabase.getReadConcern(), mongoDatabase.getWriteConcern(),
                                                       mongoDatabase.getExecutor(), pipeline,
                                                       AggregationLevel.DATABASE, mongoDatabase.getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoDatabase.aggregate(clientSession, pipeline, BsonDocument.class),
                                                 "With session & result class");
                  }
        );
    }

    @Test
    void shouldListCollections() {
        MongoDatabaseImpl mongoDatabase = createMongoDatabase();
        mongoDatabase.listCollections();

        assertAll("listCollections tests",
                  () -> assertAll("check validation",
                              () -> assertThrows(IllegalArgumentException.class, () -> mongoDatabase.listCollections((Class<?>) null)),
                              () -> assertThrows(IllegalArgumentException.class, () -> mongoDatabase.listCollections((ClientSession) null)),
                              () -> assertThrows(IllegalArgumentException.class, () -> mongoDatabase.listCollections(clientSession, null))
                  ),
                  () -> {
                      ListCollectionsPublisher<Document> expected =
                              createListCollectionsPublisher(null, mongoDatabase.getName(), Document.class,
                                                             mongoDatabase.getCodecRegistry(),
                                                             mongoDatabase.getReadPreference(), mongoDatabase.getExecutor(),
                                                             mongoDatabase.getRetryReads(), false);
                      assertPublisherIsTheSameAs(expected, mongoDatabase.listCollections(), "Default");
                  },
                  () -> {
                      ListCollectionsPublisher<BsonDocument> expected =
                              createListCollectionsPublisher(null, mongoDatabase.getName(), BsonDocument.class,
                                                             mongoDatabase.getCodecRegistry(),
                                                             mongoDatabase.getReadPreference(), mongoDatabase.getExecutor(),
                                                             mongoDatabase.getRetryReads(), false);
                      assertPublisherIsTheSameAs(expected, mongoDatabase.listCollections(BsonDocument.class), "With result class");
                  },
                  () -> {
                      ListCollectionsPublisher<Document> expected =
                              createListCollectionsPublisher(clientSession, mongoDatabase.getName(), Document.class,
                                                             mongoDatabase.getCodecRegistry(),
                                                             mongoDatabase.getReadPreference(), mongoDatabase.getExecutor(),
                                                             mongoDatabase.getRetryReads(), false);
                      assertPublisherIsTheSameAs(expected, mongoDatabase.listCollections(clientSession), "With client session");
                  },
                  () -> {
                      ListCollectionsPublisher<BsonDocument> expected =
                              createListCollectionsPublisher(clientSession, mongoDatabase.getName(), BsonDocument.class,
                                                             mongoDatabase.getCodecRegistry(),
                                                             mongoDatabase.getReadPreference(), mongoDatabase.getExecutor(),
                                                             mongoDatabase.getRetryReads(), false);
                      assertPublisherIsTheSameAs(expected, mongoDatabase.listCollections(clientSession, BsonDocument.class),
                                                 "With client session & result class");
                  }
        );
    }

    @Test
    void testListCollectionNames() {
        MongoDatabaseImpl mongoDatabase = createMongoDatabase();
        mongoDatabase.listCollectionNames();

        assertAll("listCollectionNames",
                  () -> assertAll("check validation",
                              () -> assertThrows(IllegalArgumentException.class, () -> mongoDatabase.listCollectionNames(null))
                  ),
                  () -> {
                      ListCollectionsPublisher<Document> expected =
                              createListCollectionsPublisher(null, mongoDatabase.getName(), Document.class,
                                                             mongoDatabase.getCodecRegistry(),
                                                             mongoDatabase.getReadPreference(), mongoDatabase.getExecutor(),
                                                             mongoDatabase.getRetryReads(), false);
                      assertPublisherIsTheSameAs(expected, mongoDatabase.listCollectionNames(), "Default");
                  },
                  () -> {
                      ListCollectionsPublisher<Document> expected =
                              createListCollectionsPublisher(clientSession, mongoDatabase.getName(), Document.class,
                                                             mongoDatabase.getCodecRegistry(),
                                                             mongoDatabase.getReadPreference(), mongoDatabase.getExecutor(),
                                                             mongoDatabase.getRetryReads(), false);
                      assertPublisherIsTheSameAs(expected, mongoDatabase.listCollectionNames(clientSession), "With client session");
                  }
        );
    }

    @Test
    void testCreateCollection() {
        MongoDatabaseImpl mongoDatabase = createMongoDatabase();
        String collectionName = "coll";
        assertAll("createCollection",
                  () -> assertAll("check validation",
                              () -> assertThrows(IllegalArgumentException.class, () -> mongoDatabase.createCollection(null)),
                              () -> assertThrows(IllegalArgumentException.class,
                                               () -> mongoDatabase.createCollection(collectionName, null)),
                              () -> assertThrows(IllegalArgumentException.class,
                                               () -> mongoDatabase.createCollection(null, collectionName)),
                              () -> assertThrows(IllegalArgumentException.class,
                                               () -> mongoDatabase.createCollection(clientSession, collectionName, null))
                  ),
                  () -> {
                      Mono<Void> expected =
                              createCreateCollectionMono(null, new MongoNamespace(mongoDatabase.getName(), collectionName),
                                                         mongoDatabase.getCodecRegistry(), mongoDatabase.getReadConcern(),
                                                         mongoDatabase.getWriteConcern(), mongoDatabase.getExecutor(),
                                                         new CreateCollectionOptions());
                      assertPublisherIsTheSameAs(expected, mongoDatabase.createCollection(collectionName), "Default");
                  },
                  () -> {
                        CreateCollectionOptions options = new CreateCollectionOptions().sizeInBytes(500).capped(true);
                        Mono<Void> expected =
                              createCreateCollectionMono(null, new MongoNamespace(mongoDatabase.getName(), collectionName),
                                                         mongoDatabase.getCodecRegistry(), mongoDatabase.getReadConcern(),
                                                         mongoDatabase.getWriteConcern(), mongoDatabase.getExecutor(),
                                                         options);
                      assertPublisherIsTheSameAs(expected, mongoDatabase.createCollection(collectionName, options), "Default");
                  },
                  () -> {
                      Mono<Void> expected =
                              createCreateCollectionMono(clientSession, new MongoNamespace(mongoDatabase.getName(), collectionName),
                                                         mongoDatabase.getCodecRegistry(), mongoDatabase.getReadConcern(),
                                                         mongoDatabase.getWriteConcern(), mongoDatabase.getExecutor(),
                                                         new CreateCollectionOptions());
                      assertPublisherIsTheSameAs(expected, mongoDatabase.createCollection(clientSession, collectionName),
                                                 "With client session");
                  },
                  () -> {
                      CreateCollectionOptions options = new CreateCollectionOptions().sizeInBytes(500).capped(true);
                      Mono<Void> expected =
                              createCreateCollectionMono(clientSession, new MongoNamespace(mongoDatabase.getName(), collectionName),
                                                         mongoDatabase.getCodecRegistry(), mongoDatabase.getReadConcern(),
                                                         mongoDatabase.getWriteConcern(), mongoDatabase.getExecutor(),
                                                         options);
                      assertPublisherIsTheSameAs(expected, mongoDatabase.createCollection(clientSession, collectionName, options),
                                                 "With client session & options");
                  }
        );
    }

    @Test
    void testCreateView() {
        MongoDatabaseImpl mongoDatabase = createMongoDatabase();
        String viewName = "viewName";
        String viewOn = "viewOn";
        List<Bson> pipeline = singletonList(BsonDocument.parse("{$match: {open: true}}"));
        CreateViewOptions options = new CreateViewOptions().collation(Collation.builder().locale("de").build());

        assertAll("createView",
                  () -> assertAll("check validation",
                              () -> assertThrows(IllegalArgumentException.class,
                                               () -> mongoDatabase.createView(null, viewOn, pipeline)),
                              () -> assertThrows(IllegalArgumentException.class,
                                               () -> mongoDatabase.createView(viewName, null, pipeline)),
                              () -> assertThrows(IllegalArgumentException.class,
                                               () -> mongoDatabase.createView(viewName, viewOn, null)),
                              () -> assertThrows(IllegalArgumentException.class,
                                               () -> mongoDatabase.createView(viewName, viewOn, pipeline, null)),
                              () -> assertThrows(IllegalArgumentException.class,
                                               () -> mongoDatabase.createView(null, viewName, viewOn, pipeline)),
                              () -> assertThrows(IllegalArgumentException.class,
                                               () -> mongoDatabase.createView(null, viewName, viewOn, pipeline, options))

                  ),
                  () -> {
                      Mono<Void> expected = createViewMono(null, new MongoNamespace(mongoDatabase.getName(), viewOn),
                                                           mongoDatabase.getCodecRegistry(), mongoDatabase.getReadConcern(),
                                                           mongoDatabase.getWriteConcern(), mongoDatabase.getExecutor(),
                                                           viewName, pipeline, new CreateViewOptions());
                      assertPublisherIsTheSameAs(expected, mongoDatabase.createView(viewName, viewOn, pipeline), "Default");
                  },
                  () -> {
                      Mono<Void> expected = createViewMono(null, new MongoNamespace(mongoDatabase.getName(), viewOn),
                                                           mongoDatabase.getCodecRegistry(), mongoDatabase.getReadConcern(),
                                                           mongoDatabase.getWriteConcern(), mongoDatabase.getExecutor(),
                                                           viewName, pipeline, options);
                      assertPublisherIsTheSameAs(expected, mongoDatabase.createView(viewName, viewOn, pipeline, options),
                                                 "With options");
                  },
                  () -> {
                      Mono<Void> expected = createViewMono(clientSession, new MongoNamespace(mongoDatabase.getName(), viewOn),
                                                           mongoDatabase.getCodecRegistry(), mongoDatabase.getReadConcern(),
                                                           mongoDatabase.getWriteConcern(), mongoDatabase.getExecutor(),
                                                           viewName, pipeline, new CreateViewOptions());
                      assertPublisherIsTheSameAs(expected, mongoDatabase.createView(clientSession, viewName, viewOn, pipeline),
                                                 "With client session");
                  },
                  () -> {
                      Mono<Void> expected = createViewMono(clientSession, new MongoNamespace(mongoDatabase.getName(), viewOn),
                                                           mongoDatabase.getCodecRegistry(), mongoDatabase.getReadConcern(),
                                                           mongoDatabase.getWriteConcern(), mongoDatabase.getExecutor(),
                                                           viewName, pipeline, options);
                      assertPublisherIsTheSameAs(expected, mongoDatabase.createView(clientSession, viewName, viewOn, pipeline, options),
                                                 "With client session & options");
                  }
        );
    }

    @Test
    void testDrop() {
        MongoDatabaseImpl mongoDatabase = createMongoDatabase();
        mongoDatabase.drop();

        assertAll("drop",
                  () -> assertAll("check validation",
                              () -> assertThrows(IllegalArgumentException.class, () -> mongoDatabase.drop(null))
                  ),
                  () -> {
                      Mono<Void> expected = createDropDatabaseMono(null, mongoDatabase.getName(), mongoDatabase.getReadConcern(),
                                                                   mongoDatabase.getWriteConcern(), mongoDatabase.getExecutor());
                      assertPublisherIsTheSameAs(expected, mongoDatabase.drop(), "Default");
                  },
                  () -> {
                      Mono<Void> expected = createDropDatabaseMono(clientSession, mongoDatabase.getName(), mongoDatabase.getReadConcern(),
                                                                   mongoDatabase.getWriteConcern(), mongoDatabase.getExecutor());
                      assertPublisherIsTheSameAs(expected, mongoDatabase.drop(clientSession), "With client session");
                  }
        );
    }

    @Test
    void testRunCommand() {
        MongoDatabaseImpl mongoDatabase = createMongoDatabase();
        Bson command = BsonDocument.parse("{ping : 1}");

        assertAll("runCommand",
                  () -> assertAll("check validation",
                              () -> assertThrows(IllegalArgumentException.class,
                                               () -> mongoDatabase.runCommand(null)),
                              () -> assertThrows(IllegalArgumentException.class,
                                               () -> mongoDatabase.runCommand(command, (ReadPreference) null)),
                              () -> assertThrows(IllegalArgumentException.class,
                                               () -> mongoDatabase.runCommand(command, (Class<?>) null)),
                              () -> assertThrows(IllegalArgumentException.class,
                                               () -> mongoDatabase.runCommand(command, ReadPreference.nearest(), null)),
                              () -> assertThrows(IllegalArgumentException.class,
                                               () -> mongoDatabase.runCommand(null, command)),
                              () -> assertThrows(IllegalArgumentException.class,
                                               () -> mongoDatabase.runCommand(null, command, ReadPreference.nearest())),
                              () -> assertThrows(IllegalArgumentException.class,
                                               () -> mongoDatabase.runCommand(null, command, Document.class)),
                              () -> assertThrows(IllegalArgumentException.class,
                                               () -> mongoDatabase.runCommand(null, command, ReadPreference.nearest(), Document.class))
                  ),
                  () -> {
                      Mono<Document> expected = createRunCommandMono(null, mongoDatabase.getName(), Document.class,
                                                                     mongoDatabase.getCodecRegistry(), ReadPreference.primary(),
                                                                     mongoDatabase.getReadConcern(), mongoDatabase.getExecutor(), command);

                      assertPublisherIsTheSameAs(expected, mongoDatabase.runCommand(command), "Default");
                  },
                  () -> {
                      Mono<BsonDocument> expected = createRunCommandMono(null, mongoDatabase.getName(), BsonDocument.class,
                                                                     mongoDatabase.getCodecRegistry(), ReadPreference.primary(),
                                                                     mongoDatabase.getReadConcern(), mongoDatabase.getExecutor(), command);

                      assertPublisherIsTheSameAs(expected, mongoDatabase.runCommand(command, BsonDocument.class),
                                                 "With result class");
                  },
                  () -> {
                      Mono<Document> expected = createRunCommandMono(null, mongoDatabase.getName(), Document.class,
                                                                     mongoDatabase.getCodecRegistry(), ReadPreference.nearest(),
                                                                     mongoDatabase.getReadConcern(), mongoDatabase.getExecutor(), command);

                      assertPublisherIsTheSameAs(expected, mongoDatabase.runCommand(command, ReadPreference.nearest()),
                                                 "With read preference");
                  },
                  () -> {
                      Mono<BsonDocument> expected = createRunCommandMono(null, mongoDatabase.getName(), BsonDocument.class,
                                                                     mongoDatabase.getCodecRegistry(), ReadPreference.nearest(),
                                                                     mongoDatabase.getReadConcern(), mongoDatabase.getExecutor(), command);

                      assertPublisherIsTheSameAs(expected, mongoDatabase.runCommand(command, ReadPreference.nearest(), BsonDocument.class),
                                                 "With read preference & result class");
                  },
                  () -> {
                      Mono<Document> expected = createRunCommandMono(clientSession, mongoDatabase.getName(), Document.class,
                                                                     mongoDatabase.getCodecRegistry(), ReadPreference.primary(),
                                                                     mongoDatabase.getReadConcern(), mongoDatabase.getExecutor(), command);

                      assertPublisherIsTheSameAs(expected, mongoDatabase.runCommand(clientSession, command),
                                                 "With client session");
                  },
                  () -> {
                      Mono<BsonDocument> expected = createRunCommandMono(clientSession, mongoDatabase.getName(), BsonDocument.class,
                                                                         mongoDatabase.getCodecRegistry(),
                                                                         ReadPreference.primary(), mongoDatabase.getReadConcern(),
                                                                         mongoDatabase.getExecutor(), command);

                      assertPublisherIsTheSameAs(expected, mongoDatabase.runCommand(clientSession, command, BsonDocument.class),
                                                 "With client session & result class");
                  },
                  () -> {
                      Mono<Document> expected = createRunCommandMono(clientSession, mongoDatabase.getName(), Document.class,
                                                                     mongoDatabase.getCodecRegistry(), ReadPreference.nearest(),
                                                                     mongoDatabase.getReadConcern(), mongoDatabase.getExecutor(), command);

                      assertPublisherIsTheSameAs(expected, mongoDatabase.runCommand(clientSession, command, ReadPreference.nearest()),
                                                 "With client session & read preference");
                  },
                  () -> {
                      Mono<BsonDocument> expected = createRunCommandMono(clientSession, mongoDatabase.getName(), BsonDocument.class,
                                                                         mongoDatabase.getCodecRegistry(), ReadPreference.nearest(),
                                                                         mongoDatabase.getReadConcern(), mongoDatabase.getExecutor(),
                                                                         command);

                      assertPublisherIsTheSameAs(expected, mongoDatabase.runCommand(clientSession, command, ReadPreference.nearest(),
                                                                                    BsonDocument.class),
                                                 "With client session, read preference & result class");
                  }
        );
    }

    @Test
    void testWatch() {
        MongoDatabaseImpl mongoDatabase = createMongoDatabase();
        List<Bson> pipeline = singletonList(BsonDocument.parse("{$match: {open: true}}"));
        assertAll("watch",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoDatabase.watch((Class<?>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoDatabase.watch((List<Bson>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoDatabase.watch(pipeline, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoDatabase.watch((ClientSession) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> mongoDatabase.watch(null, pipeline)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> mongoDatabase.watch(null, pipeline, Document.class))
                  ),
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              createChangeStreamPublisher(null, mongoDatabase.getName(), Document.class,
                                                          mongoDatabase.getCodecRegistry(),
                                                          mongoDatabase.getReadPreference(),
                                                          mongoDatabase.getReadConcern(), mongoDatabase.getExecutor(),
                                                          emptyList(), ChangeStreamLevel.DATABASE, mongoDatabase.getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoDatabase.watch(), "Default");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              createChangeStreamPublisher(null, mongoDatabase.getName(), Document.class,
                                                          mongoDatabase.getCodecRegistry(),
                                                          mongoDatabase.getReadPreference(),
                                                          mongoDatabase.getReadConcern(), mongoDatabase.getExecutor(), pipeline,
                                                          ChangeStreamLevel.DATABASE, mongoDatabase.getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoDatabase.watch(pipeline), "With pipeline");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              createChangeStreamPublisher(null, mongoDatabase.getName(), BsonDocument.class,
                                                          mongoDatabase.getCodecRegistry(),
                                                          mongoDatabase.getReadPreference(),
                                                          mongoDatabase.getReadConcern(), mongoDatabase.getExecutor(),
                                                          emptyList(), ChangeStreamLevel.DATABASE, mongoDatabase.getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoDatabase.watch(BsonDocument.class),
                                                 "With result class");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              createChangeStreamPublisher(null, mongoDatabase.getName(), BsonDocument.class,
                                                          mongoDatabase.getCodecRegistry(),
                                                          mongoDatabase.getReadPreference(),
                                                          mongoDatabase.getReadConcern(), mongoDatabase.getExecutor(), pipeline,
                                                          ChangeStreamLevel.DATABASE, mongoDatabase.getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoDatabase.watch(pipeline, BsonDocument.class),
                                                 "With pipeline & result class");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              createChangeStreamPublisher(clientSession, mongoDatabase.getName(), Document.class,
                                                          mongoDatabase.getCodecRegistry(),
                                                          mongoDatabase.getReadPreference(),
                                                          mongoDatabase.getReadConcern(), mongoDatabase.getExecutor(),
                                                          emptyList(), ChangeStreamLevel.DATABASE, mongoDatabase.getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoDatabase.watch(clientSession), "with session");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              createChangeStreamPublisher(clientSession, mongoDatabase.getName(), Document.class,
                                                          mongoDatabase.getCodecRegistry(),
                                                          mongoDatabase.getReadPreference(),
                                                          mongoDatabase.getReadConcern(), mongoDatabase.getExecutor(), pipeline,
                                                          ChangeStreamLevel.DATABASE, mongoDatabase.getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoDatabase.watch(clientSession, pipeline), "With session & pipeline");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              createChangeStreamPublisher(clientSession, mongoDatabase.getName(), BsonDocument.class,
                                                          mongoDatabase.getCodecRegistry(),
                                                          mongoDatabase.getReadPreference(),
                                                          mongoDatabase.getReadConcern(), mongoDatabase.getExecutor(),
                                                          emptyList(), ChangeStreamLevel.DATABASE, mongoDatabase.getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoDatabase.watch(clientSession, BsonDocument.class),
                                                 "With session & resultClass");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              createChangeStreamPublisher(clientSession, mongoDatabase.getName(), BsonDocument.class,
                                                          mongoDatabase.getCodecRegistry(),
                                                          mongoDatabase.getReadPreference(),
                                                          mongoDatabase.getReadConcern(), mongoDatabase.getExecutor(), pipeline,
                                                          ChangeStreamLevel.DATABASE, mongoDatabase.getRetryReads());

                      assertPublisherIsTheSameAs(expected, mongoDatabase.watch(clientSession, pipeline, BsonDocument.class),
                                                 "With clientSession, pipeline & result class");
                  }
        );
    }

    MongoDatabaseImpl createMongoDatabase() {
        return new MongoDatabaseImpl("db", MongoClientSettings.getDefaultCodecRegistry(), ReadPreference.primary(),
                                     ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED, mock(OperationExecutor.class),
                                     true, true, UuidRepresentation.STANDARD);
    }
}
