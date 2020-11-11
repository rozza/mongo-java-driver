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

import com.mongodb.CreateIndexCommitQuorum;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.internal.async.client.OperationExecutor;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.client.model.CountStrategy;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.DistinctPublisher;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.ListIndexesPublisher;
import com.mongodb.reactivestreams.client.MapReducePublisher;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.client.model.CountOptionsHelper.fromEstimatedDocumentCountOptions;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.createAggregatePublisher;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.createChangeStreamPublisher;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.createDistinctPublisher;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.createFindPublisher;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.createIndexesFlux;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.createListIndexesPublisher;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.createMapReducePublisher;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.createReadOperationMono;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.createSingleWriteRequestMono;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.createWriteOperationMono;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;


public class MongoCollectionImplTest extends TestHelper {
    @Mock
    private ClientSession clientSession;

    private final Bson filter = BsonDocument.parse("{$match: {open: true}}");
    private final List<Bson> pipeline = singletonList(filter);
    private final Collation collation = Collation.builder().locale("de").build();

    @Test
    void testAggregate() {
        MongoCollectionImpl<Document> collection = createMongoCollection();

        assertAll("Aggregate tests",
                  () -> assertAll("check validation",
                                () -> assertThrows(IllegalArgumentException.class, () -> collection.aggregate(null)),
                                () -> assertThrows(IllegalArgumentException.class, () -> collection.aggregate(clientSession, null))
                  ),
                  () -> {
                      AggregatePublisher<Document> expected =
                              createAggregatePublisher(null, collection.getNamespace(), Document.class, Document.class,
                                                       collection.getCodecRegistry(), collection.getReadPreference(),
                                                       collection.getReadConcern(), collection.getWriteConcern(),
                                                       collection.getExecutor(), pipeline,
                                                       AggregationLevel.COLLECTION, collection.getRetryReads());

                      assertPublisherIsTheSameAs(expected, collection.aggregate(pipeline), "Default");
                  },
                  () -> {
                      AggregatePublisher<BsonDocument> expected =
                              createAggregatePublisher(null, collection.getNamespace(), Document.class, BsonDocument.class,
                                                       collection.getCodecRegistry(), collection.getReadPreference(),
                                                       collection.getReadConcern(), collection.getWriteConcern(),
                                                       collection.getExecutor(), pipeline,
                                                       AggregationLevel.COLLECTION, collection.getRetryReads());

                      assertPublisherIsTheSameAs(expected, collection.aggregate(pipeline, BsonDocument.class),
                                                 "With result class");
                  },
                  () -> {
                      AggregatePublisher<Document> expected =
                              createAggregatePublisher(clientSession, collection.getNamespace(), Document.class, Document.class,
                                                       collection.getCodecRegistry(), collection.getReadPreference(),
                                                       collection.getReadConcern(), collection.getWriteConcern(),
                                                       collection.getExecutor(), pipeline,
                                                       AggregationLevel.COLLECTION, collection.getRetryReads());

                      assertPublisherIsTheSameAs(expected, collection.aggregate(clientSession, pipeline), "With session");
                  },
                  () -> {
                      AggregatePublisher<BsonDocument> expected =
                              createAggregatePublisher(clientSession, collection.getNamespace(), Document.class, BsonDocument.class,
                                                       collection.getCodecRegistry(), collection.getReadPreference(),
                                                       collection.getReadConcern(), collection.getWriteConcern(),
                                                       collection.getExecutor(), pipeline,
                                                       AggregationLevel.COLLECTION, collection.getRetryReads());

                      assertPublisherIsTheSameAs(expected, collection.aggregate(clientSession, pipeline, BsonDocument.class),
                                                 "With session & result class");
                  }
        );
    }

    @Test
    public void testBulkWrite() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        List<WriteModel<Document>> requests = singletonList(new InsertOneModel<>(new Document()));
        BulkWriteOptions options = new BulkWriteOptions().ordered(false);

        assertAll("bulkWrite",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.bulkWrite(null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.bulkWrite(requests, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.bulkWrite(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.bulkWrite(clientSession, requests, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.bulkWrite(null, requests)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.bulkWrite(null, requests, options))
                  ),
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createWriteOperationMono(() -> collection.getOperations().bulkWrite(requests, new BulkWriteOptions()),
                                                       null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.bulkWrite(requests), "Default");
                  },
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createWriteOperationMono(() -> collection.getOperations().bulkWrite(requests, options),
                                                       null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.bulkWrite(requests, options), "With options");
                  },
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createWriteOperationMono(() -> collection.getOperations().bulkWrite(requests, new BulkWriteOptions()),
                                                       clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.bulkWrite(clientSession, requests), "With client session");
                  },
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createWriteOperationMono(() -> collection.getOperations().bulkWrite(requests, options),
                                                       clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.bulkWrite(clientSession, requests, options),
                                                 "With client session & options");
                  }
        );
    }

    @Test
    public void testCountDocuments() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        CountOptions options = new CountOptions().collation(Collation.builder().locale("de").build());

        assertAll("countDocuments",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.countDocuments((Bson) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.countDocuments(filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.countDocuments((ClientSession) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.countDocuments(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.countDocuments(clientSession, filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.countDocuments(null, filter)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.countDocuments(null, filter, options))
                  ),
                  () -> {
                      Mono<Long> expected = createReadOperationMono(
                              () -> collection.getOperations().count(new BsonDocument(), new CountOptions(), CountStrategy.AGGREGATE),
                              null, collection.getReadPreference(), collection.getReadConcern(), collection.getExecutor());

                      assertPublisherIsTheSameAs(expected, collection.countDocuments(), "Default");
                  },
                  () -> {
                      Mono<Long> expected = createReadOperationMono(
                              () -> collection.getOperations().count(filter, new CountOptions(), CountStrategy.AGGREGATE),
                              null, collection.getReadPreference(), collection.getReadConcern(), collection.getExecutor());

                      assertPublisherIsTheSameAs(expected, collection.countDocuments(filter), "With filter");
                  },
                  () -> {
                      Mono<Long> expected = createReadOperationMono(
                              () -> collection.getOperations().count(filter, options, CountStrategy.AGGREGATE),
                              null, collection.getReadPreference(), collection.getReadConcern(), collection.getExecutor());

                      assertPublisherIsTheSameAs(expected, collection.countDocuments(filter, options), "With filter & options");
                  },
                  () -> {
                      Mono<Long> expected = createReadOperationMono(
                              () -> collection.getOperations().count(new BsonDocument(), new CountOptions(), CountStrategy.AGGREGATE),
                              clientSession, collection.getReadPreference(), collection.getReadConcern(), collection.getExecutor());

                      assertPublisherIsTheSameAs(expected, collection.countDocuments(clientSession), "With client session");
                  },
                  () -> {
                      Mono<Long> expected = createReadOperationMono(
                              () -> collection.getOperations().count(filter, new CountOptions(), CountStrategy.AGGREGATE),
                              clientSession, collection.getReadPreference(), collection.getReadConcern(), collection.getExecutor());

                      assertPublisherIsTheSameAs(expected, collection.countDocuments(clientSession, filter),
                                                 "With client session & filter");
                  },
                  () -> {
                      Mono<Long> expected = createReadOperationMono(
                              () -> collection.getOperations().count(filter, options, CountStrategy.AGGREGATE),
                              clientSession, collection.getReadPreference(), collection.getReadConcern(), collection.getExecutor());

                      assertPublisherIsTheSameAs(expected, collection.countDocuments(clientSession, filter, options),
                                                 "With client session, filter & options");
                  }
        );
    }

    @Test
    public void testCreateIndex() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        Bson key = BsonDocument.parse("{key: 1}");
        CreateIndexOptions createIndexOptions = new CreateIndexOptions();
        IndexOptions indexOptions = new IndexOptions();
        IndexOptions customOptions = new IndexOptions().background(true).bits(9);
        List<IndexModel> indexes = singletonList(new IndexModel(key, new IndexOptions()));
        List<IndexModel> customIndexes = singletonList(new IndexModel(key, customOptions));

        assertAll("createIndex",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndex(null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndex(key, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndex(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndex(clientSession, key, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndex(null, key)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndex(null, key, indexOptions))
                  ),
                  () -> {

                      Flux<String> expected = createIndexesFlux(() -> collection.getOperations().createIndexes(indexes, createIndexOptions),
                                                                   null, collection.getReadConcern(), collection.getExecutor(),
                                                                indexes, collection.getCodecRegistry());

                      assertPublisherIsTheSameAs(expected, collection.createIndex(key), "Default");
                  },
                  () -> {

                      Flux<String> expected = createIndexesFlux(() -> collection.getOperations()
                                                                        .createIndexes(customIndexes, createIndexOptions),
                                                                null, collection.getReadConcern(), collection.getExecutor(), indexes,
                                                                collection.getCodecRegistry());

                      assertPublisherIsTheSameAs(expected, collection.createIndex(key, customOptions), "With custom options");
                  },
                  () -> {

                      Flux<String> expected = createIndexesFlux(() -> collection.getOperations().createIndexes(indexes, createIndexOptions),
                                                                clientSession, collection.getReadConcern(), collection.getExecutor(),
                                                                indexes, collection.getCodecRegistry());

                      assertPublisherIsTheSameAs(expected, collection.createIndex(clientSession, key), "With client session");
                  },
                  () -> {

                      Flux<String> expected = createIndexesFlux(
                              () -> collection.getOperations().createIndexes(customIndexes, createIndexOptions),
                              clientSession, collection.getReadConcern(), collection.getExecutor(), indexes, collection.getCodecRegistry());

                      assertPublisherIsTheSameAs(expected, collection.createIndex(clientSession, key, customOptions),
                                                 "With client session, & custom options");
                  }
        );
    }

    @Test
    public void testCreateIndexes() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        Bson key = BsonDocument.parse("{key: 1}");
        CreateIndexOptions createIndexOptions = new CreateIndexOptions();
        CreateIndexOptions customCreateIndexOptions = new CreateIndexOptions().commitQuorum(CreateIndexCommitQuorum.VOTING_MEMBERS);
        List<IndexModel> indexes = singletonList(new IndexModel(key, new IndexOptions().background(true).bits(9)));
        assertAll("createIndexes",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndexes(null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndexes(indexes, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndexes(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndexes(clientSession, indexes, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndexes(null, indexes)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.createIndexes(null, indexes, createIndexOptions))
                  ),
                  () -> {
                      Flux<String> expected = createIndexesFlux(() -> collection.getOperations().createIndexes(indexes, createIndexOptions),
                                                                null, collection.getReadConcern(), collection.getExecutor(),
                                                                indexes, collection.getCodecRegistry());

                      assertPublisherIsTheSameAs(expected, collection.createIndexes(indexes), "Default");
                  },
                  () -> {
                      Flux<String> expected = createIndexesFlux(() -> collection.getOperations()
                                                                        .createIndexes(indexes, customCreateIndexOptions),
                                                                null, collection.getReadConcern(), collection.getExecutor(), indexes,
                                                                collection.getCodecRegistry());

                      assertPublisherIsTheSameAs(expected, collection.createIndexes(indexes, customCreateIndexOptions),
                                                 "With custom options");
                  },
                  () -> {
                      Flux<String> expected = createIndexesFlux(() -> collection.getOperations().createIndexes(indexes, createIndexOptions),
                                                                clientSession, collection.getReadConcern(), collection.getExecutor(),
                                                                indexes, collection.getCodecRegistry());

                      assertPublisherIsTheSameAs(expected, collection.createIndexes(clientSession, indexes), "With client session");
                  },
                  () -> {
                      Flux<String> expected = createIndexesFlux(
                              () -> collection.getOperations().createIndexes(indexes, customCreateIndexOptions),
                              clientSession, collection.getReadConcern(), collection.getExecutor(), indexes,
                              collection.getCodecRegistry());

                      assertPublisherIsTheSameAs(expected, collection.createIndexes(clientSession, indexes, customCreateIndexOptions),
                                                 "With client session, & custom options");
                  }
        );
    }

    @Test
    public void testDeleteOne() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        DeleteOptions customOptions = new DeleteOptions().collation(collation);
        assertAll("deleteOne",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.deleteOne(null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.deleteOne(filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.deleteOne(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.deleteOne(clientSession, filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.deleteOne(null, filter)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.deleteOne(clientSession, filter, null))
                  ),
                  () -> {
                      final Mono<BulkWriteResult> expected =
                              createSingleWriteRequestMono(() -> collection.getOperations().deleteOne(filter, new DeleteOptions()),
                                                           null, collection.getReadConcern(), collection.getExecutor(),
                                                           WriteRequest.Type.DELETE);

                      assertPublisherIsTheSameAs(expected, collection.deleteOne(filter), "Default");
                  },
                  () -> {
                      final Mono<BulkWriteResult> expected =
                              createSingleWriteRequestMono(() -> collection.getOperations().deleteOne(filter, customOptions),
                                                           null, collection.getReadConcern(), collection.getExecutor(),
                                                           WriteRequest.Type.DELETE);

                      assertPublisherIsTheSameAs(expected, collection.deleteOne(filter, customOptions), "With options");
                  },
                  () -> {
                      final Mono<BulkWriteResult> expected =
                              createSingleWriteRequestMono(() -> collection.getOperations().deleteOne(filter, new DeleteOptions()),
                                                           clientSession, collection.getReadConcern(), collection.getExecutor(),
                                                           WriteRequest.Type.DELETE);

                      assertPublisherIsTheSameAs(expected, collection.deleteOne(clientSession, filter), "With client session");
                  },
                  () -> {
                      final Mono<BulkWriteResult> expected =
                              createSingleWriteRequestMono(() -> collection.getOperations().deleteOne(filter, customOptions),
                                                           clientSession, collection.getReadConcern(), collection.getExecutor(),
                                                           WriteRequest.Type.DELETE);

                      assertPublisherIsTheSameAs(expected, collection.deleteOne(clientSession, filter, customOptions),
                                                 "With client session & options");
                  }
        );
    }

    @Test
    public void testDeleteMany() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        DeleteOptions customOptions = new DeleteOptions().collation(collation);
        assertAll("deleteMany",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.deleteMany(null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.deleteMany(filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.deleteMany(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.deleteMany(clientSession, filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.deleteMany(null, filter)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.deleteMany(clientSession, filter, null))
                  ),

                  () -> {
                      final Mono<BulkWriteResult> expected =
                              createSingleWriteRequestMono(() -> collection.getOperations().deleteMany(filter, new DeleteOptions()),
                                                           null, collection.getReadConcern(), collection.getExecutor(),
                                                           WriteRequest.Type.DELETE);

                      assertPublisherIsTheSameAs(expected, collection.deleteMany(filter), "Default");
                  },
                  () -> {
                      final Mono<BulkWriteResult> expected =
                              createSingleWriteRequestMono(() -> collection.getOperations().deleteMany(filter, customOptions),
                                                           null, collection.getReadConcern(), collection.getExecutor(),
                                                           WriteRequest.Type.DELETE);

                      assertPublisherIsTheSameAs(expected, collection.deleteMany(filter, customOptions), "With options");
                  },
                  () -> {
                      final Mono<BulkWriteResult> expected =
                              createSingleWriteRequestMono(() -> collection.getOperations().deleteMany(filter, new DeleteOptions()),
                                                           clientSession, collection.getReadConcern(), collection.getExecutor(),
                                                           WriteRequest.Type.DELETE);

                      assertPublisherIsTheSameAs(expected, collection.deleteMany(clientSession, filter), "With client session");
                  },
                  () -> {
                      final Mono<BulkWriteResult> expected =
                              createSingleWriteRequestMono(() -> collection.getOperations().deleteMany(filter, customOptions),
                                                           clientSession, collection.getReadConcern(), collection.getExecutor(),
                                                           WriteRequest.Type.DELETE);

                      assertPublisherIsTheSameAs(expected, collection.deleteMany(clientSession, filter, customOptions),
                                                 "With client session & options");
                  }
        );
    }

    @Test
    public void testDistinct() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        String fieldName = "fieldName";
        assertAll("distinct",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.distinct(null, Document.class)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.distinct(fieldName, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.distinct(fieldName, null, Document.class)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.distinct(clientSession, null, Document.class)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.distinct(clientSession, fieldName, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.distinct(clientSession, fieldName, null, Document.class)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.distinct(null, fieldName, Document.class)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.distinct(null, fieldName, filter, Document.class))
                  ),
                  () -> {
                      DistinctPublisher<Document> expected =
                              createDistinctPublisher(null, collection.getNamespace(), collection.getDocumentClass(), Document.class,
                                                      collection.getCodecRegistry(), collection.getReadPreference(),
                                                      collection.getReadConcern(), collection.getExecutor(), fieldName,
                                                      new BsonDocument(), collection.getRetryReads());
                      assertPublisherIsTheSameAs(expected, collection.distinct(fieldName, Document.class), "Default");
                  },
                  () -> {
                      DistinctPublisher<BsonDocument> expected =
                              createDistinctPublisher(null, collection.getNamespace(), collection.getDocumentClass(),
                                                      BsonDocument.class, collection.getCodecRegistry(), collection.getReadPreference(),
                                                      collection.getReadConcern(), collection.getExecutor(), fieldName,
                                                      filter, collection.getRetryReads());
                      assertPublisherIsTheSameAs(expected, collection.distinct(fieldName, filter, BsonDocument.class),
                                                 "With filter & result class");
                  },
                  () -> {
                      DistinctPublisher<Document> expected =
                              createDistinctPublisher(clientSession, collection.getNamespace(), collection.getDocumentClass(),
                                                      Document.class, collection.getCodecRegistry(), collection.getReadPreference(),
                                                      collection.getReadConcern(), collection.getExecutor(), fieldName,
                                                      new BsonDocument(), collection.getRetryReads());
                      assertPublisherIsTheSameAs(expected, collection.distinct(fieldName, Document.class), "With client session");
                  },
                  () -> {
                      DistinctPublisher<BsonDocument> expected =
                              createDistinctPublisher(clientSession, collection.getNamespace(), collection.getDocumentClass(),
                                                      BsonDocument.class, collection.getCodecRegistry(), collection.getReadPreference(),
                                                      collection.getReadConcern(), collection.getExecutor(), fieldName,
                                                      filter, collection.getRetryReads());
                      assertPublisherIsTheSameAs(expected, collection.distinct(fieldName, filter, BsonDocument.class),
                                                 "With client session, filter & result class");
                  }
        );
    }

    @Test
    public void testDrop() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        assertAll("drop",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.drop(null))
                  ),
                  () -> {
                      Mono<Void> expected = createWriteOperationMono(() -> collection.getOperations().dropCollection(),
                                                                     null, collection.getReadConcern(),
                                                                     collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.drop(), "Default");
                  },
                  () -> {
                      Mono<Void> expected = createWriteOperationMono(() -> collection.getOperations().dropCollection(),
                                                                     clientSession, collection.getReadConcern(),
                                                                     collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.drop(clientSession), "With client session");
                  }
        );
    }

    @Test
    public void testDropIndex() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        String indexName = "index_name";
        Bson index = Indexes.ascending(indexName);
        DropIndexOptions options = new DropIndexOptions().maxTime(1, TimeUnit.MILLISECONDS);
        assertAll("dropIndex",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.dropIndex((String) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.dropIndex((Bson) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.dropIndex(indexName, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.dropIndex(clientSession, (String) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.dropIndex(clientSession, (Bson) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.dropIndex(clientSession, (String) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.dropIndex(clientSession, indexName, null))

                                  ),
                  () -> {
                      Mono<Void> expected =
                              createWriteOperationMono(() -> collection.getOperations().dropIndex(indexName, new DropIndexOptions()),
                                                       null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.dropIndex(indexName), "Default string");
                  },
                  () -> {
                      Mono<Void> expected =
                              createWriteOperationMono(() -> collection.getOperations().dropIndex(index, new DropIndexOptions()),
                                                       null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.dropIndex(index), "Default bson");
                  },
                  () -> {
                      Mono<Void> expected =
                              createWriteOperationMono(() -> collection.getOperations().dropIndex(indexName, options),
                                                       null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.dropIndex(indexName, options), "With string & options");
                  },
                  () -> {
                      Mono<Void> expected =
                              createWriteOperationMono(() -> collection.getOperations().dropIndex(index, options),
                                                       null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.dropIndex(index, options), "With bson & options");
                  },
                  () -> {
                      Mono<Void> expected =
                              createWriteOperationMono(() -> collection.getOperations().dropIndex(indexName, new DropIndexOptions()),
                                                       clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.dropIndex(clientSession, indexName),
                                                 "With client session & string");
                  },
                  () -> {
                      Mono<Void> expected =
                              createWriteOperationMono(() -> collection.getOperations().dropIndex(index, new DropIndexOptions()),
                                                       clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.dropIndex(clientSession, index),
                                                 "With client session & bson");
                  },
                  () -> {
                      Mono<Void> expected =
                              createWriteOperationMono(() -> collection.getOperations().dropIndex(indexName, options),
                                                       clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.dropIndex(clientSession, indexName, options),
                                                 "With client session, string & options");
                  },
                  () -> {
                      Mono<Void> expected =
                              createWriteOperationMono(() -> collection.getOperations().dropIndex(index, options),
                                                       clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.dropIndex(clientSession, index, options),
                                                 "With client session, bson & options");
                  }
        );
    }

    @Test
    public void testDropIndexes() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        String allIndexes = "*";
        DropIndexOptions options = new DropIndexOptions().maxTime(1, TimeUnit.MILLISECONDS);
        assertAll("dropIndexes",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.dropIndexes((DropIndexOptions) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.dropIndexes(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.dropIndexes(null, options))

                  ),
                  () -> {
                      Mono<Void> expected =
                              createWriteOperationMono(() -> collection.getOperations().dropIndex(allIndexes, new DropIndexOptions()),
                                                       null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.dropIndexes(), "Default");
                  },
                  () -> {
                      Mono<Void> expected =
                              createWriteOperationMono(() -> collection.getOperations().dropIndex(allIndexes, options),
                                                       null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.dropIndexes(options), "With options");
                  },
                  () -> {
                      Mono<Void> expected =
                              createWriteOperationMono(() -> collection.getOperations().dropIndex(allIndexes, new DropIndexOptions()),
                                                       clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.dropIndexes(clientSession), "With client session");
                  },
                  () -> {
                      Mono<Void> expected =
                              createWriteOperationMono(() -> collection.getOperations().dropIndex(allIndexes, options),
                                                       clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.dropIndexes(clientSession, options),
                                                 "With client session & options");
                  }
        );
    }


    @Test
    public void testEstimatedDocumentCount() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        EstimatedDocumentCountOptions options = new EstimatedDocumentCountOptions().maxTime(1, TimeUnit.MILLISECONDS);
        assertAll("estimatedDocumentCount",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.estimatedDocumentCount(null))
                  ),
                  () -> {
                      Mono<Long> expected = createReadOperationMono(() -> collection.getOperations().count(
                              new BsonDocument(), fromEstimatedDocumentCountOptions(new EstimatedDocumentCountOptions()),
                              CountStrategy.COMMAND), null, collection.getReadPreference(), collection.getReadConcern(),
                                                                             collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.estimatedDocumentCount(), "Default");
                  },
                  () -> {
                      Mono<Long> expected = createReadOperationMono(() -> collection.getOperations().count(
                              new BsonDocument(), fromEstimatedDocumentCountOptions(options),
                              CountStrategy.COMMAND), null, collection.getReadPreference(), collection.getReadConcern(),
                                                                    collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.estimatedDocumentCount(options), "With options");
                  }
        );
    }

    @Test
    public void testFind() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        assertAll("find",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.find((Bson) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.find((Class<?>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.find(filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.find(clientSession, (Bson) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.find(clientSession, (Class<?>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.find(clientSession, filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.find(null, filter)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.find((ClientSession) null, Document.class)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.find(null, filter, Document.class))
                                  ),
                  () -> {
                      FindPublisher<Document> expected =
                              createFindPublisher(null, collection.getNamespace(), collection.getDocumentClass(),
                                                  collection.getDocumentClass(), collection.getCodecRegistry(),
                                                  collection.getReadPreference(), collection.getReadConcern(), collection.getExecutor(),
                                                  new BsonDocument(), collection.getRetryReads());
                      assertPublisherIsTheSameAs(expected, collection.find(), "Default");
                  },
                  () -> {
                      FindPublisher<Document> expected =
                              createFindPublisher(null, collection.getNamespace(), collection.getDocumentClass(),
                                                  collection.getDocumentClass(), collection.getCodecRegistry(),
                                                  collection.getReadPreference(), collection.getReadConcern(), collection.getExecutor(),
                                                  filter, collection.getRetryReads());
                      assertPublisherIsTheSameAs(expected, collection.find(filter), "With filter");
                  },
                  () -> {
                      FindPublisher<BsonDocument> expected =
                              createFindPublisher(null, collection.getNamespace(), collection.getDocumentClass(),
                                                  BsonDocument.class, collection.getCodecRegistry(),
                                                  collection.getReadPreference(), collection.getReadConcern(), collection.getExecutor(),
                                                  filter, collection.getRetryReads());
                      assertPublisherIsTheSameAs(expected, collection.find(filter, BsonDocument.class), "With filter & result class");
                  },
                  () -> {
                      FindPublisher<Document> expected =
                              createFindPublisher(clientSession, collection.getNamespace(), collection.getDocumentClass(),
                                                  collection.getDocumentClass(), collection.getCodecRegistry(),
                                                  collection.getReadPreference(), collection.getReadConcern(), collection.getExecutor(),
                                                  new BsonDocument(), collection.getRetryReads());
                      assertPublisherIsTheSameAs(expected, collection.find(clientSession), "With client session");
                  },
                  () -> {
                      FindPublisher<Document> expected =
                              createFindPublisher(clientSession, collection.getNamespace(), collection.getDocumentClass(),
                                                  collection.getDocumentClass(), collection.getCodecRegistry(),
                                                  collection.getReadPreference(), collection.getReadConcern(), collection.getExecutor(),
                                                  filter, collection.getRetryReads());
                      assertPublisherIsTheSameAs(expected, collection.find(clientSession, filter), "With client session & filter");
                  },
                  () -> {
                      FindPublisher<BsonDocument> expected =
                              createFindPublisher(clientSession, collection.getNamespace(), collection.getDocumentClass(),
                                                  BsonDocument.class, collection.getCodecRegistry(),
                                                  collection.getReadPreference(), collection.getReadConcern(), collection.getExecutor(),
                                                  filter, collection.getRetryReads());
                      assertPublisherIsTheSameAs(expected, collection.find(clientSession, filter, BsonDocument.class),
                                                 "With client session, filter & result class");
                  }
        );
    }

    @Test
    public void testFindOneAndDelete() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        FindOneAndDeleteOptions options = new FindOneAndDeleteOptions().collation(collation);
        assertAll("findOneAndDelete",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.findOneAndDelete(null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.findOneAndDelete(filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndDelete(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndDelete(clientSession, filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndDelete(null, filter)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndDelete(null, filter, options))
                                  ),
                  () -> {
                      Mono<Document> expected =
                              createWriteOperationMono(() -> collection.getOperations()
                                                               .findOneAndDelete(filter, new FindOneAndDeleteOptions()),
                                                       null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.findOneAndDelete(filter), "Default");
                  },
                  () -> {
                      Mono<Document> expected =
                              createWriteOperationMono(() -> collection.getOperations().findOneAndDelete(filter, options),
                                                       null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.findOneAndDelete(filter, options), "With filter & options");
                  },
                  () -> {
                      Mono<Document> expected =
                              createWriteOperationMono(() -> collection.getOperations()
                                                               .findOneAndDelete(filter, new FindOneAndDeleteOptions()),
                                                       clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.findOneAndDelete(clientSession, filter), "With client session");
                  },
                  () -> {
                      Mono<Document> expected =
                              createWriteOperationMono(() -> collection.getOperations().findOneAndDelete(filter, options),
                                                       clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.findOneAndDelete(clientSession, filter, options),
                                                 "With client session, filter & options");
                  }
        );
    }

    @Test
    public void testFindOneAndReplace() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        FindOneAndReplaceOptions options = new FindOneAndReplaceOptions().collation(collation);
        Document replacement = new Document();
        assertAll("findOneAndReplace",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.findOneAndReplace(null, replacement)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndReplace(filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndReplace(clientSession, null, replacement)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndReplace(clientSession, filter, replacement, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndReplace(null, filter, replacement)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndReplace(null, filter, replacement, options))
                  ),
                  () -> {
                      Mono<Document> expected =
                              createWriteOperationMono(() -> collection.getOperations()
                                                               .findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions()),
                                                       null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.findOneAndReplace(filter, replacement), "Default");
                  },
                  () -> {
                      Mono<Document> expected =
                              createWriteOperationMono(() -> collection.getOperations().findOneAndReplace(filter, replacement, options),
                                                       null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.findOneAndReplace(filter, replacement, options),
                                                 "With filter & options");
                  },
                  () -> {
                      Mono<Document> expected =
                              createWriteOperationMono(() -> collection.getOperations()
                                                               .findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions()),
                                                       clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.findOneAndReplace(clientSession, filter, replacement),
                                                 "With client session");
                  },
                  () -> {
                      Mono<Document> expected =
                              createWriteOperationMono(() -> collection.getOperations().findOneAndReplace(filter, replacement, options),
                                                       clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.findOneAndReplace(clientSession, filter, replacement, options),
                                                 "With client session, filter & options");
                  }
        );
    }

    @Test
    public void testFindOneAndUpdate() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions().collation(collation);
        Document update = new Document();
        assertAll("findOneAndUpdate",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.findOneAndUpdate(null, update)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndUpdate(filter, (Bson) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndUpdate(clientSession, null, update)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndUpdate(clientSession, filter, update, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndUpdate(null, filter, update)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.findOneAndUpdate(null, filter, update, options))
                  ),
                  () -> {
                      Mono<Document> expected =
                              createWriteOperationMono(() -> collection.getOperations()
                                                               .findOneAndUpdate(filter, update, new FindOneAndUpdateOptions()),
                                                       null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.findOneAndUpdate(filter, update), "Default");
                  },
                  () -> {
                      Mono<Document> expected =
                              createWriteOperationMono(() -> collection.getOperations().findOneAndUpdate(filter, update, options),
                                                       null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.findOneAndUpdate(filter, update, options),
                                                 "With filter & options");
                  },
                  () -> {
                      Mono<Document> expected =
                              createWriteOperationMono(() -> collection.getOperations()
                                                               .findOneAndUpdate(filter, update, new FindOneAndUpdateOptions()),
                                                       clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.findOneAndUpdate(clientSession, filter, update),
                                                 "With client session");
                  },
                  () -> {
                      Mono<Document> expected =
                              createWriteOperationMono(() -> collection.getOperations().findOneAndUpdate(filter, update, options),
                                                       clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.findOneAndUpdate(clientSession, filter, update, options),
                                                 "With client session, filter & options");
                  }
        );
    }

    @Test
    public void testInsertOne() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        InsertOneOptions options = new InsertOneOptions().bypassDocumentValidation(true);
        Document insert = new Document("_id", 1);
        assertAll("insertOne",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.insertOne(null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.insertOne(insert, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.insertOne(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.insertOne(clientSession, insert, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.insertOne(null, insert)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.insertOne(null, insert, options))
                  ),
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createSingleWriteRequestMono(() -> collection.getOperations().insertOne(insert, new InsertOneOptions()),
                                                           null, collection.getReadConcern(), collection.getExecutor(),
                                                           WriteRequest.Type.INSERT);
                      assertPublisherIsTheSameAs(expected, collection.insertOne(insert), "Default");
                  },
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createSingleWriteRequestMono(() -> collection.getOperations().insertOne(insert, options),
                                                           null, collection.getReadConcern(), collection.getExecutor(),
                                                           WriteRequest.Type.INSERT);
                      assertPublisherIsTheSameAs(expected, collection.insertOne(insert, options), "With options");
                  },
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createSingleWriteRequestMono(() -> collection.getOperations().insertOne(insert, new InsertOneOptions()),
                                                           clientSession, collection.getReadConcern(), collection.getExecutor(),
                                                           WriteRequest.Type.INSERT);
                      assertPublisherIsTheSameAs(expected, collection.insertOne(clientSession, insert), "With client session");
                  },
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createSingleWriteRequestMono(() -> collection.getOperations().insertOne(insert, options),
                                                           clientSession, collection.getReadConcern(), collection.getExecutor(),
                                                           WriteRequest.Type.INSERT);
                      assertPublisherIsTheSameAs(expected, collection.insertOne(clientSession, insert, options),
                                                 "With client session & options");
                  }
        );
    }

    @Test
    public void testInsertMany() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        InsertManyOptions options = new InsertManyOptions().bypassDocumentValidation(true);
        List<Document> inserts = singletonList(new Document("_id", 1));
        assertAll("insertMany",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.insertMany(null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.insertMany(inserts, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.insertMany(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.insertMany(clientSession, inserts, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.insertMany(null, inserts)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.insertMany(null, inserts, options))
                  ),
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createSingleWriteRequestMono(() -> collection.getOperations().insertMany(inserts, new InsertManyOptions()),
                                                           null, collection.getReadConcern(), collection.getExecutor(),
                                                           WriteRequest.Type.INSERT);
                      assertPublisherIsTheSameAs(expected, collection.insertMany(inserts), "Default");
                  },
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createSingleWriteRequestMono(() -> collection.getOperations().insertMany(inserts, options),
                                                           null, collection.getReadConcern(), collection.getExecutor(),
                                                           WriteRequest.Type.INSERT);
                      assertPublisherIsTheSameAs(expected, collection.insertMany(inserts, options), "With options");
                  },
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createSingleWriteRequestMono(() -> collection.getOperations().insertMany(inserts, new InsertManyOptions()),
                                                           clientSession, collection.getReadConcern(), collection.getExecutor(),
                                                           WriteRequest.Type.INSERT);
                      assertPublisherIsTheSameAs(expected, collection.insertMany(clientSession, inserts), "With client session");
                  },
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createSingleWriteRequestMono(() -> collection.getOperations().insertMany(inserts, options),
                                                           clientSession, collection.getReadConcern(), collection.getExecutor(),
                                                           WriteRequest.Type.INSERT);
                      assertPublisherIsTheSameAs(expected, collection.insertMany(clientSession, inserts, options),
                                                 "With client session & options");
                  }
        );
    }

    @Test
    public void testListIndexes() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        assertAll("listIndexes",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.listIndexes((Class<?>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.listIndexes(null, Document.class)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.listIndexes(clientSession, null))
                  ),
                  () -> {
                      ListIndexesPublisher<Document> expected =
                              createListIndexesPublisher(null, collection.getNamespace(), collection.getDocumentClass(),
                                                         collection.getCodecRegistry(), collection.getReadPreference(),
                                                         collection.getExecutor(),
                                                         collection.getRetryReads());
                      assertPublisherIsTheSameAs(expected, collection.listIndexes(), "Default");
                  },
                  () -> {
                      ListIndexesPublisher<BsonDocument> expected =
                              createListIndexesPublisher(null, collection.getNamespace(), BsonDocument.class,
                                                         collection.getCodecRegistry(), collection.getReadPreference(),
                                                         collection.getExecutor(),
                                                         collection.getRetryReads());
                      assertPublisherIsTheSameAs(expected, collection.listIndexes(BsonDocument.class), "With result class");
                  },
                  () -> {
                      ListIndexesPublisher<Document> expected =
                              createListIndexesPublisher(clientSession, collection.getNamespace(), collection.getDocumentClass(),
                                                         collection.getCodecRegistry(), collection.getReadPreference(),
                                                         collection.getExecutor(),
                                                         collection.getRetryReads());
                      assertPublisherIsTheSameAs(expected, collection.listIndexes(clientSession), "With client session");
                  },
                  () -> {
                      ListIndexesPublisher<BsonDocument> expected =
                              createListIndexesPublisher(clientSession, collection.getNamespace(), BsonDocument.class,
                                                         collection.getCodecRegistry(), collection.getReadPreference(),
                                                         collection.getExecutor(),
                                                         collection.getRetryReads());
                      assertPublisherIsTheSameAs(expected, collection.listIndexes(clientSession, BsonDocument.class),
                                                 "With client session & result class");
                  }
        );
    }

    @Test
    public void testMapReduce() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        String map = "map";
        String reduce = "reduce";

        assertAll("mapReduce",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.mapReduce(null, reduce)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.mapReduce(map, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.mapReduce(map, reduce, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.mapReduce(clientSession, null, reduce)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.mapReduce(clientSession, map, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.mapReduce(clientSession, map, reduce, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.mapReduce(null, map, reduce)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.mapReduce(null, map, reduce, Document.class))
                  ),
                  () -> {
                      MapReducePublisher<Document> expected =
                              createMapReducePublisher(null, collection.getNamespace(), collection.getDocumentClass(),
                                                       collection.getDocumentClass(), collection.getCodecRegistry(),
                                                       collection.getReadPreference(), collection.getReadConcern(),
                                                       collection.getWriteConcern(), collection.getExecutor(),
                                                       map, reduce);
                      assertPublisherIsTheSameAs(expected, collection.mapReduce(map, reduce), "Default");
                  },
                  () -> {
                      MapReducePublisher<BsonDocument> expected =
                              createMapReducePublisher(null, collection.getNamespace(), collection.getDocumentClass(),
                                                       BsonDocument.class, collection.getCodecRegistry(),
                                                       collection.getReadPreference(), collection.getReadConcern(),
                                                       collection.getWriteConcern(), collection.getExecutor(),
                                                       map, reduce);
                      assertPublisherIsTheSameAs(expected, collection.mapReduce(map, reduce, BsonDocument.class),
                                                 "With result class");
                  },
                  () -> {
                      MapReducePublisher<Document> expected =
                              createMapReducePublisher(clientSession, collection.getNamespace(), collection.getDocumentClass(),
                                                       collection.getDocumentClass(), collection.getCodecRegistry(),
                                                       collection.getReadPreference(), collection.getReadConcern(),
                                                       collection.getWriteConcern(), collection.getExecutor(),
                                                       map, reduce);
                      assertPublisherIsTheSameAs(expected, collection.mapReduce(clientSession, map, reduce), "With client session");
                  },
                  () -> {
                      MapReducePublisher<BsonDocument> expected =
                              createMapReducePublisher(clientSession, collection.getNamespace(), collection.getDocumentClass(),
                                                       BsonDocument.class, collection.getCodecRegistry(),
                                                       collection.getReadPreference(), collection.getReadConcern(),
                                                       collection.getWriteConcern(), collection.getExecutor(),
                                                       map, reduce);
                      assertPublisherIsTheSameAs(expected, collection.mapReduce(clientSession, map, reduce, BsonDocument.class),
                                                 "With client session & result class");
                  }
        );
    }

    @Test
    public void testRenameCollection() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        MongoNamespace mongoNamespace = new MongoNamespace("db2.coll2");
        RenameCollectionOptions options = new RenameCollectionOptions().dropTarget(true);
        assertAll("renameCollection",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.renameCollection(null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.renameCollection(mongoNamespace, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.renameCollection(clientSession, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.renameCollection(clientSession, mongoNamespace, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.renameCollection(null, mongoNamespace)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.renameCollection(null, mongoNamespace, options))
                                  ),
                  () -> {
                      Mono<Void> expected = createWriteOperationMono(
                              () -> collection.getOperations().renameCollection(mongoNamespace, new RenameCollectionOptions()),
                              null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.renameCollection(mongoNamespace), "Default");
                  },
                  () -> {
                      Mono<Void> expected = createWriteOperationMono(
                              () -> collection.getOperations().renameCollection(mongoNamespace, options),
                              null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.renameCollection(mongoNamespace, options), "With options");
                  },
                  () -> {
                      Mono<Void> expected = createWriteOperationMono(
                              () -> collection.getOperations().renameCollection(mongoNamespace, new RenameCollectionOptions()),
                              clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.renameCollection(clientSession, mongoNamespace),
                                                 "With client session");
                  },
                  () -> {
                      Mono<Void> expected = createWriteOperationMono(
                              () -> collection.getOperations().renameCollection(mongoNamespace, options),
                              clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.renameCollection(clientSession, mongoNamespace, options),
                                                 "With client session & options");
                  }
        );
    }

    @Test
    public void testReplaceOne() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        ReplaceOptions options = new ReplaceOptions().collation(collation);
        Document replacement = new Document();
        assertAll("replaceOne",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.replaceOne(null, replacement)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.replaceOne(filter, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.replaceOne(clientSession, null, replacement)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.replaceOne(clientSession, filter, replacement, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.replaceOne(null, filter, replacement)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.replaceOne(null, filter, replacement, options))
                  ),
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createWriteOperationMono(() -> collection.getOperations()
                                                               .replaceOne(filter, replacement, new ReplaceOptions()),
                                                       null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.replaceOne(filter, replacement), "Default");
                  },
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createWriteOperationMono(() -> collection.getOperations().replaceOne(filter, replacement, options),
                                                       null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.replaceOne(filter, replacement, options),
                                                 "With filter & options");
                  },
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createWriteOperationMono(() -> collection.getOperations()
                                                               .replaceOne(filter, replacement, new ReplaceOptions()),
                                                       clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.replaceOne(clientSession, filter, replacement),
                                                 "With client session");
                  },
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createWriteOperationMono(() -> collection.getOperations().replaceOne(filter, replacement, options),
                                                       clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.replaceOne(clientSession, filter, replacement, options),
                                                 "With client session, filter & options");
                  }
        );
    }

    @Test
    public void testUpdateOne() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        UpdateOptions options = new UpdateOptions().collation(collation);
        Document update = new Document();
        assertAll("updateOne",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.updateOne(null, update)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.updateOne(filter, (Bson) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.updateOne(clientSession, null, update)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.updateOne(clientSession, filter, update, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.updateOne(null, filter, update)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.updateOne(null, filter, update, options))
                  ),
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createWriteOperationMono(() -> collection.getOperations()
                                                               .updateOne(filter, update, new UpdateOptions()),
                                                       null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.updateOne(filter, update), "Default");
                  },
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createWriteOperationMono(() -> collection.getOperations().updateOne(filter, update, options),
                                                       null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.updateOne(filter, update, options),
                                                 "With filter & options");
                  },
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createWriteOperationMono(() -> collection.getOperations()
                                                               .updateOne(filter, update, new UpdateOptions()),
                                                       clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.updateOne(clientSession, filter, update),
                                                 "With client session");
                  },
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createWriteOperationMono(() -> collection.getOperations().updateOne(filter, update, options),
                                                       clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.updateOne(clientSession, filter, update, options),
                                                 "With client session, filter & options");
                  }
        );
    }

    @Test
    public void testUpdateMany() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        UpdateOptions options = new UpdateOptions().collation(collation);
        List<Document> updates = singletonList(new Document());
        assertAll("updateMany",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.updateMany(null, updates)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.updateMany(filter, (Bson) null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.updateMany(clientSession, null, updates)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.updateMany(clientSession, filter, updates, null)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.updateMany(null, filter, updates)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.updateMany(null, filter, updates, options))
                  ),
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createWriteOperationMono(() -> collection.getOperations()
                                                               .updateMany(filter, updates, new UpdateOptions()),
                                                       null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.updateMany(filter, updates), "Default");
                  },
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createWriteOperationMono(() -> collection.getOperations().updateMany(filter, updates, options),
                                                       null, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.updateMany(filter, updates, options),
                                                 "With filter & options");
                  },
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createWriteOperationMono(() -> collection.getOperations()
                                                               .updateMany(filter, updates, new UpdateOptions()),
                                                       clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.updateMany(clientSession, filter, updates),
                                                 "With client session");
                  },
                  () -> {
                      Mono<BulkWriteResult> expected =
                              createWriteOperationMono(() -> collection.getOperations().updateMany(filter, updates, options),
                                                       clientSession, collection.getReadConcern(), collection.getExecutor());
                      assertPublisherIsTheSameAs(expected, collection.updateMany(clientSession, filter, updates, options),
                                                 "With client session, filter & options");
                  }
        );
    }

    @Test
    void testWatch() {
        MongoCollectionImpl<Document> collection = createMongoCollection();
        List<Bson> pipeline = singletonList(BsonDocument.parse("{$match: {open: true}}"));
        assertAll("watch",
                  () -> assertAll("check validation",
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.watch((Class<?>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.watch((List<Bson>) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.watch(pipeline, null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.watch((ClientSession) null)),
                                  () -> assertThrows(IllegalArgumentException.class, () -> collection.watch(null, pipeline)),
                                  () -> assertThrows(IllegalArgumentException.class,
                                                     () -> collection.watch(null, pipeline, Document.class))
                  ),
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              createChangeStreamPublisher(null, collection.getNamespace(), Document.class,
                                                          collection.getCodecRegistry(),
                                                          collection.getReadPreference(),
                                                          collection.getReadConcern(), collection.getExecutor(),
                                                          emptyList(), ChangeStreamLevel.COLLECTION, collection.getRetryReads());

                      assertPublisherIsTheSameAs(expected, collection.watch(), "Default");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              createChangeStreamPublisher(null, collection.getNamespace(), Document.class,
                                                          collection.getCodecRegistry(),
                                                          collection.getReadPreference(),
                                                          collection.getReadConcern(), collection.getExecutor(), pipeline,
                                                          ChangeStreamLevel.COLLECTION, collection.getRetryReads());

                      assertPublisherIsTheSameAs(expected, collection.watch(pipeline), "With pipeline");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              createChangeStreamPublisher(null, collection.getNamespace(), BsonDocument.class,
                                                          collection.getCodecRegistry(),
                                                          collection.getReadPreference(),
                                                          collection.getReadConcern(), collection.getExecutor(),
                                                          emptyList(), ChangeStreamLevel.COLLECTION, collection.getRetryReads());

                      assertPublisherIsTheSameAs(expected, collection.watch(BsonDocument.class),
                                                 "With result class");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              createChangeStreamPublisher(null, collection.getNamespace(), BsonDocument.class,
                                                          collection.getCodecRegistry(),
                                                          collection.getReadPreference(),
                                                          collection.getReadConcern(), collection.getExecutor(), pipeline,
                                                          ChangeStreamLevel.COLLECTION, collection.getRetryReads());

                      assertPublisherIsTheSameAs(expected, collection.watch(pipeline, BsonDocument.class),
                                                 "With pipeline & result class");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              createChangeStreamPublisher(clientSession, collection.getNamespace(), Document.class,
                                                          collection.getCodecRegistry(),
                                                          collection.getReadPreference(),
                                                          collection.getReadConcern(), collection.getExecutor(),
                                                          emptyList(), ChangeStreamLevel.COLLECTION, collection.getRetryReads());

                      assertPublisherIsTheSameAs(expected, collection.watch(clientSession), "with session");
                  },
                  () -> {
                      ChangeStreamPublisher<Document> expected =
                              createChangeStreamPublisher(clientSession, collection.getNamespace(), Document.class,
                                                          collection.getCodecRegistry(),
                                                          collection.getReadPreference(),
                                                          collection.getReadConcern(), collection.getExecutor(), pipeline,
                                                          ChangeStreamLevel.COLLECTION, collection.getRetryReads());

                      assertPublisherIsTheSameAs(expected, collection.watch(clientSession, pipeline), "With session & pipeline");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              createChangeStreamPublisher(clientSession, collection.getNamespace(), BsonDocument.class,
                                                          collection.getCodecRegistry(),
                                                          collection.getReadPreference(),
                                                          collection.getReadConcern(), collection.getExecutor(),
                                                          emptyList(), ChangeStreamLevel.COLLECTION, collection.getRetryReads());

                      assertPublisherIsTheSameAs(expected, collection.watch(clientSession, BsonDocument.class),
                                                 "With session & resultClass");
                  },
                  () -> {
                      ChangeStreamPublisher<BsonDocument> expected =
                              createChangeStreamPublisher(clientSession, collection.getNamespace(), BsonDocument.class,
                                                          collection.getCodecRegistry(),
                                                          collection.getReadPreference(),
                                                          collection.getReadConcern(), collection.getExecutor(), pipeline,
                                                          ChangeStreamLevel.COLLECTION, collection.getRetryReads());

                      assertPublisherIsTheSameAs(expected, collection.watch(clientSession, pipeline, BsonDocument.class),
                                                 "With clientSession, pipeline & result class");
                  }
        );
    }

    MongoCollectionImpl<Document> createMongoCollection() {
        return new MongoCollectionImpl<>(new MongoNamespace("db.coll"), Document.class,
                                         MongoClientSettings.getDefaultCodecRegistry(), ReadPreference.primary(),
                                         ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED, mock(OperationExecutor.class),
                                     true, true, UuidRepresentation.STANDARD);
    }
}
