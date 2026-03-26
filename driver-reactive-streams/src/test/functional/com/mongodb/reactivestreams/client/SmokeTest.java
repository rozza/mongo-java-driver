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

import com.mongodb.MongoDriverInformation;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.ClusterFixture.getConnectionString;
import static com.mongodb.reactivestreams.client.Fixture.getMongoClient;
import static com.mongodb.reactivestreams.client.Fixture.isReplicaSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class SmokeTest extends FunctionalSpecification {

    private static final Logger LOGGER = Loggers.getLogger("smokeTest");

    @Test
    void shouldHandleCommonScenariosWithoutError() {
        MongoClient mongoClient = getMongoClient();
        String databaseName = getDatabaseName();
        String collectionName = getCollectionName();
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        Document document = new Document("_id", 1);
        Document updatedDocument = new Document("_id", 1).append("a", 1);

        // clean up old database
        LOGGER.debug("clean up old database");
        Flux.from(mongoClient.getDatabase(databaseName).drop()).collectList().block(TIMEOUT_DURATION);

        LOGGER.debug("get database names");
        List<String> names = Flux.from(mongoClient.listDatabaseNames()).collectList().block(TIMEOUT_DURATION);

        // Get Database Names
        assertFalse(names.contains(null));

        // Create a collection and the created database is in the list
        LOGGER.debug("Create a collection and the created database is in the list");
        List<Void> createResult = Flux.from(database.createCollection(collectionName)).collectList().block(TIMEOUT_DURATION);
        assertEquals(Collections.emptyList(), createResult);

        LOGGER.debug("get database names");
        List<String> updatedNames = Flux.from(mongoClient.listDatabaseNames()).collectList().block(TIMEOUT_DURATION);

        // The database names should contain the database and be one bigger than before
        assertTrue(updatedNames.contains(databaseName));
        assertEquals(names.size() + 1, updatedNames.size());

        // The collection name should be in the collection names list
        LOGGER.debug("The collection name should be in the collection names list");
        List<String> collectionNames = Flux.from(database.listCollectionNames()).collectList().block(TIMEOUT_DURATION);
        assertFalse(collectionNames.contains(null));
        assertTrue(collectionNames.contains(collectionName));

        // The count is zero
        LOGGER.debug("The count is zero");
        Long count = Flux.from(collection.countDocuments()).collectList().block(TIMEOUT_DURATION).get(0);
        assertEquals(0L, count);

        // find first should return nothing if no documents
        LOGGER.debug("find first should return nothing if no documents");
        List<Document> firstResult = Flux.from(collection.find().first()).collectList().block(TIMEOUT_DURATION);
        assertEquals(Collections.emptyList(), firstResult);

        // find should return an empty list
        LOGGER.debug("find should return an empty list");
        List<Document> findResult = Flux.from(collection.find()).collectList().block(TIMEOUT_DURATION);
        assertEquals(Collections.emptyList(), findResult);

        // Insert a document
        LOGGER.debug("Insert a document");
        InsertOneResult insertResult = Flux.from(collection.insertOne(document)).collectList().block(TIMEOUT_DURATION).get(0);
        assertEquals(InsertOneResult.acknowledged(new BsonInt32(1)), insertResult);

        // The count is one
        LOGGER.debug("The count is one");
        count = Flux.from(collection.countDocuments()).collectList().block(TIMEOUT_DURATION).get(0);
        assertEquals(1L, count);

        // find that document
        LOGGER.debug("find that document");
        Document foundDocument = Flux.from(collection.find().first()).collectList().block(TIMEOUT_DURATION).get(0);
        assertEquals(document, foundDocument);

        // update that document
        LOGGER.debug("update that document");
        assertTrue(Flux.from(collection.updateOne(document, new Document("$set", new Document("a", 1))))
                .collectList().block(TIMEOUT_DURATION).get(0).wasAcknowledged());

        // find the updated document
        LOGGER.debug("find the updated document");
        Document foundUpdatedDocument = Flux.from(collection.find().first()).collectList().block(TIMEOUT_DURATION).get(0);
        assertEquals(updatedDocument, foundUpdatedDocument);

        // aggregate the collection
        LOGGER.debug("aggregate the collection");
        Document aggregatedDocument = Flux.from(collection.aggregate(
                Collections.singletonList(new Document("$match", new Document("a", 1)))))
                .collectList().block(TIMEOUT_DURATION).get(0);
        assertEquals(updatedDocument, aggregatedDocument);

        // remove all documents
        LOGGER.debug("remove all documents");
        assertEquals(1, Flux.from(collection.deleteOne(new Document()))
                .collectList().block(TIMEOUT_DURATION).get(0).getDeletedCount());

        // The count is zero
        LOGGER.debug("The count is zero");
        count = Flux.from(collection.countDocuments()).collectList().block(TIMEOUT_DURATION).get(0);
        assertEquals(0L, count);

        // create an index
        LOGGER.debug("create an index");
        String indexName = Flux.from(collection.createIndex(new Document("test", 1)))
                .collectList().block(TIMEOUT_DURATION).get(0);
        assertEquals("test_1", indexName);

        // has the newly created index
        LOGGER.debug("has the newly created index");
        List<String> indexNames = Flux.from(collection.listIndexes()).collectList().block(TIMEOUT_DURATION)
                .stream().map(doc -> doc.getString("name")).collect(Collectors.toList());
        assertTrue(indexNames.containsAll(Arrays.asList("_id_", "test_1")));

        // create multiple indexes
        LOGGER.debug("create multiple indexes");
        String multiIndexName = Flux.from(collection.createIndexes(
                Collections.singletonList(new IndexModel(new Document("multi", 1)))))
                .collectList().block(TIMEOUT_DURATION).get(0);
        assertEquals("multi_1", multiIndexName);

        // has the newly created index
        LOGGER.debug("has the newly created index");
        List<String> indexNamesUpdated = Flux.from(collection.listIndexes()).collectList().block(TIMEOUT_DURATION)
                .stream().map(doc -> doc.getString("name")).collect(Collectors.toList());
        assertTrue(indexNamesUpdated.containsAll(Arrays.asList("_id_", "test_1", "multi_1")));

        // drop the index
        LOGGER.debug("drop the index");
        assertEquals(Collections.emptyList(),
                Flux.from(collection.dropIndex("multi_1")).collectList().block(TIMEOUT_DURATION));

        // has a single index left "_id_"
        LOGGER.debug("has a single index left \"_id_\"");
        assertEquals(2, Flux.from(collection.listIndexes()).collectList().block(TIMEOUT_DURATION).size());

        // drop the index
        LOGGER.debug("drop the index");
        assertEquals(Collections.emptyList(),
                Flux.from(collection.dropIndex("test_1")).collectList().block(TIMEOUT_DURATION));

        // has a single index left "_id_"
        LOGGER.debug("has a single index left \"_id_\"");
        assertEquals(1, Flux.from(collection.listIndexes()).collectList().block(TIMEOUT_DURATION).size());

        // can rename the collection
        String newCollectionName = "new" + collectionName.substring(0, 1).toUpperCase() + collectionName.substring(1);
        LOGGER.debug("can rename the collection");
        assertEquals(Collections.emptyList(),
                Flux.from(collection.renameCollection(new MongoNamespace(databaseName, newCollectionName)))
                        .collectList().block(TIMEOUT_DURATION));

        // the new collection name is in the collection names list
        LOGGER.debug("the new collection name is in the collection names list");
        assertFalse(Flux.from(database.listCollectionNames()).collectList().block(TIMEOUT_DURATION)
                .contains(collectionName));
        assertTrue(Flux.from(database.listCollectionNames()).collectList().block(TIMEOUT_DURATION)
                .contains(newCollectionName));

        collection = database.getCollection(newCollectionName);

        // drop the collection
        LOGGER.debug("drop the collection");
        assertEquals(Collections.emptyList(),
                Flux.from(collection.drop()).collectList().block(TIMEOUT_DURATION));

        // there are no indexes
        LOGGER.debug("there are no indexes");
        assertEquals(0, Flux.from(collection.listIndexes()).collectList().block(TIMEOUT_DURATION).size());

        // the collection name is no longer in the collectionNames list
        LOGGER.debug("the collection name is no longer in the collectionNames list");
        assertFalse(Flux.from(database.listCollectionNames()).collectList().block(TIMEOUT_DURATION)
                .contains(collectionName));
    }

    @Test
    void shouldCommitATransaction() {
        assumeTrue(isReplicaSet());

        LOGGER.debug("create collection");
        Flux.from(database.createCollection(collection.getNamespace().getCollectionName()))
                .collectList().block(TIMEOUT_DURATION);

        ClientSession session = null;
        try {
            LOGGER.debug("start a session");
            session = Flux.from(getMongoClient().startSession()).collectList().block(TIMEOUT_DURATION).get(0);
            session.startTransaction();

            LOGGER.debug("insert a document");
            Flux.from(collection.insertOne(session, new Document("_id", 1))).collectList().block(TIMEOUT_DURATION);

            LOGGER.debug("commit a transaction");
            Flux.from(session.commitTransaction()).collectList().block(TIMEOUT_DURATION);

            LOGGER.debug("The count is one");
            Long count = Flux.from(collection.countDocuments()).collectList().block(TIMEOUT_DURATION).get(0);
            assertEquals(1L, count);
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    @Test
    void shouldAbortATransaction() {
        assumeTrue(isReplicaSet());

        LOGGER.debug("create collection");
        Flux.from(database.createCollection(collection.getNamespace().getCollectionName()))
                .collectList().block(TIMEOUT_DURATION);

        ClientSession session = null;
        try {
            LOGGER.debug("start a session");
            session = Flux.from(getMongoClient().startSession()).collectList().block(TIMEOUT_DURATION).get(0);
            session.startTransaction();

            LOGGER.debug("insert a document");
            Flux.from(collection.insertOne(session, new Document("_id", 1))).collectList().block(TIMEOUT_DURATION);

            LOGGER.debug("abort a transaction");
            Flux.from(session.abortTransaction()).collectList().block(TIMEOUT_DURATION);

            LOGGER.debug("The count is zero");
            Long count = Flux.from(collection.countDocuments()).collectList().block(TIMEOUT_DURATION).get(0);
            assertEquals(0L, count);
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    @Test
    void shouldNotLeakExceptionsWhenAClientIsClosed() {
        MongoClient mongoClient = MongoClients.create(getConnectionString());
        mongoClient.close();

        assertThrows(IllegalStateException.class, () ->
                Flux.from(mongoClient.listDatabaseNames()).collectList().block(TIMEOUT_DURATION));
    }

    @Test
    void shouldAcceptCustomMongoDriverInformation() {
        MongoDriverInformation driverInformation = MongoDriverInformation.builder()
                .driverName("test").driverVersion("1.2.0").build();
        MongoClient client = MongoClients.create(getConnectionString(), driverInformation);
        client.close();
    }

    @Test
    void shouldVisitAllDocumentsFromACursorWithMultipleBatches() {
        int total = 1000;
        List<Document> documents = IntStream.rangeClosed(1, total)
                .mapToObj(i -> new Document("_id", i))
                .collect(Collectors.toList());

        LOGGER.debug("Insert 1000 documents");
        Flux.from(collection.insertMany(documents)).collectList().block(TIMEOUT_DURATION);

        int counted = Flux.from(collection.find(new Document()).sort(new Document("_id", 1)).batchSize(10))
                .collectList().block(TIMEOUT_DURATION).size();

        assertEquals(documents.size(), counted);
    }

    @Test
    void shouldBulkInsertRawBsonDocuments() {
        List<RawBsonDocument> docs = Arrays.asList(
                RawBsonDocument.parse("{a: 1}"),
                RawBsonDocument.parse("{a: 2}"));

        LOGGER.debug("Insert RawBsonDocuments");
        List<InsertManyResult> result = Flux.from(collection.withDocumentClass(RawBsonDocument.class).insertMany(docs))
                .collectList().block(TIMEOUT_DURATION);

        Map<Integer, BsonValue> expectedIds = new HashMap<>();
        expectedIds.put(0, null);
        expectedIds.put(1, null);
        assertEquals(expectedIds, result.get(0).getInsertedIds());
    }
}
