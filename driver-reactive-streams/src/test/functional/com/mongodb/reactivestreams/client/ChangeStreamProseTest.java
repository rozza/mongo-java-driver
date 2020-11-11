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

import com.mongodb.MongoChangeStreamException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoQueryException;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.test.CollectionHelper;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.util.List;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.reactivestreams.client.Fixture.getDefaultDatabaseName;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

// See https://github.com/mongodb/specifications/tree/master/source/change-streams/tests/README.rst#prose-tests
public class ChangeStreamProseTest extends DatabaseTestCase {
    private BsonDocument failPointDocument;
    private CollectionHelper<Document> collectionHelper;

    @Before
    @Override
    public void setUp() {
        assumeTrue(canRunTests());
        super.setUp();
        collectionHelper = new CollectionHelper<>(new DocumentCodec(), new MongoNamespace(getDefaultDatabaseName(), "test"));

        // create the collection before starting tests
        Mono.from(collection.insertOne(Document.parse("{ _id : 0 }"))).block(TIMEOUT_DURATION);
    }

    //
    // Test that the ChangeStream will throw an exception if the server response is missing the resume token (if wire version is < 8).
    //
    @Test
    public void testMissingResumeTokenThrowsException() {
        boolean exceptionFound = false;

        try (AggregationBatchCursor<ChangeStreamDocument<Document>> cursor = createChangeStreamCursor(
                collection.watch(singletonList(Aggregates.project(Document.parse("{ _id : 0 }")))))) {
            insertOneDocument();
            getNextBatch(cursor);
        } catch (MongoChangeStreamException e) {
            exceptionFound = true;
        } catch (MongoQueryException e) {
            if (serverVersionAtLeast(4, 1)) {
                exceptionFound = true;
            }
        }
        assertTrue(exceptionFound);
    }

    //
    // Test that the ChangeStream will automatically resume one time on a resumable error (including not master)
    // with the initial pipeline and options, except for the addition/update of a resumeToken.
    //
    @Test
    public void testResumeOneTimeOnError() {
        assumeTrue(serverVersionAtLeast(4, 0));

        try (AggregationBatchCursor<ChangeStreamDocument<Document>> cursor = createChangeStreamCursor()) {
            insertOneDocument();
            setFailPoint("getMore", 10107);
            assertNotNull(getNextBatch(cursor));
        } finally {
            disableFailPoint();
        }
    }

    //
    // Test that ChangeStream will not attempt to resume on any error encountered while executing an aggregate command.
    //
    @Test
    public void testNoResumeForAggregateErrors() {
        boolean exceptionFound = false;
        AggregationBatchCursor<ChangeStreamDocument<Document>> cursor = null;

        try {
            cursor = createChangeStreamCursor(collection.watch(singletonList(Document.parse("{ $unsupportedStage: { _id : 0 } }"))));
        } catch (MongoCommandException e) {
            exceptionFound = true;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        assertTrue(exceptionFound);
    }

    private void insertOneDocument() {
       Mono.from(collection.insertOne(Document.parse("{ x: 1 }"))).block(TIMEOUT_DURATION);
    }

    private AggregationBatchCursor<ChangeStreamDocument<Document>> createChangeStreamCursor() {
        return createChangeStreamCursor(collection.watch());
    }

    private AggregationBatchCursor<ChangeStreamDocument<Document>> createChangeStreamCursor(
            final ChangeStreamPublisher<Document> changeStreamPublisher) {
        return Mono.from(changeStreamPublisher.batchCursor()).block(TIMEOUT_DURATION);
    }

    private List<ChangeStreamDocument<Document>> getNextBatch(final AggregationBatchCursor<ChangeStreamDocument<Document>> cursor) {
        return Mono.from(cursor.next()).block(TIMEOUT_DURATION);
    }

    private void setFailPoint(final String command, final int errCode) {
        failPointDocument = new BsonDocument("configureFailPoint", new BsonString("failCommand"))
                .append("mode", new BsonDocument("times", new BsonInt32(1)))
                .append("data", new BsonDocument("failCommands", new BsonArray(asList(new BsonString(command))))
                        .append("errorCode", new BsonInt32(errCode))
                        .append("errorLabels", new BsonArray(asList(new BsonString("ResumableChangeStreamError")))));
        collectionHelper.runAdminCommand(failPointDocument);
    }

    private void disableFailPoint() {
        collectionHelper.runAdminCommand(failPointDocument.append("mode", new BsonString("off")));
    }

    private boolean canRunTests() {
        return isDiscoverableReplicaSet() && serverVersionAtLeast(3, 6);
    }

}
