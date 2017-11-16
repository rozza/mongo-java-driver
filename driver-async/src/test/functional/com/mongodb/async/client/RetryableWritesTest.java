/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.async.client;

import com.mongodb.ClusterFixture;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.connection.ServerVersion;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.async.client.Fixture.getMongoClientBuilderFromConnectionString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

// See https://github.com/mongodb/specifications/tree/master/source/retryable-writes/tests
@RunWith(Parameterized.class)
public class RetryableWritesTest extends DatabaseTestCase {
    private static MongoClient mongoClient;
    private final String filename;
    private final String description;
    private final String databaseName;
    private final String collectionName;
    private final BsonArray data;
    private final BsonDocument definition;
    private MongoCollection<BsonDocument> collection;
    private JsonPoweredCrudTestHelper helper;

    public RetryableWritesTest(final String filename, final String description, final BsonArray data, final BsonDocument definition) {
        this.filename = filename;
        this.description = description;
        this.databaseName = getDefaultDatabaseName();
        this.collectionName = filename.substring(0, filename.lastIndexOf("."));
        this.data = data;
        this.definition = definition;
    }

    @BeforeClass
    public static void beforeClass() {
        MongoClientSettings.Builder builder = getMongoClientBuilderFromConnectionString();
        mongoClient = MongoClients.create(builder.retryWrites(true).build());
    }

    @AfterClass
    public static void afterClass() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Before
    @Override
    public void setUp() {
        assumeTrue(canRunTests());

        ServerVersion serverVersion = ClusterFixture.getServerVersion();
        if (definition.containsKey("ignore_if_server_version_less_than")) {
            assumeFalse(serverVersion.compareTo(getServerVersion("ignore_if_server_version_less_than")) < 0);
        }
        if (definition.containsKey("ignore_if_server_version_greater_than")) {
            assumeFalse(serverVersion.compareTo(getServerVersion("ignore_if_server_version_greater_than")) > 0);
        }
        if (definition.containsKey("ignore_if_topology_type")) {
            BsonArray topologyTypes = definition.getArray("ignore_if_topology_type");
            for (BsonValue type : topologyTypes) {
                String typeString = type.asString().getValue();
                if (typeString.equals("sharded")) {
                    assumeFalse(isSharded());
                } else if (typeString.equals("replica_set")) {
                    assumeFalse(isDiscoverableReplicaSet());
                } else if (typeString.equals("standalone")) {
                    assumeFalse(isStandalone());
                }
            }
        }

        List<BsonDocument> documents = new ArrayList<BsonDocument>();
        for (BsonValue document : data) {
            documents.add(document.asDocument());
        }
        CollectionHelper<Document> collectionHelper = new CollectionHelper<Document>(new DocumentCodec(),
                new MongoNamespace(databaseName, collectionName));

        collectionHelper.drop();
        collectionHelper.insertDocuments(documents);

        collection = mongoClient.getDatabase(databaseName).getCollection(collectionName, BsonDocument.class);
        helper = new JsonPoweredCrudTestHelper(description, collection);
        setFailPoint();
    }

    @After
    public void cleanUp() {
        if (canRunTests()) {
            unsetFailPoint();
        }
    }

    @Test
    public void shouldPassAllOutcomes() {
        BsonDocument operation = definition.getDocument("operation");
        BsonDocument outcome = definition.getDocument("outcome");

        BsonDocument result = new BsonDocument();
        boolean wasException = false;
        try {
            result = helper.getOperationResults(operation);
        } catch (Exception e) {
            wasException = true;
        }

        if (outcome.containsKey("collection")) {
            FutureResultCallback<List<BsonDocument>> futureResultCallback = new FutureResultCallback<List<BsonDocument>>();
            collection.withDocumentClass(BsonDocument.class).find().into(new ArrayList<BsonDocument>(), futureResultCallback);
            assertEquals(outcome.getDocument("collection").getArray("data").getValues(), futureResult(futureResultCallback));
        }

        if (outcome.getBoolean("error", BsonBoolean.FALSE).getValue()) {
            assertEquals(outcome.containsKey("error"), wasException);
        } else {
            BsonDocument fixedExpectedResult = fixExpectedResult(outcome.getDocument("result", new BsonDocument()));
            assertEquals(result.getDocument("result", new BsonDocument()), fixedExpectedResult);
        }
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/retryable-writes")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            for (BsonValue test : testDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(),
                        testDocument.getArray("data"), test.asDocument()});
            }
        }
        return data;
    }

    private boolean canRunTests() {
        return serverVersionAtLeast(3, 6) && isDiscoverableReplicaSet();
    }

    private BsonDocument fixResult(final BsonDocument result) {
        if (result.containsKey("insertedIds") && result.get("insertedIds").isDocument()) {
            result.remove("insertedIds");
        }
        return result;
    }

    private BsonDocument fixExpectedResult(final BsonDocument result) {
        if (result.containsKey("insertedIds") && result.get("insertedIds").isDocument()) {
            BsonDocument insertedIds = result.getDocument("insertedIds");
            result.put("insertedIds", new BsonArray(new ArrayList<BsonValue>(insertedIds.values())));

            if (result.containsKey("modifiedCount") && !result.containsKey("insertedCount")) {
                result.put("insertedCount", new BsonInt32(insertedIds.size()));
                result.put("insertedIds", new BsonDocument());
            }
        }

        return result;
    }

    private void setFailPoint() {
        if (definition.containsKey("failPoint")) {
            BsonDocument command = new BsonDocument("configureFailPoint", new BsonString("onPrimaryTransactionalWrite"));
            for (Map.Entry<String, BsonValue> args : definition.getDocument("failPoint").entrySet()) {
                command.put(args.getKey(), args.getValue());
            }
            FutureResultCallback<Document> futureResultCallback = new FutureResultCallback<Document>();
            mongoClient.getDatabase("admin").runCommand(command, futureResultCallback);
            futureResult(futureResultCallback);
        }
    }

    private void unsetFailPoint() {
        FutureResultCallback<Document> futureResultCallback = new FutureResultCallback<Document>();
        mongoClient.getDatabase("admin").runCommand(
                BsonDocument.parse("{ configureFailPoint: 'onPrimaryTransactionalWrite', mode: 'off'}"), futureResultCallback);
        futureResult(futureResultCallback);
    }

    <T> T futureResult(final FutureResultCallback<T> callback) {
        try {
            return callback.get();
        } catch (Throwable t) {
            throw new MongoException("FutureResultCallback failed", t);
        }
    }

    private ServerVersion getServerVersion(final String fieldName) {
        String[] versionStringArray = definition.getString(fieldName).getValue().split("\\.");
        return new ServerVersion(Integer.parseInt(versionStringArray[0]), Integer.parseInt(versionStringArray[1]));
    }
}
