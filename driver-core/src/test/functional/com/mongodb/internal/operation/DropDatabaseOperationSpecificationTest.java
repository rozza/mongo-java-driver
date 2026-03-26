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

package com.mongodb.internal.operation;

import com.mongodb.ClusterFixture;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.OperationFunctionalSpecification;
import com.mongodb.WriteConcern;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.mongodb.ClusterFixture.configureFailPoint;
import static com.mongodb.ClusterFixture.executeAsync;
import static com.mongodb.ClusterFixture.getBinding;
import static com.mongodb.ClusterFixture.getOperationContext;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isSharded;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class DropDatabaseOperationSpecificationTest extends OperationFunctionalSpecification {

    @Test
    void shouldDropDatabaseThatExistsSync() {
        assumeTrue(!isSharded());
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document("documentTo", "createTheCollection"));
        assertTrue(databaseNameExists(getDatabaseName()));

        execute(new DropDatabaseOperation(getDatabaseName(), WriteConcern.ACKNOWLEDGED), false);

        assertFalse(databaseNameExists(getDatabaseName()));
    }

    @Test
    void shouldDropDatabaseThatExistsAsync() {
        assumeTrue(!isSharded());
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document("documentTo", "createTheCollection"));
        assertTrue(databaseNameExists(getDatabaseName()));

        execute(new DropDatabaseOperation(getDatabaseName(), WriteConcern.ACKNOWLEDGED), true);

        assertFalse(databaseNameExists(getDatabaseName()));
    }

    @Test
    void shouldNotErrorWhenDroppingNonExistentDatabaseSync() {
        String dbName = "nonExistingDatabase";
        execute(new DropDatabaseOperation(dbName, WriteConcern.ACKNOWLEDGED), false);
        assertFalse(databaseNameExists(dbName));
    }

    @Test
    void shouldNotErrorWhenDroppingNonExistentDatabaseAsync() {
        String dbName = "nonExistingDatabase";
        execute(new DropDatabaseOperation(dbName, WriteConcern.ACKNOWLEDGED), true);
        assertFalse(databaseNameExists(dbName));
    }

    @Test
    void shouldThrowOnWriteConcernErrorSync() {
        assumeTrue(isDiscoverableReplicaSet());
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document("documentTo", "createTheCollection"));

        DropDatabaseOperation operation = new DropDatabaseOperation(getDatabaseName(), new WriteConcern(2));
        configureFailPoint(BsonDocument.parse("{ configureFailPoint: \"failCommand\", "
                + "mode : {times : 1}, "
                + "data : {failCommands : [\"dropDatabase\"], "
                + "writeConcernError : {code : 100, errmsg : \"failed\"}}}"));

        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        MongoWriteConcernException ex = assertThrows(MongoWriteConcernException.class, () ->
                operation.execute(binding, getOperationContext(binding.getReadPreference())));
        assertTrue(ex.getWriteConcernError().getCode() == 100);
        assertTrue(ex.getWriteResult().wasAcknowledged());
    }

    @Test
    void shouldThrowOnWriteConcernErrorAsync() {
        assumeTrue(isDiscoverableReplicaSet());
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document("documentTo", "createTheCollection"));

        DropDatabaseOperation operation = new DropDatabaseOperation(getDatabaseName(), new WriteConcern(2));
        configureFailPoint(BsonDocument.parse("{ configureFailPoint: \"failCommand\", "
                + "mode : {times : 1}, "
                + "data : {failCommands : [\"dropDatabase\"], "
                + "writeConcernError : {code : 100, errmsg : \"failed\"}}}"));

        MongoWriteConcernException ex = assertThrows(MongoWriteConcernException.class, () ->
                executeAsync(operation));
        assertTrue(ex.getWriteConcernError().getCode() == 100);
        assertTrue(ex.getWriteResult().wasAcknowledged());
    }

    @SuppressWarnings("unchecked")
    private boolean databaseNameExists(final String databaseName) {
        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        List<Document> databases = new ListDatabasesOperation(new DocumentCodec())
                .execute(binding, getOperationContext(binding.getReadPreference())).next();
        return databases.stream().anyMatch(doc -> databaseName.equals(doc.get("name")));
    }
}
