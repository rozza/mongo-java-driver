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

import com.mongodb.MongoNamespace;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.OperationFunctionalSpecification;
import com.mongodb.WriteConcern;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.mongodb.ClusterFixture.getBinding;
import static com.mongodb.ClusterFixture.getOperationContext;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class CreateViewOperationSpecificationTest extends OperationFunctionalSpecification {

    @Test
    void shouldCreateViewSync() {
        String viewOn = getCollectionName();
        String viewName = getCollectionName() + "-view";
        MongoNamespace viewNamespace = new MongoNamespace(getDatabaseName(), viewName);

        assertFalse(collectionNameExists(viewOn));
        assertFalse(collectionNameExists(viewName));

        BsonDocument trueXDocument = new BsonDocument("_id", new BsonInt32(1)).append("x", BsonBoolean.TRUE);
        BsonDocument falseXDocument = new BsonDocument("_id", new BsonInt32(2)).append("x", BsonBoolean.FALSE);
        getCollectionHelper().insertDocuments(Arrays.asList(trueXDocument, falseXDocument));

        List<BsonDocument> pipeline = Arrays.asList(new BsonDocument("$match", trueXDocument));
        CreateViewOperation operation = new CreateViewOperation(getDatabaseName(), viewName, viewOn, pipeline,
                WriteConcern.ACKNOWLEDGED);

        try {
            execute(operation, false);

            BsonDocument options = getCollectionInfo(viewName).getDocument("options");
            assertEquals(new BsonString(viewOn), options.get("viewOn"));
            assertEquals(new BsonArray(pipeline), options.get("pipeline"));
            assertEquals(Arrays.asList(trueXDocument), getCollectionHelper(viewNamespace).find(new BsonDocumentCodec()));
        } finally {
            getCollectionHelper(viewNamespace).drop();
        }
    }

    @Test
    void shouldCreateViewAsync() {
        String viewOn = getCollectionName();
        String viewName = getCollectionName() + "-view";
        MongoNamespace viewNamespace = new MongoNamespace(getDatabaseName(), viewName);

        assertFalse(collectionNameExists(viewOn));
        assertFalse(collectionNameExists(viewName));

        BsonDocument trueXDocument = new BsonDocument("_id", new BsonInt32(1)).append("x", BsonBoolean.TRUE);
        BsonDocument falseXDocument = new BsonDocument("_id", new BsonInt32(2)).append("x", BsonBoolean.FALSE);
        getCollectionHelper().insertDocuments(Arrays.asList(trueXDocument, falseXDocument));

        List<BsonDocument> pipeline = Arrays.asList(new BsonDocument("$match", trueXDocument));
        CreateViewOperation operation = new CreateViewOperation(getDatabaseName(), viewName, viewOn, pipeline,
                WriteConcern.ACKNOWLEDGED);

        try {
            execute(operation, true);

            BsonDocument options = getCollectionInfo(viewName).getDocument("options");
            assertEquals(new BsonString(viewOn), options.get("viewOn"));
            assertEquals(new BsonArray(pipeline), options.get("pipeline"));
        } finally {
            getCollectionHelper(viewNamespace).drop();
        }
    }

    @Test
    void shouldCreateViewWithCollationSync() {
        String viewOn = getCollectionName();
        String viewName = getCollectionName() + "-view";
        MongoNamespace viewNamespace = new MongoNamespace(getDatabaseName(), viewName);

        CreateViewOperation operation = new CreateViewOperation(getDatabaseName(), viewName, viewOn,
                Collections.emptyList(), WriteConcern.ACKNOWLEDGED)
                .collation(DEFAULT_COLLATION);

        try {
            execute(operation, false);
            BsonDocument collectionCollation = getCollectionInfo(viewName)
                    .getDocument("options").getDocument("collation");
            collectionCollation.remove("version");

            assertEquals(DEFAULT_COLLATION.asDocument(), collectionCollation);
        } finally {
            getCollectionHelper(viewNamespace).drop();
        }
    }

    @Test
    void shouldThrowOnWriteConcernErrorSync() {
        assumeTrue(isDiscoverableReplicaSet());
        String viewName = getCollectionName() + "-view";
        MongoNamespace viewNamespace = new MongoNamespace(getDatabaseName(), viewName);
        assertFalse(collectionNameExists(viewName));

        CreateViewOperation operation = new CreateViewOperation(getDatabaseName(), viewName,
                getCollectionName(), Collections.emptyList(), new WriteConcern(5));

        try {
            MongoWriteConcernException ex = assertThrows(MongoWriteConcernException.class, () ->
                    execute(operation, false));
            assertTrue(ex.getWriteConcernError().getCode() == 100);
            assertTrue(ex.getWriteResult().wasAcknowledged());
        } finally {
            getCollectionHelper(viewNamespace).drop();
        }
    }

    @Test
    void shouldThrowOnWriteConcernErrorAsync() {
        assumeTrue(isDiscoverableReplicaSet());
        String viewName = getCollectionName() + "-view";
        MongoNamespace viewNamespace = new MongoNamespace(getDatabaseName(), viewName);
        assertFalse(collectionNameExists(viewName));

        CreateViewOperation operation = new CreateViewOperation(getDatabaseName(), viewName,
                getCollectionName(), Collections.emptyList(), new WriteConcern(5));

        try {
            MongoWriteConcernException ex = assertThrows(MongoWriteConcernException.class, () ->
                    execute(operation, true));
            assertTrue(ex.getWriteConcernError().getCode() == 100);
            assertTrue(ex.getWriteResult().wasAcknowledged());
        } finally {
            getCollectionHelper(viewNamespace).drop();
        }
    }

    private BsonDocument getCollectionInfo(final String collectionName) {
        com.mongodb.internal.binding.ReadWriteBinding binding = getBinding();
        BatchCursor<BsonDocument> cursor = new ListCollectionsOperation(getDatabaseName(), new BsonDocumentCodec())
                .filter(new BsonDocument("name", new BsonString(collectionName)))
                .execute(binding, getOperationContext(binding.getReadPreference()));
        java.util.List<BsonDocument> batch = cursor.tryNext();
        return batch != null && !batch.isEmpty() ? batch.get(0) : null;
    }

    private boolean collectionNameExists(final String collectionName) {
        return getCollectionInfo(collectionName) != null;
    }
}
