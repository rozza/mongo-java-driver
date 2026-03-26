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

import org.bson.BsonDocument;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.mongodb.ClusterFixture.configureFailPoint;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@SuppressWarnings("deprecation")
class DBFunctionalTest extends FunctionalSpecification {

    @Test
    @DisplayName("should throw WriteConcernException on write concern error for drop")
    void shouldThrowWriteConcernExceptionForDrop() {
        assumeTrue(isDiscoverableReplicaSet());

        database.createCollection("ctest", new BasicDBObject());
        int w = 2;
        database.setWriteConcern(new WriteConcern(w));
        configureFailPoint(BsonDocument.parse("{ configureFailPoint: \"failCommand\", "
                + "mode : {times : 1}, "
                + "data : {failCommands : [\"dropDatabase\"], "
                + "writeConcernError : {code : 100, errmsg : \"failed\"}}}"));
        try {
            WriteConcernException e = assertThrows(WriteConcernException.class, () ->
                    database.dropDatabase());
            assertEquals(100, e.getErrorCode());
        } finally {
            database.setWriteConcern(null);
        }
    }

    @Test
    @DisplayName("should throw WriteConcernException on write concern error for create collection")
    void shouldThrowWriteConcernExceptionForCreateCollection() {
        assumeTrue(isDiscoverableReplicaSet());

        database.setWriteConcern(new WriteConcern(5));
        try {
            WriteConcernException e = assertThrows(WriteConcernException.class, () ->
                    database.createCollection("ctest", new BasicDBObject()));
            assertEquals(100, e.getErrorCode());
        } finally {
            database.setWriteConcern(null);
        }
    }

    @Test
    @DisplayName("should throw WriteConcernException on write concern error for create view")
    void shouldThrowWriteConcernExceptionForCreateView() {
        assumeTrue(isDiscoverableReplicaSet());

        database.setWriteConcern(new WriteConcern(5));
        try {
            WriteConcernException e = assertThrows(WriteConcernException.class, () ->
                    database.createView("view1", "collection1", java.util.Collections.emptyList()));
            assertEquals(100, e.getErrorCode());
        } finally {
            database.setWriteConcern(null);
        }
    }

    @Test
    @DisplayName("should execute command with custom encoder")
    void shouldExecuteCommandWithCustomEncoder() {
        CommandResult commandResult = database.command(new BasicDBObject("isMaster", 1), DefaultDBEncoder.FACTORY.create());
        assertTrue(commandResult.ok());
    }
}
