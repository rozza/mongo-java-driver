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

package com.mongodb.connection;

import com.mongodb.OperationFunctionalSpecification;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.operation.CommandReadOperation;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.Test;

import static com.mongodb.ClusterFixture.LEGACY_HELLO;
import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ClusterFixture.getBinding;
import static com.mongodb.connection.ConnectionDescription.getDefaultMaxMessageSize;
import static com.mongodb.connection.ConnectionDescription.getDefaultMaxWriteBatchSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ConnectionSpecificationTest extends OperationFunctionalSpecification {

    @Test
    void shouldHaveId() {
        ConnectionSource source = getBinding().getReadConnectionSource(OPERATION_CONTEXT);
        Connection connection = source.getConnection(OPERATION_CONTEXT);
        try {
            assertNotNull(connection.getDescription().getConnectionId());
        } finally {
            connection.release();
            source.release();
        }
    }

    @Test
    void shouldHaveDescription() {
        BsonDocument commandResult = getHelloResult();
        int expectedMaxMessageSize = commandResult.getNumber("maxMessageSizeBytes",
                new BsonInt32(getDefaultMaxMessageSize())).intValue();
        int expectedMaxBatchCount = commandResult.getNumber("maxWriteBatchSize",
                new BsonInt32(getDefaultMaxWriteBatchSize())).intValue();

        ConnectionSource source = getBinding().getReadConnectionSource(OPERATION_CONTEXT);
        Connection connection = source.getConnection(OPERATION_CONTEXT);
        try {
            assertEquals(source.getServerDescription().getAddress(),
                    connection.getDescription().getServerAddress());
            assertEquals(source.getServerDescription().getType(),
                    connection.getDescription().getServerType());
            assertEquals(source.getServerDescription().getMaxDocumentSize(),
                    connection.getDescription().getMaxDocumentSize());
            assertEquals(expectedMaxMessageSize, connection.getDescription().getMaxMessageSize());
            assertEquals(expectedMaxBatchCount, connection.getDescription().getMaxBatchCount());
        } finally {
            connection.release();
            source.release();
        }
    }

    private static BsonDocument getHelloResult() {
        return new CommandReadOperation<>("admin",
                new BsonDocument(LEGACY_HELLO, new BsonInt32(1)),
                new BsonDocumentCodec()).execute(getBinding(), OPERATION_CONTEXT);
    }
}
