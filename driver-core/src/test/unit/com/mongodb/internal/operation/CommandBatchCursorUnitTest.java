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

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.ServerAddress;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.internal.connection.OperationContext;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CLOSED_AS_CURSOR;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CommandBatchCursorUnitTest {

    private static final MongoNamespace NAMESPACE = new MongoNamespace("db", "coll");
    private static final ServerAddress SERVER_ADDRESS = new ServerAddress();
    private static final long CURSOR_ID = 42;
    private static final List<Document> FIRST_BATCH = Arrays.asList(new Document("_id", 1), new Document("_id", 2));
    private static final DocumentCodec CODEC = new DocumentCodec();

    @Test
    void shouldCloseTheCursorWhenNoCursorId() {
        Connection initialConnection = createMockConnection();
        Connection connection = createMockConnection();
        ConnectionSource connectionSource = createMockConnectionSource(connection);

        BsonDocument firstBatch = createCommandResult(FIRST_BATCH, 0);
        CommandCursor<Document> commandCoreCursor = new CommandCursor<>(firstBatch, 0, CODEC, null, connectionSource, initialConnection);
        CommandBatchCursor<Document> cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, 0,
                getOperationContext(), commandCoreCursor);

        cursor.close();
        // Should not throw; cursor with id=0 doesn't need killCursors
    }

    @Test
    void shouldReturnExpectedResultsFromNext() {
        Connection initialConnection = createMockConnection();
        Connection connection = createMockConnection();
        ConnectionSource connectionSource = createMockConnectionSource(connection);

        BsonDocument firstBatch = createCommandResult(FIRST_BATCH, 0);
        CommandCursor<Document> commandCoreCursor = new CommandCursor<>(firstBatch, 0, CODEC, null, connectionSource, initialConnection);
        CommandBatchCursor<Document> cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, 0,
                getOperationContext(), commandCoreCursor);

        assertEquals(FIRST_BATCH, cursor.next());
    }

    @Test
    void shouldHandleExceptionsWhenClosing() {
        Connection initialConnection = createMockConnection();
        Connection connection = mock(Connection.class);
        ConnectionDescription connDesc = mock(ConnectionDescription.class);
        when(connDesc.getMaxWireVersion()).thenReturn(4);
        when(connection.getDescription()).thenReturn(connDesc);
        when(connection.command(any(), any(), any(), any(), any(), any()))
                .thenThrow(new MongoSocketException("No MongoD", SERVER_ADDRESS));

        ConnectionSource connectionSource = createMockConnectionSource(connection);

        BsonDocument initialResults = createCommandResult(Collections.emptyList(), CURSOR_ID);
        CommandCursor<Document> commandCoreCursor = new CommandCursor<>(initialResults, 2, CODEC, null,
                connectionSource, initialConnection);
        CommandBatchCursor<Document> cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, 100,
                getOperationContext(), commandCoreCursor);

        assertDoesNotThrow(cursor::close);
        assertDoesNotThrow(cursor::close);
    }

    @Test
    void shouldHandleExceptionsWhenKillingCursorAndConnectionCanNotBeObtained() {
        Connection initialConnection = createMockConnection();
        Connection connection = createMockConnection();
        ConnectionSource connectionSource = createMockConnectionSource(connection);
        when(connectionSource.getConnection(any()))
                .thenThrow(new MongoSocketOpenException("can't open socket", SERVER_ADDRESS, new IOException()));

        BsonDocument initialResults = createCommandResult(Collections.emptyList(), CURSOR_ID);
        CommandCursor<Document> commandCoreCursor = new CommandCursor<>(initialResults, 2, CODEC, null,
                connectionSource, initialConnection);
        CommandBatchCursor<Document> cursor = new CommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, 100,
                getOperationContext(), commandCoreCursor);

        assertDoesNotThrow(cursor::close);
        assertDoesNotThrow(cursor::close);
    }

    private static BsonDocument createCommandResult(final List<Document> results, final long cursorId) {
        return new BsonDocument("ok", new BsonInt32(1))
                .append("cursor",
                        new BsonDocument("ns", new BsonString(NAMESPACE.getFullName()))
                                .append("id", new BsonInt64(cursorId))
                                .append("firstBatch", new BsonArrayWrapper<>(results)));
    }

    private Connection createMockConnection() {
        Connection connection = mock(Connection.class);
        ConnectionDescription desc = mock(ConnectionDescription.class);
        when(desc.getMaxWireVersion()).thenReturn(6);
        when(desc.getServerType()).thenReturn(ServerType.STANDALONE);
        when(connection.getDescription()).thenReturn(desc);
        when(connection.retain()).thenReturn(connection);
        return connection;
    }

    private ConnectionSource createMockConnectionSource(final Connection... connections) {
        ConnectionSource connectionSource = mock(ConnectionSource.class);
        when(connectionSource.getServerDescription()).thenReturn(
                ServerDescription.builder()
                        .address(new ServerAddress())
                        .type(ServerType.STANDALONE)
                        .state(ServerConnectionState.CONNECTED)
                        .build());
        if (connections.length == 1) {
            when(connectionSource.getConnection(any())).thenReturn(connections[0]);
        }
        when(connectionSource.retain()).thenReturn(connectionSource);
        return connectionSource;
    }

    private OperationContext getOperationContext() {
        TimeoutContext timeoutContext = new TimeoutContext(TimeoutSettings.create(
                MongoClientSettings.builder().timeout(3, TimeUnit.SECONDS).build()));
        return new OperationContext(
                IgnorableRequestContext.INSTANCE,
                NoOpSessionContext.INSTANCE,
                timeoutContext, null);
    }
}
