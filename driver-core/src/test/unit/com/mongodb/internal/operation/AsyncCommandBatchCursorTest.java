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
import com.mongodb.ServerAddress;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.internal.connection.OperationContext;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CLOSED_AS_CURSOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AsyncCommandBatchCursorTest {

    private static final MongoNamespace NAMESPACE = new MongoNamespace("db", "coll");
    private static final ServerAddress SERVER_ADDRESS = new ServerAddress();
    private static final long CURSOR_ID = 42;
    private static final List<Document> FIRST_BATCH = Arrays.asList(new Document("_id", 1), new Document("_id", 2));
    private static final DocumentCodec CODEC = new DocumentCodec();
    private static final MongoException MONGO_EXCEPTION = new MongoException("error");

    @Test
    void shouldReturnExpectedResultsFromNext() {
        AsyncConnection initialConnection = createMockAsyncConnection();
        AsyncConnection connection = createMockAsyncConnection();
        AsyncConnectionSource connectionSource = createMockAsyncConnectionSource(connection);
        OperationContext operationContext = getOperationContext();

        BsonDocument firstBatch = createCommandResult(FIRST_BATCH, 0);
        AsyncCommandCursor<Document> commandCoreCursor = new AsyncCommandCursor<>(firstBatch, 0, CODEC, null,
                connectionSource, initialConnection);
        AsyncCommandBatchCursor<Document> cursor = new AsyncCommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, 0,
                operationContext, commandCoreCursor);

        assertEquals(FIRST_BATCH, nextBatch(cursor));
        assertTrue(cursor.isClosed());

        // Calling next after cursor is closed (no cursor id) should throw
        assertThrows(IllegalStateException.class, () -> nextBatch(cursor));
    }

    @Test
    void shouldHandleErrorsWhenCallingClose() {
        AsyncConnection initialConnection = createMockAsyncConnection();
        AsyncConnectionSource connectionSource = createMockAsyncConnectionSourceWithError(MONGO_EXCEPTION);

        BsonDocument firstBatch = createCommandResult(FIRST_BATCH, CURSOR_ID);
        AsyncCommandCursor<Document> commandCoreCursor = new AsyncCommandCursor<>(firstBatch, 0, CODEC, null,
                connectionSource, initialConnection);
        AsyncCommandBatchCursor<Document> cursor = new AsyncCommandBatchCursor<>(TimeoutMode.CURSOR_LIFETIME, 0,
                getOperationContext(), commandCoreCursor);

        cursor.close();
        assertTrue(cursor.isClosed());
    }

    private List<Document> nextBatch(final AsyncCommandBatchCursor<Document> cursor) {
        FutureResultCallback<List<Document>> futureResultCallback = new FutureResultCallback<>();
        cursor.next(futureResultCallback);
        try {
            return futureResultCallback.get();
        } catch (Throwable t) {
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            }
            throw new RuntimeException(t);
        }
    }

    private static BsonDocument createCommandResult(final List<Document> results, final long cursorId) {
        return new BsonDocument("ok", new BsonInt32(1))
                .append("cursor",
                        new BsonDocument("ns", new BsonString(NAMESPACE.getFullName()))
                                .append("id", new BsonInt64(cursorId))
                                .append("firstBatch", new BsonArrayWrapper<>(results)));
    }

    private AsyncConnection createMockAsyncConnection() {
        AsyncConnection connection = mock(AsyncConnection.class);
        ConnectionDescription desc = mock(ConnectionDescription.class);
        when(desc.getMaxWireVersion()).thenReturn(6);
        when(desc.getServerAddress()).thenReturn(SERVER_ADDRESS);
        when(desc.getServerType()).thenReturn(ServerType.STANDALONE);
        when(connection.getDescription()).thenReturn(desc);
        when(connection.retain()).thenReturn(connection);
        return connection;
    }

    @SuppressWarnings("unchecked")
    private AsyncConnectionSource createMockAsyncConnectionSource(final AsyncConnection... connections) {
        AsyncConnectionSource connectionSource = mock(AsyncConnectionSource.class);
        when(connectionSource.getServerDescription()).thenReturn(
                ServerDescription.builder()
                        .address(new ServerAddress())
                        .type(ServerType.STANDALONE)
                        .state(ServerConnectionState.CONNECTED)
                        .build());
        if (connections.length > 0) {
            int[] index = {0};
            doAnswer(invocation -> {
                SingleResultCallback<AsyncConnection> cb = invocation.getArgument(1);
                AsyncConnection conn = connections[Math.min(index[0]++, connections.length - 1)];
                cb.onResult(conn.retain(), null);
                return null;
            }).when(connectionSource).getConnection(any(OperationContext.class), any(SingleResultCallback.class));
        }
        when(connectionSource.retain()).thenReturn(connectionSource);
        return connectionSource;
    }

    @SuppressWarnings("unchecked")
    private AsyncConnectionSource createMockAsyncConnectionSourceWithError(final MongoException error) {
        AsyncConnectionSource connectionSource = mock(AsyncConnectionSource.class);
        when(connectionSource.getServerDescription()).thenReturn(
                ServerDescription.builder()
                        .address(new ServerAddress())
                        .type(ServerType.STANDALONE)
                        .state(ServerConnectionState.CONNECTED)
                        .build());
        doAnswer(invocation -> {
            SingleResultCallback<AsyncConnection> cb = invocation.getArgument(1);
            cb.onResult(null, error);
            return null;
        }).when(connectionSource).getConnection(any(OperationContext.class), any(SingleResultCallback.class));
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
