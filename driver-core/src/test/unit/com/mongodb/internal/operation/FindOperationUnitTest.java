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
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.session.SessionContext;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.CursorType.TailableAwait;
import static com.mongodb.internal.operation.OperationUnitSpecification.getMaxWireVersionForServerVersion;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FindOperationUnitTest {

    private static final MongoNamespace NAMESPACE = new MongoNamespace("db", "coll");
    private static final BsonDocument COMMAND_RESULT = new BsonDocument("cursor", new BsonDocument("id", new BsonInt64(0))
            .append("ns", new BsonString("db.coll"))
            .append("firstBatch", new BsonArrayWrapper<>(Collections.emptyList())));

    @Test
    void shouldFindWithCorrectCommandSync() {
        FindOperation<BsonDocument> operation = new FindOperation<>(NAMESPACE, new BsonDocumentCodec());
        BsonDocument expectedCommand = new BsonDocument("find", new BsonString(NAMESPACE.getCollectionName()));

        testSyncOperation(operation, asList(3, 2, 0), expectedCommand, COMMAND_RESULT);
    }

    @Test
    void shouldFindWithCorrectCommandAsync() {
        FindOperation<BsonDocument> operation = new FindOperation<>(NAMESPACE, new BsonDocumentCodec());
        BsonDocument expectedCommand = new BsonDocument("find", new BsonString(NAMESPACE.getCollectionName()));

        testAsyncOperation(operation, asList(3, 2, 0), expectedCommand, COMMAND_RESULT);
    }

    @Test
    void shouldFindWithOverridesSync() {
        FindOperation<BsonDocument> operation = new FindOperation<>(NAMESPACE, new BsonDocumentCodec())
                .filter(new BsonDocument("a", BsonBoolean.TRUE))
                .projection(new BsonDocument("x", new BsonInt32(1)))
                .skip(2)
                .limit(100)
                .batchSize(10)
                .cursorType(TailableAwait)
                .noCursorTimeout(true)
                .partial(true)
                .comment(new BsonString("my comment"))
                .hint(BsonDocument.parse("{ hint : 1}"))
                .min(BsonDocument.parse("{ abc: 99 }"))
                .max(BsonDocument.parse("{ abc: 1000 }"))
                .returnKey(true)
                .showRecordId(true);

        BsonDocument expectedCommand = new BsonDocument("find", new BsonString(NAMESPACE.getCollectionName()))
                .append("filter", operation.getFilter())
                .append("projection", operation.getProjection())
                .append("skip", new BsonInt32(operation.getSkip()))
                .append("tailable", BsonBoolean.TRUE)
                .append("awaitData", BsonBoolean.TRUE)
                .append("allowPartialResults", BsonBoolean.TRUE)
                .append("noCursorTimeout", BsonBoolean.TRUE)
                .append("comment", operation.getComment())
                .append("hint", operation.getHint())
                .append("min", operation.getMin())
                .append("max", operation.getMax())
                .append("returnKey", BsonBoolean.TRUE)
                .append("showRecordId", BsonBoolean.TRUE)
                .append("limit", new BsonInt32(100))
                .append("batchSize", new BsonInt32(10));

        testSyncOperation(operation, asList(3, 2, 0), expectedCommand, COMMAND_RESULT);
    }

    @Test
    void shouldFindWithOverridesAsync() {
        FindOperation<BsonDocument> operation = new FindOperation<>(NAMESPACE, new BsonDocumentCodec())
                .filter(new BsonDocument("a", BsonBoolean.TRUE))
                .projection(new BsonDocument("x", new BsonInt32(1)))
                .skip(2)
                .limit(100)
                .batchSize(10)
                .cursorType(TailableAwait)
                .noCursorTimeout(true)
                .partial(true)
                .comment(new BsonString("my comment"))
                .hint(BsonDocument.parse("{ hint : 1}"))
                .min(BsonDocument.parse("{ abc: 99 }"))
                .max(BsonDocument.parse("{ abc: 1000 }"))
                .returnKey(true)
                .showRecordId(true);

        BsonDocument expectedCommand = new BsonDocument("find", new BsonString(NAMESPACE.getCollectionName()))
                .append("filter", operation.getFilter())
                .append("projection", operation.getProjection())
                .append("skip", new BsonInt32(operation.getSkip()))
                .append("tailable", BsonBoolean.TRUE)
                .append("awaitData", BsonBoolean.TRUE)
                .append("allowPartialResults", BsonBoolean.TRUE)
                .append("noCursorTimeout", BsonBoolean.TRUE)
                .append("comment", operation.getComment())
                .append("hint", operation.getHint())
                .append("min", operation.getMin())
                .append("max", operation.getMax())
                .append("returnKey", BsonBoolean.TRUE)
                .append("showRecordId", BsonBoolean.TRUE)
                .append("limit", new BsonInt32(100))
                .append("batchSize", new BsonInt32(10));

        testAsyncOperation(operation, asList(3, 2, 0), expectedCommand, COMMAND_RESULT);
    }

    @Test
    void shouldFindWithNegativeLimitSync() {
        FindOperation<BsonDocument> operation = new FindOperation<>(NAMESPACE, new BsonDocumentCodec())
                .limit(-100)
                .batchSize(10);

        BsonDocument expectedCommand = new BsonDocument("find", new BsonString(NAMESPACE.getCollectionName()))
                .append("limit", new BsonInt32(100))
                .append("singleBatch", BsonBoolean.TRUE);

        testSyncOperation(operation, asList(3, 2, 0), expectedCommand, COMMAND_RESULT);
    }

    @Test
    void shouldFindWithNegativeBatchSizeSync() {
        FindOperation<BsonDocument> operation = new FindOperation<>(NAMESPACE, new BsonDocumentCodec())
                .limit(100)
                .batchSize(-10);

        BsonDocument expectedCommand = new BsonDocument("find", new BsonString(NAMESPACE.getCollectionName()))
                .append("limit", new BsonInt32(10))
                .append("singleBatch", BsonBoolean.TRUE);

        testSyncOperation(operation, asList(3, 2, 0), expectedCommand, COMMAND_RESULT);
    }

    @Test
    void shouldFindWithAllowDiskUseSync() {
        FindOperation<BsonDocument> operation = new FindOperation<>(NAMESPACE, new BsonDocumentCodec())
                .allowDiskUse(true);

        BsonDocument expectedCommand = new BsonDocument("find", new BsonString(NAMESPACE.getCollectionName()))
                .append("allowDiskUse", new BsonBoolean(true));

        testSyncOperation(operation, asList(3, 4, 0), expectedCommand, COMMAND_RESULT);
    }

    @Test
    void shouldUseReadPreferenceToSetSecondaryOkForCommandsSync() {
        FindOperation<BsonDocument> operation = new FindOperation<>(NAMESPACE, new BsonDocumentCodec());
        testSyncOperationSecondaryOk(operation, asList(3, 2, 0), ReadPreference.secondary(), COMMAND_RESULT);
    }

    @Test
    void shouldUseReadPreferenceToSetSecondaryOkForCommandsAsync() {
        FindOperation<BsonDocument> operation = new FindOperation<>(NAMESPACE, new BsonDocumentCodec());
        testAsyncOperationSecondaryOk(operation, asList(3, 2, 0), ReadPreference.secondary(), COMMAND_RESULT);
    }

    private void testSyncOperation(final ReadOperation<?, ?> operation, final java.util.List<Integer> serverVersion,
                                   final BsonDocument expectedCommand, final BsonDocument result) {
        OperationContext operationContext = OPERATION_CONTEXT
                .withSessionContext(createSessionContext());

        Connection connection = mock(Connection.class);
        ConnectionDescription connectionDescription = mock(ConnectionDescription.class);
        when(connectionDescription.getMaxWireVersion()).thenReturn(getMaxWireVersionForServerVersion(serverVersion));
        when(connection.getDescription()).thenReturn(connectionDescription);
        when(connection.command(any(), eq(expectedCommand), any(), any(), any(), any())).thenReturn(result);

        com.mongodb.connection.ServerDescription serverDescription = mock(com.mongodb.connection.ServerDescription.class);
        when(serverDescription.getMinRoundTripTimeNanos()).thenReturn(0L);

        ConnectionSource connectionSource = mock(ConnectionSource.class);
        when(connectionSource.getConnection(any())).thenReturn(connection);
        when(connectionSource.getReadPreference()).thenReturn(ReadPreference.primary());
        when(connectionSource.getServerDescription()).thenReturn(serverDescription);

        ReadBinding readBinding = mock(ReadBinding.class);
        when(readBinding.getReadConnectionSource(any())).thenReturn(connectionSource);
        when(readBinding.getReadPreference()).thenReturn(ReadPreference.primary());

        operation.execute(readBinding, operationContext);

        verify(connection).command(any(), eq(expectedCommand), any(), any(), any(), any());
        verify(connection).release();
    }

    private void testAsyncOperation(final ReadOperation<?, ?> operation, final java.util.List<Integer> serverVersion,
                                    final BsonDocument expectedCommand, final BsonDocument result) {
        OperationContext operationContext = OPERATION_CONTEXT
                .withSessionContext(createSessionContext());

        AsyncConnection asyncConnection = mock(AsyncConnection.class);
        ConnectionDescription connectionDescription = mock(ConnectionDescription.class);
        when(connectionDescription.getMaxWireVersion()).thenReturn(getMaxWireVersionForServerVersion(serverVersion));
        when(asyncConnection.getDescription()).thenReturn(connectionDescription);
        doAnswer(invocation -> {
            SingleResultCallback<BsonDocument> cb = invocation.getArgument(invocation.getArguments().length - 1);
            cb.onResult(result, null);
            return null;
        }).when(asyncConnection).commandAsync(any(), eq(expectedCommand), any(), any(), any(), any(), any());

        com.mongodb.connection.ServerDescription serverDescription = mock(com.mongodb.connection.ServerDescription.class);
        when(serverDescription.getMinRoundTripTimeNanos()).thenReturn(0L);

        AsyncConnectionSource connectionSource = mock(AsyncConnectionSource.class);
        doAnswer(invocation -> {
            SingleResultCallback<AsyncConnection> cb = invocation.getArgument(1);
            cb.onResult(asyncConnection, null);
            return null;
        }).when(connectionSource).getConnection(any(OperationContext.class), any(SingleResultCallback.class));
        when(connectionSource.getReadPreference()).thenReturn(ReadPreference.primary());
        when(connectionSource.getServerDescription()).thenReturn(serverDescription);

        AsyncReadBinding readBinding = mock(AsyncReadBinding.class);
        doAnswer(invocation -> {
            SingleResultCallback<AsyncConnectionSource> cb = invocation.getArgument(1);
            cb.onResult(connectionSource, null);
            return null;
        }).when(readBinding).getReadConnectionSource(any(OperationContext.class), any(SingleResultCallback.class));
        when(readBinding.getReadPreference()).thenReturn(ReadPreference.primary());

        FutureResultCallback<Object> callback = new FutureResultCallback<>();
        ((ReadOperation<Object, Object>) operation).executeAsync(readBinding, operationContext, callback);
        try {
            callback.get(1000, TimeUnit.MILLISECONDS);
        } catch (Throwable ignored) {
            // expected for some tests
        }

        verify(asyncConnection).release();
    }

    private void testSyncOperationSecondaryOk(final ReadOperation<?, ?> operation, final java.util.List<Integer> serverVersion,
                                              final ReadPreference readPreference, final BsonDocument result) {
        OperationContext operationContext = OPERATION_CONTEXT
                .withSessionContext(createSessionContext());

        Connection connection = mock(Connection.class);
        ConnectionDescription connectionDescription = mock(ConnectionDescription.class);
        when(connectionDescription.getMaxWireVersion()).thenReturn(getMaxWireVersionForServerVersion(serverVersion));
        when(connection.getDescription()).thenReturn(connectionDescription);
        when(connection.command(any(), any(), any(), eq(readPreference), any(), any())).thenReturn(result);

        com.mongodb.connection.ServerDescription serverDescription = mock(com.mongodb.connection.ServerDescription.class);
        when(serverDescription.getMinRoundTripTimeNanos()).thenReturn(0L);

        ConnectionSource connectionSource = mock(ConnectionSource.class);
        when(connectionSource.getConnection(any())).thenReturn(connection);
        when(connectionSource.getReadPreference()).thenReturn(readPreference);
        when(connectionSource.getServerDescription()).thenReturn(serverDescription);

        ReadBinding readBinding = mock(ReadBinding.class);
        when(readBinding.getReadConnectionSource(any())).thenReturn(connectionSource);
        when(readBinding.getReadPreference()).thenReturn(readPreference);

        operation.execute(readBinding, operationContext);

        verify(connection).command(any(), any(), any(), eq(readPreference), any(), any());
        verify(connection).release();
    }

    private void testAsyncOperationSecondaryOk(final ReadOperation<?, ?> operation, final java.util.List<Integer> serverVersion,
                                               final ReadPreference readPreference, final BsonDocument result) {
        OperationContext operationContext = OPERATION_CONTEXT
                .withSessionContext(createSessionContext());

        AsyncConnection asyncConnection = mock(AsyncConnection.class);
        ConnectionDescription connectionDescription = mock(ConnectionDescription.class);
        when(connectionDescription.getMaxWireVersion()).thenReturn(getMaxWireVersionForServerVersion(serverVersion));
        when(asyncConnection.getDescription()).thenReturn(connectionDescription);
        doAnswer(invocation -> {
            SingleResultCallback<BsonDocument> cb = invocation.getArgument(invocation.getArguments().length - 1);
            cb.onResult(result, null);
            return null;
        }).when(asyncConnection).commandAsync(any(), any(), any(), eq(readPreference), any(), any(), any());

        com.mongodb.connection.ServerDescription serverDescription = mock(com.mongodb.connection.ServerDescription.class);
        when(serverDescription.getMinRoundTripTimeNanos()).thenReturn(0L);

        AsyncConnectionSource connectionSource = mock(AsyncConnectionSource.class);
        doAnswer(invocation -> {
            SingleResultCallback<AsyncConnection> cb = invocation.getArgument(1);
            cb.onResult(asyncConnection, null);
            return null;
        }).when(connectionSource).getConnection(any(OperationContext.class), any(SingleResultCallback.class));
        when(connectionSource.getReadPreference()).thenReturn(readPreference);
        when(connectionSource.getServerDescription()).thenReturn(serverDescription);

        AsyncReadBinding readBinding = mock(AsyncReadBinding.class);
        doAnswer(invocation -> {
            SingleResultCallback<AsyncConnectionSource> cb = invocation.getArgument(1);
            cb.onResult(connectionSource, null);
            return null;
        }).when(readBinding).getReadConnectionSource(any(OperationContext.class), any(SingleResultCallback.class));
        when(readBinding.getReadPreference()).thenReturn(readPreference);

        FutureResultCallback<Object> callback = new FutureResultCallback<>();
        ((ReadOperation<Object, Object>) operation).executeAsync(readBinding, operationContext, callback);
        try {
            callback.get(1000, TimeUnit.MILLISECONDS);
        } catch (Throwable ignored) {
        }

        verify(asyncConnection).release();
    }

    private SessionContext createSessionContext() {
        SessionContext sessionContext = mock(SessionContext.class);
        when(sessionContext.hasActiveTransaction()).thenReturn(false);
        when(sessionContext.getReadConcern()).thenReturn(ReadConcern.DEFAULT);
        return sessionContext;
    }
}
