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

import com.mongodb.MongoWriteConcernException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableWriteAsync;
import static com.mongodb.internal.operation.OperationUnitSpecification.getMaxWireVersionForServerVersion;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AsyncOperationHelperTest {

    @SuppressWarnings("unchecked")
    @Test
    void shouldRetryWithRetryableExceptionAsync() {
        String dbName = "db";
        BsonDocument command = BsonDocument.parse(
                "{findAndModify: \"coll\", query: {a: 1}, new: false, update: {$inc: {a :1}}, txnNumber: 1}");

        ConnectionDescription connectionDescription = mock(ConnectionDescription.class);
        when(connectionDescription.getMaxWireVersion()).thenReturn(getMaxWireVersionForServerVersion(asList(4, 0, 0)));
        when(connectionDescription.getServerType()).thenReturn(ServerType.REPLICA_SET_PRIMARY);

        BsonDocumentCodec decoder = new BsonDocumentCodec();
        Queue<BsonDocument> results = new LinkedList<>(asList(
                BsonDocument.parse("{ok: 1.0, writeConcernError: {code: 91, errmsg: \"Replication is being shut down\"}}"),
                BsonDocument.parse("{ok: 1.0, writeConcernError: {code: -1, errmsg: \"UnknownError\"}}")
        ));

        AsyncConnection connection = mock(AsyncConnection.class);
        when(connection.getDescription()).thenReturn(connectionDescription);
        doAnswer(invocation -> {
            SingleResultCallback<BsonDocument> cb = invocation.getArgument(invocation.getArguments().length - 1);
            cb.onResult(results.poll(), null);
            return null;
        }).when(connection).commandAsync(eq(dbName), eq(command), any(), eq(primary()), eq(decoder), any(), any());

        SessionContext sessionContext = mock(SessionContext.class);
        when(sessionContext.hasSession()).thenReturn(true);
        when(sessionContext.hasActiveTransaction()).thenReturn(false);
        when(sessionContext.getReadConcern()).thenReturn(ReadConcern.DEFAULT);

        OperationContext operationContext = OPERATION_CONTEXT.withSessionContext(sessionContext);

        ServerDescription serverDescription = mock(ServerDescription.class);
        when(serverDescription.getLogicalSessionTimeoutMinutes()).thenReturn(1);
        when(serverDescription.getMinRoundTripTimeNanos()).thenReturn(0L);

        AsyncConnectionSource connectionSource = mock(AsyncConnectionSource.class);
        doAnswer(invocation -> {
            SingleResultCallback<AsyncConnection> cb = invocation.getArgument(1);
            cb.onResult(connection, null);
            return null;
        }).when(connectionSource).getConnection(any(OperationContext.class), any(SingleResultCallback.class));
        when(connectionSource.getServerDescription()).thenReturn(serverDescription);

        AsyncWriteBinding asyncWriteBinding = mock(AsyncWriteBinding.class);
        doAnswer(invocation -> {
            SingleResultCallback<AsyncConnectionSource> cb = invocation.getArgument(1);
            cb.onResult(connectionSource, null);
            return null;
        }).when(asyncWriteBinding).getWriteConnectionSource(any(OperationContext.class), any(SingleResultCallback.class));

        AtomicReference<Object> resultRef = new AtomicReference<>();
        AtomicReference<Throwable> throwableRef = new AtomicReference<>();

        executeRetryableWriteAsync(asyncWriteBinding, operationContext, dbName, primary(),
                NoOpFieldNameValidator.INSTANCE, decoder, (csot, sd, cd) -> command,
                FindAndModifyHelper.asyncTransformer(), cmd -> cmd,
                (result, t) -> {
                    resultRef.set(result);
                    throwableRef.set(t);
                });

        assertInstanceOf(MongoWriteConcernException.class, throwableRef.get());
        assertEquals(-1, ((MongoWriteConcernException) throwableRef.get()).getWriteConcernError().getCode());
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldSetReadPreferenceToPrimaryWhenUsingAsyncWriteBinding() {
        String dbName = "db";
        BsonDocument command = new BsonDocument();
        SingleResultCallback<Object> callback = mock(SingleResultCallback.class);

        ConnectionDescription connectionDescription = mock(ConnectionDescription.class);
        AsyncConnection connection = mock(AsyncConnection.class);
        when(connection.getDescription()).thenReturn(connectionDescription);
        doAnswer(invocation -> {
            SingleResultCallback<BsonDocument> cb = invocation.getArgument(invocation.getArguments().length - 1);
            cb.onResult(new BsonDocument(), null);
            return null;
        }).when(connection).commandAsync(eq(dbName), eq(command), any(), eq(primary()), any(), any(), any());

        ServerDescription serverDescription = mock(ServerDescription.class);
        when(serverDescription.getMinRoundTripTimeNanos()).thenReturn(0L);

        AsyncConnectionSource connectionSource = mock(AsyncConnectionSource.class);
        doAnswer(invocation -> {
            SingleResultCallback<AsyncConnection> cb = invocation.getArgument(1);
            cb.onResult(connection, null);
            return null;
        }).when(connectionSource).getConnection(any(OperationContext.class), any(SingleResultCallback.class));
        when(connectionSource.getServerDescription()).thenReturn(serverDescription);

        AsyncWriteBinding asyncWriteBinding = mock(AsyncWriteBinding.class);
        doAnswer(invocation -> {
            SingleResultCallback<AsyncConnectionSource> cb = invocation.getArgument(1);
            cb.onResult(connectionSource, null);
            return null;
        }).when(asyncWriteBinding).getWriteConnectionSource(any(OperationContext.class), any(SingleResultCallback.class));

        executeCommandAsync(asyncWriteBinding, OPERATION_CONTEXT, dbName, command, connection, (t, conn) -> t, callback);

        verify(connection).commandAsync(eq(dbName), eq(command), any(), eq(primary()), any(), any(), any());
    }

    static Stream<ReadPreference> shouldUseAsyncConnectionSourceReadPreference() {
        return Stream.of(primary(), ReadPreference.secondary());
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource
    void shouldUseAsyncConnectionSourceReadPreference(final ReadPreference readPreference) {
        String dbName = "db";
        BsonDocument command = new BsonDocument("fakeCommandName", BsonNull.VALUE);
        Decoder<BsonDocument> decoder = mock(Decoder.class);
        SingleResultCallback<Object> callback = mock(SingleResultCallback.class);
        AsyncOperationHelper.CommandReadTransformerAsync<BsonDocument, Object> function =
                mock(AsyncOperationHelper.CommandReadTransformerAsync.class);

        ConnectionDescription connectionDescription = mock(ConnectionDescription.class);
        AsyncConnection connection = mock(AsyncConnection.class);
        when(connection.getDescription()).thenReturn(connectionDescription);
        doAnswer(invocation -> {
            SingleResultCallback<BsonDocument> cb = invocation.getArgument(invocation.getArguments().length - 1);
            cb.onResult(new BsonDocument(), null);
            return null;
        }).when(connection).commandAsync(eq(dbName), eq(command), any(), eq(readPreference), eq(decoder), any(), any());

        ServerDescription serverDescription = mock(ServerDescription.class);
        when(serverDescription.getMinRoundTripTimeNanos()).thenReturn(0L);

        AsyncConnectionSource connectionSource = mock(AsyncConnectionSource.class);
        doAnswer(invocation -> {
            SingleResultCallback<AsyncConnection> cb = invocation.getArgument(1);
            cb.onResult(connection, null);
            return null;
        }).when(connectionSource).getConnection(any(OperationContext.class), any(SingleResultCallback.class));
        when(connectionSource.getReadPreference()).thenReturn(readPreference);
        when(connectionSource.getServerDescription()).thenReturn(serverDescription);

        AsyncReadBinding asyncReadBinding = mock(AsyncReadBinding.class);
        doAnswer(invocation -> {
            SingleResultCallback<AsyncConnectionSource> cb = invocation.getArgument(1);
            cb.onResult(connectionSource, null);
            return null;
        }).when(asyncReadBinding).getReadConnectionSource(any(OperationContext.class), any(SingleResultCallback.class));

        executeRetryableReadAsync(asyncReadBinding, OPERATION_CONTEXT, dbName, (csot, sd, cd) -> command, decoder, function, false,
                callback);

        verify(connection).commandAsync(eq(dbName), eq(command), any(), eq(readPreference), eq(decoder), any(), any());
        verify(connection).release();
    }
}
