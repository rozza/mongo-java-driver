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
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.connection.Connection;
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
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.internal.operation.OperationUnitSpecification.getMaxWireVersionForServerVersion;
import static com.mongodb.internal.operation.SyncOperationHelper.executeCommand;
import static com.mongodb.internal.operation.SyncOperationHelper.executeRetryableRead;
import static com.mongodb.internal.operation.SyncOperationHelper.executeRetryableWrite;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SyncOperationHelperTest {

    @Test
    void shouldSetReadPreferenceToPrimaryWhenUsingWriteBinding() {
        String dbName = "db";
        BsonDocument command = new BsonDocument();
        @SuppressWarnings("unchecked")
        Decoder<BsonDocument> decoder = mock(Decoder.class);
        Connection connection = mock(Connection.class);
        @SuppressWarnings("unchecked")
        SyncOperationHelper.CommandWriteTransformer<BsonDocument, Object> function = mock(SyncOperationHelper.CommandWriteTransformer.class);
        ConnectionDescription connectionDescription = mock(ConnectionDescription.class);
        when(connection.getDescription()).thenReturn(connectionDescription);
        when(connection.command(eq(dbName), eq(command), any(), eq(primary()), eq(decoder), any())).thenReturn(new BsonDocument());

        ServerDescription serverDescription = mock(ServerDescription.class);
        when(serverDescription.getMinRoundTripTimeNanos()).thenReturn(0L);

        ConnectionSource connectionSource = mock(ConnectionSource.class);
        when(connectionSource.getConnection(any())).thenReturn(connection);
        when(connectionSource.getServerDescription()).thenReturn(serverDescription);

        WriteBinding writeBinding = mock(WriteBinding.class);
        when(writeBinding.getWriteConnectionSource(any())).thenReturn(connectionSource);

        executeCommand(writeBinding, OPERATION_CONTEXT, dbName, command, decoder, function);

        verify(connection).command(eq(dbName), eq(command), any(), eq(primary()), eq(decoder), any());
        verify(connection).release();
    }

    @Test
    void shouldRetryWithRetryableException() {
        SessionContext sessionContext = mock(SessionContext.class);
        when(sessionContext.hasSession()).thenReturn(true);
        when(sessionContext.hasActiveTransaction()).thenReturn(false);
        when(sessionContext.getReadConcern()).thenReturn(ReadConcern.DEFAULT);

        OperationContext operationContext = OPERATION_CONTEXT.withSessionContext(sessionContext);

        String dbName = "db";
        BsonDocument command = BsonDocument.parse(
                "{findAndModify: \"coll\", query: {a: 1}, new: false, update: {$inc: {a :1}}, txnNumber: 1}");

        BsonDocumentCodec decoder = new BsonDocumentCodec();
        Queue<BsonDocument> results = new LinkedList<>(asList(
                BsonDocument.parse("{ok: 1.0, writeConcernError: {code: 91, errmsg: \"Replication is being shut down\"}}"),
                BsonDocument.parse("{ok: 1.0, writeConcernError: {code: -1, errmsg: \"UnknownError\"}}")
        ));

        ConnectionDescription connectionDescription = mock(ConnectionDescription.class);
        when(connectionDescription.getMaxWireVersion()).thenReturn(getMaxWireVersionForServerVersion(asList(4, 0, 0)));
        when(connectionDescription.getServerType()).thenReturn(ServerType.REPLICA_SET_PRIMARY);

        Connection connection = mock(Connection.class);
        when(connection.getDescription()).thenReturn(connectionDescription);
        when(connection.command(eq(dbName), eq(command), any(), eq(primary()), eq(decoder), any()))
                .thenAnswer(invocation -> results.poll());

        ServerDescription serverDescription = mock(ServerDescription.class);
        when(serverDescription.getLogicalSessionTimeoutMinutes()).thenReturn(1);

        ConnectionSource connectionSource = mock(ConnectionSource.class);
        when(connectionSource.getConnection(any())).thenReturn(connection);
        when(connectionSource.getServerDescription()).thenReturn(serverDescription);

        WriteBinding writeBinding = mock(WriteBinding.class);
        when(writeBinding.getWriteConnectionSource(any())).thenReturn(connectionSource);

        MongoWriteConcernException ex = assertThrows(MongoWriteConcernException.class, () ->
                executeRetryableWrite(writeBinding, operationContext, dbName, primary(),
                        NoOpFieldNameValidator.INSTANCE, decoder, (csot, sd, cd) -> command,
                        FindAndModifyHelper.transformer(), cmd -> cmd));
        assertEquals(-1, ex.getWriteConcernError().getCode());
    }

    static Stream<ReadPreference> shouldUseConnectionSourceReadPreference() {
        return Stream.of(primary(), ReadPreference.secondary());
    }

    @ParameterizedTest
    @MethodSource
    void shouldUseConnectionSourceReadPreference(final ReadPreference readPreference) {
        String dbName = "db";
        BsonDocument command = new BsonDocument("fakeCommandName", BsonNull.VALUE);
        @SuppressWarnings("unchecked")
        Decoder<BsonDocument> decoder = mock(Decoder.class);
        @SuppressWarnings("unchecked")
        SyncOperationHelper.CommandReadTransformer<BsonDocument, Object> function =
                mock(SyncOperationHelper.CommandReadTransformer.class);

        ConnectionDescription connectionDescription = mock(ConnectionDescription.class);
        Connection connection = mock(Connection.class);
        when(connection.getDescription()).thenReturn(connectionDescription);
        when(connection.command(eq(dbName), eq(command), any(), eq(readPreference), eq(decoder), any()))
                .thenReturn(new BsonDocument());

        ServerDescription serverDescription = mock(ServerDescription.class);
        when(serverDescription.getMinRoundTripTimeNanos()).thenReturn(0L);

        ConnectionSource connectionSource = mock(ConnectionSource.class);
        when(connectionSource.getConnection(any())).thenReturn(connection);
        when(connectionSource.getReadPreference()).thenReturn(readPreference);
        when(connectionSource.getServerDescription()).thenReturn(serverDescription);

        ReadBinding readBinding = mock(ReadBinding.class);
        when(readBinding.getReadConnectionSource(any())).thenReturn(connectionSource);

        executeRetryableRead(readBinding, OPERATION_CONTEXT, dbName, (csot, sd, cd) -> command, decoder, function, false);

        verify(connection).command(eq(dbName), eq(command), any(), eq(readPreference), eq(decoder), any());
        verify(connection).release();
    }
}
