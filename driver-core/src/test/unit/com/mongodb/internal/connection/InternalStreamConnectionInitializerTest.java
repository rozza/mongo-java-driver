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

package com.mongodb.internal.connection;

import com.mongodb.ServerAddress;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.TimeoutSettings;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.internal.connection.MessageHelper.LEGACY_HELLO;
import static com.mongodb.internal.connection.MessageHelper.buildSuccessfulReply;
import static com.mongodb.internal.connection.MessageHelper.decodeCommand;
import static com.mongodb.internal.connection.OperationContext.simpleOperationContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class InternalStreamConnectionInitializerTest {

    private final ServerId serverId = new ServerId(new ClusterId(), new ServerAddress());
    private final TestInternalConnection internalConnection = new TestInternalConnection(serverId, ServerType.STANDALONE);
    private final OperationContext operationContext = simpleOperationContext(TimeoutSettings.DEFAULT, null);

    @Test
    void shouldCreateCorrectDescription() {
        InternalStreamConnectionInitializer initializer = new InternalStreamConnectionInitializer(SINGLE, null, null,
                Collections.emptyList(), null);

        enqueueSuccessfulReplies(false, 123);
        InternalConnectionInitializationDescription description = initializer.startHandshake(internalConnection, operationContext);
        description = initializer.finishHandshake(internalConnection, description, operationContext);
        ConnectionDescription connectionDescription = description.getConnectionDescription();
        ServerDescription serverDescription = description.getServerDescription();

        assertEquals(getExpectedConnectionDescription(connectionDescription.getConnectionId().getLocalValue(), 123),
                connectionDescription);
        assertEquals(getExpectedServerDescription(serverDescription), serverDescription);
    }

    @Test
    void shouldCreateCorrectDescriptionAsynchronously() {
        InternalStreamConnectionInitializer initializer = new InternalStreamConnectionInitializer(SINGLE, null, null,
                Collections.emptyList(), null);

        enqueueSuccessfulReplies(false, 123);
        FutureResultCallback<InternalConnectionInitializationDescription> futureCallback = new FutureResultCallback<>();
        initializer.startHandshakeAsync(internalConnection, operationContext, futureCallback);
        InternalConnectionInitializationDescription description = futureCallback.get();
        futureCallback = new FutureResultCallback<>();
        initializer.finishHandshakeAsync(internalConnection, description, operationContext, futureCallback);
        description = futureCallback.get();
        ConnectionDescription connectionDescription = description.getConnectionDescription();
        ServerDescription serverDescription = description.getServerDescription();

        assertEquals(getExpectedConnectionDescription(connectionDescription.getConnectionId().getLocalValue(), 123),
                connectionDescription);
        assertEquals(getExpectedServerDescription(serverDescription), serverDescription);
    }

    @Test
    void shouldAuthenticate() {
        Authenticator authenticator = mock(Authenticator.class);
        InternalStreamConnectionInitializer initializer = new InternalStreamConnectionInitializer(SINGLE, authenticator,
                null, Collections.emptyList(), null);

        enqueueSuccessfulReplies(false, 123);
        InternalConnectionInitializationDescription internalDescription = initializer.startHandshake(internalConnection, operationContext);
        ConnectionDescription connectionDescription = initializer.finishHandshake(internalConnection, internalDescription, operationContext)
                .getConnectionDescription();

        assertNotNull(connectionDescription);
        verify(authenticator, times(1)).authenticate(any(), any(), any());
    }

    @Test
    void shouldAuthenticateAsynchronously() {
        Authenticator authenticator = mock(Authenticator.class);
        org.mockito.Mockito.doAnswer(invocation -> {
            ((com.mongodb.internal.async.SingleResultCallback<?>) invocation.getArgument(3)).onResult(null, null);
            return null;
        }).when(authenticator).authenticateAsync(any(), any(), any(), any());

        InternalStreamConnectionInitializer initializer = new InternalStreamConnectionInitializer(SINGLE, authenticator,
                null, Collections.emptyList(), null);

        enqueueSuccessfulReplies(false, 123);
        FutureResultCallback<InternalConnectionInitializationDescription> futureCallback = new FutureResultCallback<>();
        initializer.startHandshakeAsync(internalConnection, operationContext, futureCallback);
        InternalConnectionInitializationDescription description = futureCallback.get();
        futureCallback = new FutureResultCallback<>();
        initializer.finishHandshakeAsync(internalConnection, description, operationContext, futureCallback);
        ConnectionDescription connectionDescription = futureCallback.get().getConnectionDescription();

        assertNotNull(connectionDescription);
        verify(authenticator, times(1)).authenticateAsync(any(), any(), any(), any());
    }

    @Test
    void shouldNotAuthenticateIfServerIsAnArbiter() {
        Authenticator authenticator = mock(Authenticator.class);
        InternalStreamConnectionInitializer initializer = new InternalStreamConnectionInitializer(SINGLE, authenticator,
                null, Collections.emptyList(), null);

        enqueueSuccessfulReplies(true, 123);
        InternalConnectionInitializationDescription internalDescription = initializer.startHandshake(internalConnection, operationContext);
        ConnectionDescription connectionDescription = initializer.finishHandshake(internalConnection, internalDescription, operationContext)
                .getConnectionDescription();

        assertNotNull(connectionDescription);
        verify(authenticator, never()).authenticate(any(), any(), any());
    }

    @Test
    void shouldNotAuthenticateAsynchronouslyIfServerIsAnArbiter() {
        Authenticator authenticator = mock(Authenticator.class);
        InternalStreamConnectionInitializer initializer = new InternalStreamConnectionInitializer(SINGLE, authenticator,
                null, Collections.emptyList(), null);

        enqueueSuccessfulReplies(true, 123);
        FutureResultCallback<InternalConnectionInitializationDescription> futureCallback = new FutureResultCallback<>();
        initializer.startHandshakeAsync(internalConnection, operationContext, futureCallback);
        InternalConnectionInitializationDescription description = futureCallback.get();
        futureCallback = new FutureResultCallback<>();
        initializer.finishHandshakeAsync(internalConnection, description, operationContext, futureCallback);
        futureCallback.get();

        verify(authenticator, never()).authenticateAsync(any(), any(), any(), any());
    }

    static Stream<Object[]> clientMetadataAndAsyncCombinations() {
        List<Object[]> combos = new ArrayList<>();
        for (BsonDocument metadata : new BsonDocument[]{
                ClientMetadataTest.createExpectedClientMetadataDocument("appName"), null}) {
            for (boolean async : new boolean[]{true, false}) {
                combos.add(new Object[]{metadata, async});
            }
        }
        return combos.stream();
    }

    @ParameterizedTest
    @MethodSource("clientMetadataAndAsyncCombinations")
    void shouldAddClientMetadataDocumentToHelloCommand(BsonDocument clientMetadataDocument, boolean async) {
        InternalStreamConnectionInitializer initializer = new InternalStreamConnectionInitializer(SINGLE, null,
                clientMetadataDocument, Collections.emptyList(), null);
        BsonDocument expectedHelloCommandDocument = new BsonDocument(LEGACY_HELLO, new BsonInt32(1))
                .append("helloOk", BsonBoolean.TRUE)
                .append("$db", new BsonString("admin"));
        if (clientMetadataDocument != null) {
            expectedHelloCommandDocument.append("client", clientMetadataDocument);
        }

        enqueueSuccessfulReplies(false, 123);
        if (async) {
            FutureResultCallback<InternalConnectionInitializationDescription> callback = new FutureResultCallback<>();
            initializer.startHandshakeAsync(internalConnection, operationContext, callback);
            InternalConnectionInitializationDescription description = callback.get();
            callback = new FutureResultCallback<>();
            initializer.finishHandshakeAsync(internalConnection, description, operationContext, callback);
            callback.get();
        } else {
            InternalConnectionInitializationDescription internalDescription = initializer.startHandshake(internalConnection,
                    operationContext);
            initializer.finishHandshake(internalConnection, internalDescription, operationContext);
        }

        assertEquals(expectedHelloCommandDocument, decodeCommand(internalConnection.getSent().get(0)));
    }

    private ConnectionDescription getExpectedConnectionDescription(long localValue, long serverValue) {
        return new ConnectionDescription(new ConnectionId(serverId, localValue, serverValue),
                3, ServerType.STANDALONE, 512, 16777216, 33554432, Collections.emptyList());
    }

    private ServerDescription getExpectedServerDescription(ServerDescription actualServerDescription) {
        return ServerDescription.builder()
                .ok(true)
                .address(serverId.getAddress())
                .type(ServerType.STANDALONE)
                .state(ServerConnectionState.CONNECTED)
                .minWireVersion(0)
                .maxWireVersion(3)
                .maxDocumentSize(16777216)
                .roundTripTime(actualServerDescription.getRoundTripTimeNanos(), TimeUnit.NANOSECONDS)
                .lastUpdateTimeNanos(actualServerDescription.getLastUpdateTime(TimeUnit.NANOSECONDS))
                .build();
    }

    private void enqueueSuccessfulReplies(boolean isArbiter, int serverConnectionId) {
        internalConnection.enqueueReply(buildSuccessfulReply(
                "{ok: 1, "
                        + "maxWireVersion: 3,"
                        + "connectionId: " + serverConnectionId
                        + (isArbiter ? ", isreplicaset: true, arbiterOnly: true" : "")
                        + "}"));
    }

    private String encode64(String string) {
        return Base64.getEncoder().encodeToString(string.getBytes(StandardCharsets.UTF_8));
    }
}
