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
import com.mongodb.connection.ServerId;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UsageTrackingConnectionTest {

    private static final ServerId SERVER_ID = new ServerId(new ClusterId(), new ServerAddress());

    @Test
    void generationReturnsWrappedValue() {
        UsageTrackingInternalConnection connection = createConnection();
        assertEquals(0, connection.getGeneration());
    }

    @Test
    void openAtShouldBeSetOnOpen() {
        UsageTrackingInternalConnection connection = createConnection();
        assertEquals(Long.MAX_VALUE, connection.getOpenedAt());

        connection.open(OPERATION_CONTEXT);
        assertTrue(connection.getOpenedAt() <= System.currentTimeMillis());
    }

    @Test
    void openAtShouldBeSetOnOpenAsynchronously() {
        FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<>();
        UsageTrackingInternalConnection connection = createConnection();
        assertEquals(Long.MAX_VALUE, connection.getOpenedAt());

        connection.openAsync(OPERATION_CONTEXT, futureResultCallback);
        futureResultCallback.get();
        assertTrue(connection.getOpenedAt() <= System.currentTimeMillis());
    }

    @Test
    void lastUsedAtShouldBeSetOnOpen() {
        UsageTrackingInternalConnection connection = createConnection();
        assertEquals(Long.MAX_VALUE, connection.getLastUsedAt());

        connection.open(OPERATION_CONTEXT);
        assertTrue(connection.getLastUsedAt() <= System.currentTimeMillis());
    }

    @Test
    void lastUsedAtShouldBeSetOnOpenAsynchronously() {
        FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<>();
        UsageTrackingInternalConnection connection = createConnection();
        assertEquals(Long.MAX_VALUE, connection.getLastUsedAt());

        connection.openAsync(OPERATION_CONTEXT, futureResultCallback);
        futureResultCallback.get();
        assertTrue(connection.getLastUsedAt() <= System.currentTimeMillis());
    }

    @Test
    void lastUsedAtShouldBeSetOnSendMessage() {
        UsageTrackingInternalConnection connection = createConnection();
        connection.open(OPERATION_CONTEXT);
        long openedLastUsedAt = connection.getLastUsedAt();

        connection.sendMessage(Collections.emptyList(), 1, OPERATION_CONTEXT);

        assertTrue(connection.getLastUsedAt() >= openedLastUsedAt);
        assertTrue(connection.getLastUsedAt() <= System.currentTimeMillis());
    }

    @Test
    void lastUsedAtShouldBeSetOnSendMessageAsynchronously() {
        UsageTrackingInternalConnection connection = createConnection();
        connection.open(OPERATION_CONTEXT);
        long openedLastUsedAt = connection.getLastUsedAt();
        FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<>();

        connection.sendMessageAsync(Collections.emptyList(), 1, OPERATION_CONTEXT, futureResultCallback);
        futureResultCallback.get();

        assertTrue(connection.getLastUsedAt() >= openedLastUsedAt);
        assertTrue(connection.getLastUsedAt() <= System.currentTimeMillis());
    }

    @Test
    void lastUsedAtShouldBeSetOnReceiveMessage() {
        UsageTrackingInternalConnection connection = createConnection();
        connection.open(OPERATION_CONTEXT);
        long openedLastUsedAt = connection.getLastUsedAt();

        connection.receiveMessage(1, OPERATION_CONTEXT);

        assertTrue(connection.getLastUsedAt() >= openedLastUsedAt);
        assertTrue(connection.getLastUsedAt() <= System.currentTimeMillis());
    }

    @Test
    void lastUsedAtShouldBeSetOnReceiveMessageAsynchronously() {
        UsageTrackingInternalConnection connection = createConnection();
        connection.open(OPERATION_CONTEXT);
        long openedLastUsedAt = connection.getLastUsedAt();
        FutureResultCallback<ResponseBuffers> futureResultCallback = new FutureResultCallback<>();

        connection.receiveMessageAsync(1, OPERATION_CONTEXT, futureResultCallback);
        futureResultCallback.get();

        assertTrue(connection.getLastUsedAt() >= openedLastUsedAt);
        assertTrue(connection.getLastUsedAt() <= System.currentTimeMillis());
    }

    @Test
    void lastUsedAtShouldBeSetOnSendAndReceive() {
        UsageTrackingInternalConnection connection = createConnection();
        connection.open(OPERATION_CONTEXT);
        long openedLastUsedAt = connection.getLastUsedAt();

        connection.sendAndReceive(new CommandMessage("test",
                new BsonDocument("ping", new BsonInt32(1)), NoOpFieldNameValidator.INSTANCE, primary(),
                MessageSettings.builder().build(), SINGLE, null), new BsonDocumentCodec(), OPERATION_CONTEXT);

        assertTrue(connection.getLastUsedAt() >= openedLastUsedAt);
        assertTrue(connection.getLastUsedAt() <= System.currentTimeMillis());
    }

    @Test
    void lastUsedAtShouldBeSetOnSendAndReceiveAsynchronously() {
        UsageTrackingInternalConnection connection = createConnection();
        connection.open(OPERATION_CONTEXT);
        long openedLastUsedAt = connection.getLastUsedAt();
        FutureResultCallback<BsonDocument> futureResultCallback = new FutureResultCallback<>();

        connection.sendAndReceiveAsync(new CommandMessage("test",
                new BsonDocument("ping", new BsonInt32(1)), NoOpFieldNameValidator.INSTANCE, primary(),
                MessageSettings.builder().build(), SINGLE, null),
                new BsonDocumentCodec(), OPERATION_CONTEXT, futureResultCallback);
        futureResultCallback.get();

        assertTrue(connection.getLastUsedAt() >= openedLastUsedAt);
        assertTrue(connection.getLastUsedAt() <= System.currentTimeMillis());
    }

    private static UsageTrackingInternalConnection createConnection() {
        return new UsageTrackingInternalConnection(new TestInternalConnectionFactory().create(SERVER_ID),
                new DefaultConnectionPool.ServiceStateManager());
    }
}
