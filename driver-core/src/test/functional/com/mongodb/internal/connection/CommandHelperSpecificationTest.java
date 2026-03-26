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

import com.mongodb.LoggerSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.SocketSettings;
import com.mongodb.internal.connection.netty.NettyStreamFactory;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.ClusterFixture.CLIENT_METADATA;
import static com.mongodb.ClusterFixture.LEGACY_HELLO;
import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ClusterFixture.getClusterConnectionMode;
import static com.mongodb.ClusterFixture.getCredentialWithCache;
import static com.mongodb.ClusterFixture.getPrimary;
import static com.mongodb.ClusterFixture.getServerApi;
import static com.mongodb.ClusterFixture.getSslSettings;
import static com.mongodb.internal.connection.CommandHelper.executeCommandAsync;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommandHelperSpecificationTest {
    private InternalConnection connection;

    @BeforeEach
    void setUp() {
        InternalStreamConnection.setRecordEverything(true);
        connection = new InternalStreamConnectionFactory(ClusterConnectionMode.SINGLE,
                new NettyStreamFactory(SocketSettings.builder().build(), getSslSettings()),
                getCredentialWithCache(), CLIENT_METADATA, Collections.emptyList(),
                LoggerSettings.builder().build(), null, getServerApi())
                .create(new ServerId(new ClusterId(), getPrimary()));
        connection.open(OPERATION_CONTEXT);
    }

    @AfterEach
    void tearDown() {
        InternalStreamConnection.setRecordEverything(false);
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void shouldExecuteCommandAsynchronously() throws InterruptedException {
        AtomicReference<BsonDocument> receivedDocument = new AtomicReference<>();
        AtomicReference<Throwable> receivedException = new AtomicReference<>();

        CountDownLatch latch1 = new CountDownLatch(1);
        executeCommandAsync("admin", new BsonDocument(LEGACY_HELLO, new BsonInt32(1)),
                getClusterConnectionMode(), getServerApi(), connection, OPERATION_CONTEXT,
                (document, exception) -> {
                    receivedDocument.set(document);
                    receivedException.set(exception);
                    latch1.countDown();
                });
        latch1.await();

        assertNull(receivedException.get());
        assertFalse(receivedDocument.get().isEmpty());
        assertTrue(receivedDocument.get().containsKey("ok"));

        CountDownLatch latch2 = new CountDownLatch(1);
        executeCommandAsync("admin", new BsonDocument("non-existent-command", new BsonInt32(1)),
                getClusterConnectionMode(), getServerApi(), connection, OPERATION_CONTEXT,
                (document, exception) -> {
                    receivedDocument.set(document);
                    receivedException.set(exception);
                    latch2.countDown();
                });
        latch2.await();

        assertNull(receivedDocument.get());
        assertInstanceOf(MongoCommandException.class, receivedException.get());
    }
}
