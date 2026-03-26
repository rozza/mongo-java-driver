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

import com.mongodb.event.ClusterListener;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerMonitorListener;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.mongodb.Fixture.getMongoClientURI;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SuppressWarnings("deprecation")
class MongoClientListenerRegistrationTest extends FunctionalSpecification {

    @Test
    @DisplayName("should register event listeners")
    void shouldRegisterEventListeners() {
        ClusterListener clusterListener = mock(ClusterListener.class);
        CommandListener commandListener = mock(CommandListener.class);
        ConnectionPoolListener connectionPoolListener = mock(ConnectionPoolListener.class);
        ServerListener serverListener = mock(ServerListener.class);

        MongoClientOptions.Builder optionBuilder = MongoClientOptions.builder(getMongoClientURI().getOptions())
                .addClusterListener(clusterListener)
                .addCommandListener(commandListener)
                .addConnectionPoolListener(connectionPoolListener)
                .addServerListener(serverListener);
        MongoClient client = new MongoClient(getMongoClientURI(optionBuilder));
        try {
            client.getDatabase("admin").runCommand(new Document("ping", 1));

            verify(clusterListener, atLeastOnce()).clusterOpening(any());
            verify(commandListener, atLeastOnce()).commandStarted(any());
            verify(connectionPoolListener, atLeastOnce()).connectionPoolCreated(any());
            verify(serverListener, atLeastOnce()).serverOpening(any());
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("should register single command listener")
    void shouldRegisterSingleCommandListener() {
        CommandListener first = mock(CommandListener.class);
        MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder(getMongoClientURI().getOptions())
                .addCommandListener(first);
        MongoClient client = new MongoClient(getMongoClientURI(optionsBuilder));
        try {
            client.getDatabase("admin").runCommand(new Document("ping", 1));

            verify(first).commandStarted(any(CommandStartedEvent.class));
            verify(first).commandSucceeded(any(CommandSucceededEvent.class));
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("should register multiple command listeners")
    void shouldRegisterMultipleCommandListeners() {
        CommandListener first = mock(CommandListener.class);
        CommandListener second = mock(CommandListener.class);
        MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder(getMongoClientURI().getOptions())
                .addCommandListener(first)
                .addCommandListener(second);
        MongoClient client = new MongoClient(getMongoClientURI(optionsBuilder));
        try {
            client.getDatabase("admin").runCommand(new Document("ping", 1));

            verify(first).commandStarted(any(CommandStartedEvent.class));
            verify(second).commandStarted(any(CommandStartedEvent.class));
            verify(first).commandSucceeded(any(CommandSucceededEvent.class));
            verify(second).commandSucceeded(any(CommandSucceededEvent.class));
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("should register single listeners for monitor events")
    void shouldRegisterSingleListenersForMonitorEvents() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ClusterListener clusterListener = mock(ClusterListener.class);
        ServerListener serverListener = mock(ServerListener.class);
        ServerMonitorListener serverMonitorListener = mock(ServerMonitorListener.class);

        MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder(getMongoClientURI().getOptions())
                .heartbeatFrequency(1)
                .addClusterListener(clusterListener)
                .addServerListener(serverListener)
                .addServerMonitorListener(new ServerMonitorListener() {
                    @Override
                    public void serverHearbeatStarted(final com.mongodb.event.ServerHeartbeatStartedEvent event) {
                        if (latch.getCount() > 0) {
                            latch.countDown();
                        }
                    }
                });
        MongoClient client = new MongoClient(getMongoClientURI(optionsBuilder));
        try {
            boolean finished = latch.await(5, TimeUnit.SECONDS);
            assertTrue(finished);

            verify(clusterListener).clusterOpening(any());
            verify(serverListener, atLeastOnce()).serverOpening(any());
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("should register multiple listeners for monitor events")
    void shouldRegisterMultipleListenersForMonitorEvents() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        ClusterListener clusterListener = mock(ClusterListener.class);
        ServerListener serverListener = mock(ServerListener.class);
        ClusterListener clusterListenerTwo = mock(ClusterListener.class);
        ServerListener serverListenerTwo = mock(ServerListener.class);

        ServerMonitorListener serverMonitorListener = new ServerMonitorListener() {
            @Override
            public void serverHearbeatStarted(final com.mongodb.event.ServerHeartbeatStartedEvent event) {
                if (latch.getCount() > 0) {
                    latch.countDown();
                }
            }
        };

        MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder(getMongoClientURI().getOptions())
                .heartbeatFrequency(1)
                .addClusterListener(clusterListener)
                .addServerListener(serverListener)
                .addServerMonitorListener(serverMonitorListener)
                .addClusterListener(clusterListenerTwo)
                .addServerListener(serverListenerTwo)
                .addServerMonitorListener(serverMonitorListener);
        MongoClient client = new MongoClient(getMongoClientURI(optionsBuilder));
        try {
            boolean finished = latch.await(5, TimeUnit.SECONDS);
            assertTrue(finished);

            verify(clusterListener).clusterOpening(any());
            verify(clusterListenerTwo).clusterOpening(any());
            verify(serverListener, atLeastOnce()).serverOpening(any());
            verify(serverListenerTwo, atLeastOnce()).serverOpening(any());
        } finally {
            client.close();
        }
    }
}
