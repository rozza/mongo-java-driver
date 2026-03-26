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

package com.mongodb.reactivestreams.client;

import com.mongodb.MongoClientSettings;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerMonitorListener;
import org.bson.Document;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class MongoClientListenerRegistrationTest extends FunctionalSpecification {

    @Disabled
    @Test
    void shouldRegisterEventListeners() {
        ClusterListener clusterListener = mock(ClusterListener.class);
        CommandListener commandListener = mock(CommandListener.class);
        ConnectionPoolListener connectionPoolListener = mock(ConnectionPoolListener.class);
        ServerListener serverListener = mock(ServerListener.class);
        ServerMonitorListener serverMonitorListener = mock(ServerMonitorListener.class);

        MongoClient client = null;
        try {
            MongoClientSettings.Builder builder = Fixture.getMongoClientBuilderFromConnectionString();
            builder.applyToClusterSettings(settings -> settings.addClusterListener(clusterListener))
                    .applyToConnectionPoolSettings(settings -> settings.addConnectionPoolListener(connectionPoolListener))
                    .applyToServerSettings(settings -> {
                        settings.addServerListener(serverListener);
                        settings.heartbeatFrequency(1, TimeUnit.MILLISECONDS);
                        settings.addServerMonitorListener(serverMonitorListener);
                    })
                    .addCommandListener(commandListener);
            MongoClientSettings settings = builder.build();
            client = MongoClients.create(settings);

            Mono.from(client.getDatabase("admin").runCommand(new Document("ping", 1))).block(TIMEOUT_DURATION);

            verify(clusterListener, atLeastOnce()).clusterOpening(any());
            verify(commandListener, atLeastOnce()).commandStarted(any());
            verify(connectionPoolListener, atLeastOnce()).connectionPoolCreated(any());
            verify(serverListener, atLeastOnce()).serverOpening(any());
            verify(serverMonitorListener, atLeastOnce()).serverHearbeatStarted(any());
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    @Test
    void shouldRegisterMultipleCommandListeners() {
        CommandListener first = mock(CommandListener.class);
        CommandListener second = mock(CommandListener.class);

        MongoClient client = null;
        try {
            client = MongoClients.create(Fixture.getMongoClientBuilderFromConnectionString()
                    .addCommandListener(first).addCommandListener(second).build());

            Mono.from(client.getDatabase("admin").runCommand(new Document("ping", 1))).block(TIMEOUT_DURATION);

            verify(first, times(1)).commandStarted(any());
            verify(second, times(1)).commandStarted(any());
            verify(first, times(1)).commandSucceeded(any());
            verify(second, times(1)).commandSucceeded(any());
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
}
