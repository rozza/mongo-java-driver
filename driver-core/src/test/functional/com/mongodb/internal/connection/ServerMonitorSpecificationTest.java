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
import com.mongodb.MongoSocketException;
import com.mongodb.OperationFunctionalSpecification;
import com.mongodb.ServerAddress;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.ServerType;
import com.mongodb.connection.SocketSettings;
import com.mongodb.internal.inject.SameObjectProvider;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.ClusterFixture.CLIENT_METADATA;
import static com.mongodb.ClusterFixture.OPERATION_CONTEXT_FACTORY;
import static com.mongodb.ClusterFixture.getClusterConnectionMode;
import static com.mongodb.ClusterFixture.getCredentialWithCache;
import static com.mongodb.ClusterFixture.getPrimary;
import static com.mongodb.ClusterFixture.getServerApi;
import static com.mongodb.ClusterFixture.getSslSettings;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.internal.connection.DefaultServerMonitor.shouldLogStageChange;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerMonitorSpecificationTest extends OperationFunctionalSpecification {

    private final AtomicReference<ServerDescription> newDescription = new AtomicReference<>();
    private ServerMonitor serverMonitor;
    private final CountDownLatch latch = new CountDownLatch(1);

    @AfterEach
    public void cleanup() {
        if (serverMonitor != null) {
            serverMonitor.close();
        }
    }

    @Test
    void shouldHavePositiveRoundTripTime() throws InterruptedException {
        initializeServerMonitor(getPrimary());
        latch.await();
        assertTrue(newDescription.get().getRoundTripTimeNanos() > 0);
    }

    @Test
    void shouldReportCurrentException() throws InterruptedException {
        initializeServerMonitor(new ServerAddress("some_unknown_server_name:34567"));
        latch.await();
        assertInstanceOf(MongoSocketException.class, newDescription.get().getException());
    }

    @Test
    void shouldLogStateChangeIfSignificantPropertiesHaveChanged() {
        ServerDescription description = createBuilder().build();

        // Same description should not log
        assertFalse(shouldLogStageChange(description, createBuilder().build()));

        // Changed address
        assertTrue(shouldLogStageChange(description,
                createBuilder().address(new ServerAddress("localhost:27018")).build()));

        // Changed type
        assertTrue(shouldLogStageChange(description,
                createBuilder().type(ServerType.STANDALONE).build()));

        // Changed tagSet
        assertTrue(shouldLogStageChange(description,
                createBuilder().tagSet(null).build()));

        // Changed setName
        assertTrue(shouldLogStageChange(description,
                createBuilder().setName("test2").build()));

        // Changed primary
        assertTrue(shouldLogStageChange(description,
                createBuilder().primary("localhost:27018").build()));

        // Changed canonicalAddress
        assertTrue(shouldLogStageChange(description,
                createBuilder().canonicalAddress("localhost:27018").build()));

        // Changed hosts
        assertTrue(shouldLogStageChange(description,
                createBuilder().hosts(new HashSet<>(Arrays.asList("localhost:27018"))).build()));

        // Changed arbiters
        assertTrue(shouldLogStageChange(description,
                createBuilder().arbiters(new HashSet<>(Arrays.asList("localhost:27018"))).build()));

        // Changed passives
        assertTrue(shouldLogStageChange(description,
                createBuilder().passives(new HashSet<>(Arrays.asList("localhost:27018"))).build()));

        // Changed ok
        assertTrue(shouldLogStageChange(description,
                createBuilder().ok(false).build()));

        // Changed state
        assertTrue(shouldLogStageChange(description,
                createBuilder().state(CONNECTING).build()));

        // Changed electionId
        assertTrue(shouldLogStageChange(description,
                createBuilder().electionId(new ObjectId()).build()));

        // Changed setVersion
        assertTrue(shouldLogStageChange(description,
                createBuilder().setVersion(3).build()));

        // Exception state changes
        assertTrue(shouldLogStageChange(
                createBuilder().exception(new IOException()).build(),
                createBuilder().exception(new RuntimeException()).build()));
        assertTrue(shouldLogStageChange(
                createBuilder().exception(new IOException("message one")).build(),
                createBuilder().exception(new IOException("message two")).build()));
    }

    private static ServerDescription.Builder createBuilder() {
        return ServerDescription.builder()
                .ok(true)
                .state(CONNECTED)
                .address(new ServerAddress())
                .type(ServerType.SHARD_ROUTER)
                .tagSet(new TagSet(Arrays.asList(new Tag("dc", "ny"))))
                .setName("test")
                .primary("localhost:27017")
                .canonicalAddress("localhost:27017")
                .hosts(new HashSet<>(Arrays.asList("localhost:27017", "localhost:27018")))
                .passives(new HashSet<>(Arrays.asList("localhost:27019")))
                .arbiters(new HashSet<>(Arrays.asList("localhost:27020")))
                .electionId(new ObjectId("abcdabcdabcdabcdabcdabcd"))
                .setVersion(2);
    }

    private void initializeServerMonitor(final ServerAddress address) {
        SdamServerDescriptionManager sdam = new SdamServerDescriptionManager() {
            @Override
            public void monitorUpdate(final ServerDescription candidateDescription) {
                newDescription.set(candidateDescription);
                latch.countDown();
            }

            @Override
            public void updateToUnknown(final ServerDescription candidateDescription) {
                newDescription.set(candidateDescription);
                latch.countDown();
            }

            @Override
            public void handleExceptionBeforeHandshake(final SdamIssue sdamIssue) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void handleExceptionAfterHandshake(final SdamIssue sdamIssue) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SdamIssue.Context context() {
                throw new UnsupportedOperationException();
            }

            @Override
            public SdamIssue.Context context(final InternalConnection connection) {
                throw new UnsupportedOperationException();
            }
        };
        serverMonitor = new DefaultServerMonitor(new ServerId(new ClusterId(), address),
                ServerSettings.builder().build(),
                new InternalStreamConnectionFactory(SINGLE,
                        new SocketStreamFactory(new DefaultInetAddressResolver(),
                                SocketSettings.builder().connectTimeout(500, TimeUnit.MILLISECONDS).build(),
                                getSslSettings()),
                        getCredentialWithCache(), CLIENT_METADATA, java.util.Collections.emptyList(),
                        LoggerSettings.builder().build(), null, getServerApi()),
                getClusterConnectionMode(), getServerApi(), false,
                SameObjectProvider.initialized(sdam), OPERATION_CONTEXT_FACTORY);
        serverMonitor.start();
    }
}
