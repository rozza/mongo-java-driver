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

import com.mongodb.client.MongoCollection;
import com.mongodb.connection.ServerDescription;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.Fixture.getDefaultDatabaseName;
import static com.mongodb.Fixture.getMongoClientURI;
import static com.mongodb.Fixture.getOptions;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@SuppressWarnings("deprecation")
class MongoClientsTest extends FunctionalSpecification {

    @Test
    @DisplayName("should use server selector from MongoClientOptions")
    void shouldUseServerSelectorFromMongoClientOptions() {
        assumeTrue(isDiscoverableReplicaSet());

        Set<ServerAddress> expectedWinningAddresses = Collections.synchronizedSet(new HashSet<>());
        Set<ServerAddress> actualWinningAddresses = Collections.synchronizedSet(new HashSet<>());

        MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder(getOptions())
                .serverSelector(clusterDescription -> {
                    ServerDescription highestPortServer = null;
                    for (ServerDescription cur : clusterDescription.getServerDescriptions()) {
                        if (highestPortServer == null || cur.getAddress().getPort() > highestPortServer.getAddress().getPort()) {
                            highestPortServer = cur;
                        }
                    }
                    if (highestPortServer == null) {
                        return Collections.emptyList();
                    }
                    expectedWinningAddresses.add(highestPortServer.getAddress());
                    return Collections.singletonList(highestPortServer);
                })
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandStarted(final CommandStartedEvent event) {
                        actualWinningAddresses.add(
                                event.getConnectionDescription().getConnectionId().getServerId().getAddress());
                    }

                    @Override
                    public void commandSucceeded(final CommandSucceededEvent event) {
                    }

                    @Override
                    public void commandFailed(final CommandFailedEvent event) {
                    }
                });

        MongoClient client = new MongoClient(getMongoClientURI(optionsBuilder));
        try {
            MongoCollection<Document> coll = client.getDatabase(getDefaultDatabaseName())
                    .getCollection(getCollectionName())
                    .withReadPreference(ReadPreference.nearest());

            for (int i = 0; i < 10; i++) {
                coll.countDocuments();
            }

            assertTrue(expectedWinningAddresses.containsAll(actualWinningAddresses));
        } finally {
            client.close();
        }
    }
}
