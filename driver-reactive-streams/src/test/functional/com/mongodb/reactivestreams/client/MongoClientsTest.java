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
import com.mongodb.MongoCompressor;
import com.mongodb.MongoCredential;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.connection.TransportSettings;
import com.mongodb.reactivestreams.client.internal.MongoClientImpl;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.ClusterFixture.getConnectionString;
import static com.mongodb.ClusterFixture.getCredential;
import static com.mongodb.ClusterFixture.getServerApi;
import static com.mongodb.ClusterFixture.getSslSettings;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class MongoClientsTest extends FunctionalSpecification {

    @BeforeAll
    static void beforeAll() {
        assumeTrue(getServerApi() == null, "Test ignored when serverApi is not null");
    }

    @Test
    void shouldConnect() {
        StringBuilder cs = new StringBuilder("mongodb://");
        if (getCredential() != null) {
            cs.append(getCredential().getUserName())
                    .append(':')
                    .append(String.valueOf(getCredential().getPassword()))
                    .append('@');
        }
        cs.append(getConnectionString().getHosts().get(0)).append("/?");
        cs.append("ssl=").append(getSslSettings().isEnabled()).append('&');
        cs.append("sslInvalidHostNameAllowed=").append(getSslSettings().isInvalidHostNameAllowed());

        MongoClient client = null;
        try {
            client = MongoClients.create(cs.toString());
            Mono.from(client.getDatabase("admin").runCommand(new Document("ping", 1))).block(TIMEOUT_DURATION);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    @Test
    void shouldApplyConnectionStringToClusterSettings() {
        MongoClientImpl client = null;
        try {
            client = (MongoClientImpl) MongoClients.create("mongodb://localhost,localhost:27018/");
            assertEquals(
                    Arrays.asList(new ServerAddress("localhost"), new ServerAddress("localhost:27018")),
                    client.getSettings().getClusterSettings().getHosts());
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    @Test
    void shouldApplyConnectionStringToCredential() {
        MongoClientImpl client = null;
        try {
            client = (MongoClientImpl) MongoClients.create("mongodb://u:p@localhost/");
            assertEquals(
                    MongoCredential.createCredential("u", "admin", "p".toCharArray()),
                    client.getSettings().getCredential());
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    @Test
    void shouldApplyConnectionStringToServerSettings() {
        MongoClientImpl client = null;
        try {
            client = (MongoClientImpl) MongoClients.create("mongodb://localhost/?heartbeatFrequencyMS=50");
            assertEquals(50, client.getSettings().getServerSettings().getHeartbeatFrequency(MILLISECONDS));
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    @Test
    void shouldApplyConnectionStringToConnectionPoolSettings() {
        MongoClientImpl client = null;
        try {
            client = (MongoClientImpl) MongoClients.create("mongodb://localhost/?maxIdleTimeMS=200&maxLifeTimeMS=300");
            assertEquals(200, client.getSettings().getConnectionPoolSettings().getMaxConnectionIdleTime(MILLISECONDS));
            assertEquals(300, client.getSettings().getConnectionPoolSettings().getMaxConnectionLifeTime(MILLISECONDS));
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    @Test
    void shouldApplyConnectionStringToSslSettings() {
        MongoClientImpl client = null;
        try {
            client = (MongoClientImpl) MongoClients.create("mongodb://localhost/?ssl=true&sslInvalidHostNameAllowed=true");
            assertTrue(client.getSettings().getSslSettings().isEnabled());
            assertTrue(client.getSettings().getSslSettings().isInvalidHostNameAllowed());
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    @Test
    void shouldApplyConnectionStringToSocketSettings() {
        MongoClientImpl client = null;
        try {
            client = (MongoClientImpl) MongoClients.create("mongodb://localhost/?connectTimeoutMS=300");
            assertEquals(300, client.getSettings().getSocketSettings().getConnectTimeout(MILLISECONDS));
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("readPreferenceArgs")
    void shouldApplyReadPreferenceFromConnectionStringToSettings(final String uri, final ReadPreference readPreference) {
        MongoClientImpl client = null;
        try {
            client = (MongoClientImpl) MongoClients.create(uri);
            assertEquals(readPreference, client.getSettings().getReadPreference());
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    private static Stream<Arguments> readPreferenceArgs() {
        return Stream.of(
                Arguments.of("mongodb://localhost/", ReadPreference.primary()),
                Arguments.of("mongodb://localhost/?readPreference=secondaryPreferred", ReadPreference.secondaryPreferred())
        );
    }

    @ParameterizedTest
    @MethodSource("readConcernArgs")
    void shouldApplyReadConcernFromConnectionStringToSettings(final String uri, final ReadConcern readConcern) {
        MongoClientImpl client = null;
        try {
            client = (MongoClientImpl) MongoClients.create(uri);
            assertEquals(readConcern, client.getSettings().getReadConcern());
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    private static Stream<Arguments> readConcernArgs() {
        return Stream.of(
                Arguments.of("mongodb://localhost/", ReadConcern.DEFAULT),
                Arguments.of("mongodb://localhost/?readConcernLevel=local", ReadConcern.LOCAL)
        );
    }

    @ParameterizedTest
    @MethodSource("writeConcernArgs")
    void shouldApplyWriteConcernFromConnectionStringToSettings(final String uri, final WriteConcern writeConcern) {
        MongoClientImpl client = null;
        try {
            client = (MongoClientImpl) MongoClients.create(uri);
            assertEquals(writeConcern, client.getSettings().getWriteConcern());
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    private static Stream<Arguments> writeConcernArgs() {
        return Stream.of(
                Arguments.of("mongodb://localhost", WriteConcern.ACKNOWLEDGED),
                Arguments.of("mongodb://localhost/?w=majority", WriteConcern.MAJORITY)
        );
    }

    @ParameterizedTest
    @MethodSource("applicationNameArgs")
    void shouldApplyApplicationNameFromConnectionStringToSettings(final String uri, final String applicationName) {
        MongoClientImpl client = null;
        try {
            client = (MongoClientImpl) MongoClients.create(uri);
            assertEquals(applicationName, client.getSettings().getApplicationName());
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    private static Stream<Arguments> applicationNameArgs() {
        return Stream.of(
                Arguments.of("mongodb://localhost", null),
                Arguments.of("mongodb://localhost/?appname=app1", "app1")
        );
    }

    @ParameterizedTest
    @MethodSource("compressorArgs")
    void shouldApplyCompressorsFromConnectionStringToSettings(final String uri, final List<MongoCompressor> compressorList) {
        MongoClientImpl client = null;
        try {
            client = (MongoClientImpl) MongoClients.create(uri);
            assertEquals(compressorList, client.getSettings().getCompressorList());
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    private static Stream<Arguments> compressorArgs() {
        return Stream.of(
                Arguments.of("mongodb://localhost", Collections.emptyList()),
                Arguments.of("mongodb://localhost/?compressors=zlib",
                        Collections.singletonList(MongoCompressor.createZlibCompressor())),
                Arguments.of("mongodb://localhost/?compressors=zstd",
                        Collections.singletonList(MongoCompressor.createZstdCompressor()))
        );
    }

    @Test
    void shouldCreateClientWithTransportSettings() {
        MongoClient client = null;
        try {
            client = MongoClients.create(MongoClientSettings.builder()
                    .transportSettings(TransportSettings.nettyBuilder().build())
                    .build());
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
}
