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

import com.mongodb.connection.ClusterConnectionMode;
import org.bson.UuidRepresentation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static com.mongodb.MongoCredential.createCredential;
import static com.mongodb.MongoCredential.createGSSAPICredential;
import static com.mongodb.MongoCredential.createMongoX509Credential;
import static com.mongodb.MongoCredential.createPlainCredential;
import static com.mongodb.MongoCredential.createScramSha1Credential;
import static com.mongodb.ReadPreference.secondaryPreferred;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MongoClientURITest {

    @Test
    @DisplayName("should not throw an Exception if URI contains an unknown option")
    void shouldNotThrowIfURIContainsUnknownOption() {
        assertDoesNotThrow(() -> new MongoClientURI("mongodb://localhost/?unknownOption=5"));
    }

    @ParameterizedTest
    @MethodSource("parseURIArgs")
    @DisplayName("should parse URI into correct components")
    void shouldParseURIIntoCorrectComponents(final MongoClientURI uri, final int num, final List<String> hosts,
                                              final String database, final String collection,
                                              final String username, final char[] password) {
        assertEquals(num, uri.getHosts().size());
        assertEquals(hosts, uri.getHosts());
        assertEquals(database, uri.getDatabase());
        assertEquals(collection, uri.getCollection());
        assertEquals(username, uri.getUsername());
        if (password == null) {
            assertNull(uri.getPassword());
        } else {
            assertEquals(new String(password), new String(uri.getPassword()));
        }
    }

    private static Stream<Object[]> parseURIArgs() {
        return Stream.of(
                new Object[]{new MongoClientURI("mongodb://db.example.com"), 1,
                        Collections.singletonList("db.example.com"), null, null, null, null},
                new Object[]{new MongoClientURI("mongodb://10.0.0.1"), 1,
                        Collections.singletonList("10.0.0.1"), null, null, null, null},
                new Object[]{new MongoClientURI("mongodb://[::1]"), 1,
                        Collections.singletonList("[::1]"), null, null, null, null},
                new Object[]{new MongoClientURI("mongodb://foo/bar"), 1,
                        Collections.singletonList("foo"), "bar", null, null, null},
                new Object[]{new MongoClientURI("mongodb://10.0.0.1/bar"), 1,
                        Collections.singletonList("10.0.0.1"), "bar", null, null, null},
                new Object[]{new MongoClientURI("mongodb://[::1]/bar"), 1,
                        Collections.singletonList("[::1]"), "bar", null, null, null},
                new Object[]{new MongoClientURI("mongodb://localhost/test.my.coll"), 1,
                        Collections.singletonList("localhost"), "test", "my.coll", null, null},
                new Object[]{new MongoClientURI("mongodb://foo/bar.goo"), 1,
                        Collections.singletonList("foo"), "bar", "goo", null, null},
                new Object[]{new MongoClientURI("mongodb://user:pass@host/bar"), 1,
                        Collections.singletonList("host"), "bar", null, "user", "pass".toCharArray()},
                new Object[]{new MongoClientURI("mongodb://user:pass@host:27011/bar"), 1,
                        Collections.singletonList("host:27011"), "bar", null, "user", "pass".toCharArray()},
                new Object[]{new MongoClientURI("mongodb://user:pass@10.0.0.1:27011/bar"), 1,
                        Collections.singletonList("10.0.0.1:27011"), "bar", null, "user", "pass".toCharArray()},
                new Object[]{new MongoClientURI("mongodb://user:pass@[::1]:27011/bar"), 1,
                        Collections.singletonList("[::1]:27011"), "bar", null, "user", "pass".toCharArray()},
                new Object[]{new MongoClientURI("mongodb://user:pass@host:7,host2:8,host3:9/bar"), 3,
                        Arrays.asList("host2:8", "host3:9", "host:7"), "bar", null, "user", "pass".toCharArray()},
                new Object[]{new MongoClientURI("mongodb://user:pass@10.0.0.1:7,[::1]:8,host3:9/bar"), 3,
                        Arrays.asList("10.0.0.1:7", "[::1]:8", "host3:9"), "bar", null, "user", "pass".toCharArray()}
        );
    }

    @ParameterizedTest
    @MethodSource("writeConcernArgs")
    @DisplayName("should correctly parse different write concerns")
    void shouldCorrectlyParseDifferentWriteConcerns(final MongoClientURI uri, final WriteConcern writeConcern) {
        assertEquals(writeConcern, uri.getOptions().getWriteConcern());
    }

    private static Stream<Object[]> writeConcernArgs() {
        return Stream.of(
                new Object[]{new MongoClientURI("mongodb://localhost"), WriteConcern.ACKNOWLEDGED},
                new Object[]{new MongoClientURI("mongodb://localhost/?safe=true"), WriteConcern.ACKNOWLEDGED},
                new Object[]{new MongoClientURI("mongodb://localhost/?safe=false"), WriteConcern.UNACKNOWLEDGED},
                new Object[]{new MongoClientURI("mongodb://localhost/?wTimeout=5"),
                        WriteConcern.ACKNOWLEDGED.withWTimeout(5, MILLISECONDS)},
                new Object[]{new MongoClientURI("mongodb://localhost/?journal=true"),
                        WriteConcern.ACKNOWLEDGED.withJournal(true)},
                new Object[]{new MongoClientURI("mongodb://localhost/?w=2&wtimeoutMS=5&fsync=true&journal=true"),
                        new WriteConcern(2, 5).withJournal(true)},
                new Object[]{new MongoClientURI("mongodb://localhost/?w=majority&wtimeoutMS=5&j=true"),
                        new WriteConcern("majority").withWTimeout(5, MILLISECONDS).withJournal(true)}
        );
    }

    @ParameterizedTest
    @MethodSource("legacyWtimeoutWriteConcernArgs")
    @DisplayName("should correctly parse legacy wtimeout write concerns")
    void shouldCorrectlyParseLegacyWtimeoutWriteConcerns(final MongoClientURI uri, final WriteConcern writeConcern) {
        assertEquals(writeConcern, uri.getOptions().getWriteConcern());
    }

    private static Stream<Object[]> legacyWtimeoutWriteConcernArgs() {
        return Stream.of(
                new Object[]{new MongoClientURI("mongodb://localhost"), WriteConcern.ACKNOWLEDGED},
                new Object[]{new MongoClientURI("mongodb://localhost/?wTimeout=5"),
                        WriteConcern.ACKNOWLEDGED.withWTimeout(5, MILLISECONDS)},
                new Object[]{new MongoClientURI("mongodb://localhost/?w=2&wtimeout=5&j=true"),
                        new WriteConcern(2, 5).withJournal(true)},
                new Object[]{new MongoClientURI("mongodb://localhost/?w=majority&wtimeout=5&j=true"),
                        new WriteConcern("majority").withWTimeout(5, MILLISECONDS).withJournal(true)},
                new Object[]{new MongoClientURI("mongodb://localhost/?wTimeout=1&wtimeoutMS=5"),
                        WriteConcern.ACKNOWLEDGED.withWTimeout(5, MILLISECONDS)}
        );
    }

    @Test
    @DisplayName("should correctly parse URI options")
    void shouldCorrectlyParseURIOptions() {
        MongoClientURI uri = new MongoClientURI("mongodb://localhost/?minPoolSize=5&maxPoolSize=10&waitQueueTimeoutMS=150&"
                + "maxIdleTimeMS=200&maxLifeTimeMS=300&maxConnecting=1&replicaSet=test&"
                + "connectTimeoutMS=2500&socketTimeoutMS=5500&"
                + "safe=false&w=1&wtimeout=2500&ssl=true&readPreference=secondary&"
                + "sslInvalidHostNameAllowed=true&"
                + "serverSelectionTimeoutMS=25000&"
                + "localThresholdMS=30&"
                + "heartbeatFrequencyMS=20000&"
                + "retryWrites=true&"
                + "retryReads=true&"
                + "uuidRepresentation=csharpLegacy&"
                + "appName=app1&"
                + "timeoutMS=10000");

        MongoClientOptions options = uri.getOptions();

        assertEquals(new WriteConcern(1, 2500), options.getWriteConcern());
        assertEquals(ReadPreference.secondary(), options.getReadPreference());
        assertEquals(10, options.getConnectionsPerHost());
        assertEquals(5, options.getMinConnectionsPerHost());
        assertEquals(150, options.getMaxWaitTime());
        assertEquals(200, options.getMaxConnectionIdleTime());
        assertEquals(300, options.getMaxConnectionLifeTime());
        assertEquals(1, options.getMaxConnecting());
        assertEquals(Long.valueOf(10_000), options.getTimeout());
        assertEquals(5500, options.getSocketTimeout());
        assertEquals(2500, options.getConnectTimeout());
        assertEquals("test", options.getRequiredReplicaSetName());
        assertTrue(options.isSslEnabled());
        assertTrue(options.isSslInvalidHostNameAllowed());
        assertEquals(25000, options.getServerSelectionTimeout());
        assertEquals(30, options.getLocalThreshold());
        assertEquals(20000, options.getHeartbeatFrequency());
        assertTrue(options.getRetryWrites());
        assertTrue(options.getRetryReads());
        assertEquals(UuidRepresentation.C_SHARP_LEGACY, options.getUuidRepresentation());
        assertEquals("app1", options.getApplicationName());
    }

    @Test
    @DisplayName("should have correct defaults for options")
    void shouldHaveCorrectDefaultsForOptions() {
        MongoClientOptions options = new MongoClientURI("mongodb://localhost").getOptions();

        assertEquals(100, options.getConnectionsPerHost());
        assertEquals(2, options.getMaxConnecting());
        assertNull(options.getTimeout());
        assertEquals(120000, options.getMaxWaitTime());
        assertEquals(10000, options.getConnectTimeout());
        assertEquals(0, options.getSocketTimeout());
        assertEquals(ReadPreference.primary(), options.getReadPreference());
        assertNull(options.getRequiredReplicaSetName());
        assertFalse(options.isSslEnabled());
        assertTrue(options.getRetryWrites());
        assertTrue(options.getRetryReads());
        assertEquals(UuidRepresentation.UNSPECIFIED, options.getUuidRepresentation());
    }

    @Test
    @DisplayName("should apply default uri to options")
    void shouldApplyDefaultUriToOptions() throws NoSuchAlgorithmException {
        MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder()
                .applicationName("appName")
                .readPreference(ReadPreference.secondary())
                .retryWrites(true)
                .retryReads(true)
                .writeConcern(WriteConcern.JOURNALED)
                .minConnectionsPerHost(30)
                .connectionsPerHost(500)
                .timeout(10_000)
                .connectTimeout(100)
                .socketTimeout(700)
                .serverSelectionTimeout(150)
                .maxWaitTime(200)
                .maxConnectionIdleTime(300)
                .maxConnectionLifeTime(400)
                .maxConnecting(1)
                .sslEnabled(true)
                .sslInvalidHostNameAllowed(true)
                .sslContext(SSLContext.getDefault())
                .heartbeatFrequency(5)
                .minHeartbeatFrequency(11)
                .heartbeatConnectTimeout(15)
                .heartbeatSocketTimeout(20)
                .localThreshold(25)
                .requiredReplicaSetName("test")
                .compressorList(Collections.singletonList(MongoCompressor.createZlibCompressor()))
                .uuidRepresentation(UuidRepresentation.C_SHARP_LEGACY);

        MongoClientOptions options = new MongoClientURI("mongodb://localhost", optionsBuilder).getOptions();

        assertEquals("appName", options.getApplicationName());
        assertEquals(ReadPreference.secondary(), options.getReadPreference());
        assertEquals(WriteConcern.JOURNALED, options.getWriteConcern());
        assertTrue(options.getRetryWrites());
        assertTrue(options.getRetryReads());
        assertEquals(Long.valueOf(10_000), options.getTimeout());
        assertEquals(150, options.getServerSelectionTimeout());
        assertEquals(200, options.getMaxWaitTime());
        assertEquals(300, options.getMaxConnectionIdleTime());
        assertEquals(400, options.getMaxConnectionLifeTime());
        assertEquals(1, options.getMaxConnecting());
        assertEquals(30, options.getMinConnectionsPerHost());
        assertEquals(500, options.getConnectionsPerHost());
        assertEquals(100, options.getConnectTimeout());
        assertEquals(700, options.getSocketTimeout());
        assertTrue(options.isSslEnabled());
        assertTrue(options.isSslInvalidHostNameAllowed());
        assertEquals(5, options.getHeartbeatFrequency());
        assertEquals(11, options.getMinHeartbeatFrequency());
        assertEquals(15, options.getHeartbeatConnectTimeout());
        assertEquals(20, options.getHeartbeatSocketTimeout());
        assertEquals(25, options.getLocalThreshold());
        assertEquals("test", options.getRequiredReplicaSetName());
        assertEquals(5, options.asMongoClientSettings(null, null, ClusterConnectionMode.SINGLE, null)
                .getServerSettings().getHeartbeatFrequency(MILLISECONDS));
        assertEquals(11, options.asMongoClientSettings(null, null, ClusterConnectionMode.SINGLE, null)
                .getServerSettings().getMinHeartbeatFrequency(MILLISECONDS));
        assertEquals(Collections.singletonList(MongoCompressor.createZlibCompressor()), options.getCompressorList());
        assertEquals(UuidRepresentation.C_SHARP_LEGACY, options.getUuidRepresentation());
    }

    @ParameterizedTest
    @MethodSource("credentialArgs")
    @DisplayName("should support all credential types")
    void shouldSupportAllCredentialTypes(final MongoClientURI uri, final MongoCredential credential) {
        assertEquals(credential, uri.getCredentials());
    }

    private static Stream<Object[]> credentialArgs() {
        return Stream.of(
                new Object[]{new MongoClientURI("mongodb://jeff:123@localhost"),
                        createCredential("jeff", "admin", "123".toCharArray())},
                new Object[]{new MongoClientURI("mongodb://jeff:123@localhost/?authMechanism=MONGODB-CR"),
                        createCredential("jeff", "admin", "123".toCharArray())},
                new Object[]{new MongoClientURI("mongodb://jeff:123@localhost/?authMechanism=MONGODB-CR&authSource=test"),
                        createCredential("jeff", "test", "123".toCharArray())},
                new Object[]{new MongoClientURI("mongodb://jeff:123@localhost/?authMechanism=SCRAM-SHA-1"),
                        createScramSha1Credential("jeff", "admin", "123".toCharArray())},
                new Object[]{new MongoClientURI("mongodb://jeff:123@localhost/?authMechanism=SCRAM-SHA-1&authSource=test"),
                        createScramSha1Credential("jeff", "test", "123".toCharArray())},
                new Object[]{new MongoClientURI("mongodb://jeff@localhost/?authMechanism=GSSAPI"),
                        createGSSAPICredential("jeff")},
                new Object[]{new MongoClientURI("mongodb://jeff:123@localhost/?authMechanism=PLAIN"),
                        createPlainCredential("jeff", "$external", "123".toCharArray())},
                new Object[]{new MongoClientURI("mongodb://jeff@localhost/?authMechanism=MONGODB-X509"),
                        createMongoX509Credential("jeff")},
                new Object[]{new MongoClientURI("mongodb://jeff@localhost/?authMechanism=GSSAPI&gssapiServiceName=foo"),
                        createGSSAPICredential("jeff").withMechanismProperty("SERVICE_NAME", "foo")},
                new Object[]{new MongoClientURI("mongodb://jeff:123@localhost/?authMechanism=SCRAM-SHA-1"),
                        createScramSha1Credential("jeff", "admin", "123".toCharArray())}
        );
    }

    @ParameterizedTest
    @MethodSource("readPreferenceArgs")
    @DisplayName("should correctly parse read preference")
    void shouldCorrectlyParseReadPreference(final MongoClientURI uri, final ReadPreference readPreference) {
        assertEquals(readPreference, uri.getOptions().getReadPreference());
    }

    private static Stream<Object[]> readPreferenceArgs() {
        return Stream.of(
                new Object[]{
                        new MongoClientURI("mongodb://localhost/?readPreference=secondaryPreferred"),
                        secondaryPreferred()
                },
                new Object[]{
                        new MongoClientURI("mongodb://localhost/?readPreference=secondaryPreferred"
                                + "&readPreferenceTags=dc:ny,rack:1"
                                + "&readPreferenceTags=dc:ny"
                                + "&readPreferenceTags="),
                        secondaryPreferred(Arrays.asList(
                                new TagSet(Arrays.asList(new Tag("dc", "ny"), new Tag("rack", "1"))),
                                new TagSet(Arrays.asList(new Tag("dc", "ny"))),
                                new TagSet()))
                }
        );
    }

    @Test
    @DisplayName("should apply SRV parameters")
    void shouldApplySRVParameters() {
        MongoClientURI uri = new MongoClientURI("mongodb+srv://test3.test.build.10gen.cc/?srvMaxHosts=4&srvServiceName=test");

        assertEquals(Integer.valueOf(4), uri.getSrvMaxHosts());
        assertEquals("test", uri.getSrvServiceName());

        MongoClientOptions options = uri.getOptions();
        assertEquals(Integer.valueOf(4), options.getSrvMaxHosts());
        assertEquals("test", options.getSrvServiceName());
    }

    @Test
    @DisplayName("should respect MongoClientOptions builder")
    void shouldRespectMongoClientOptionsBuilder() {
        MongoClientURI uri = new MongoClientURI("mongodb://localhost/", MongoClientOptions.builder().connectionsPerHost(200));

        assertEquals(200, uri.getOptions().getConnectionsPerHost());
    }

    @Test
    @DisplayName("should override MongoClientOptions builder")
    void shouldOverrideMongoClientOptionsBuilder() {
        MongoClientURI uri = new MongoClientURI("mongodb://localhost/?maxPoolSize=250",
                MongoClientOptions.builder().connectionsPerHost(200));

        assertEquals(250, uri.getOptions().getConnectionsPerHost());
    }

    @ParameterizedTest
    @MethodSource("equalURIArgs")
    @DisplayName("should be equal to another MongoClientURI with the same string values")
    void shouldBeEqualToAnotherMongoClientURIWithSameStringValues(final MongoClientURI uri1, final MongoClientURI uri2) {
        assertEquals(uri1, uri2);
        assertEquals(uri1.hashCode(), uri2.hashCode());
    }

    private static Stream<Object[]> equalURIArgs() {
        return Stream.of(
                new Object[]{
                        new MongoClientURI("mongodb://user:pass@host1:1/"),
                        new MongoClientURI("mongodb://user:pass@host1:1/")
                },
                new Object[]{
                        new MongoClientURI("mongodb://user:pass@host1:1,host2:2,host3:3/bar"),
                        new MongoClientURI("mongodb://user:pass@host3:3,host1:1,host2:2/bar")
                },
                new Object[]{
                        new MongoClientURI("mongodb://localhost/"
                                + "?readPreference=secondaryPreferred"
                                + "&readPreferenceTags=dc:ny,rack:1"
                                + "&readPreferenceTags=dc:ny"
                                + "&readPreferenceTags="),
                        new MongoClientURI("mongodb://localhost/"
                                + "?readPreference=secondaryPreferred"
                                + "&readPreferenceTags=dc:ny, rack:1"
                                + "&readPreferenceTags=dc:ny"
                                + "&readPreferenceTags=")
                },
                new Object[]{
                        new MongoClientURI("mongodb://localhost/?readPreference=secondaryPreferred"),
                        new MongoClientURI("mongodb://localhost/?readPreference=secondaryPreferred")
                },
                new Object[]{
                        new MongoClientURI("mongodb://ross:123@localhost/?authMechanism=SCRAM-SHA-1"),
                        new MongoClientURI("mongodb://ross:123@localhost/?authMechanism=SCRAM-SHA-1")
                },
                new Object[]{
                        new MongoClientURI("mongodb://localhost/db.coll"
                                + "?minPoolSize=5;"
                                + "maxPoolSize=10;"
                                + "waitQueueTimeoutMS=150;"
                                + "maxIdleTimeMS=200;"
                                + "maxLifeTimeMS=300;replicaSet=test;"
                                + "maxConnecting=1;"
                                + "connectTimeoutMS=2500;"
                                + "socketTimeoutMS=5500;"
                                + "safe=false;w=1;wtimeout=2500;"
                                + "fsync=true;readPreference=primary;"
                                + "ssl=true"),
                        new MongoClientURI("mongodb://localhost/db.coll?minPoolSize=5;"
                                + "maxPoolSize=10;"
                                + "waitQueueTimeoutMS=150;"
                                + "maxIdleTimeMS=200&maxLifeTimeMS=300;"
                                + "maxConnecting=1;"
                                + "&replicaSet=test;connectTimeoutMS=2500;"
                                + "socketTimeoutMS=5500&safe=false&w=1;"
                                + "wtimeout=2500;fsync=true"
                                + "&readPreference=primary;ssl=true")
                }
        );
    }

    @ParameterizedTest
    @MethodSource("notEqualURIArgs")
    @DisplayName("should be not equal to another MongoClientURI with different string values")
    void shouldBeNotEqualToAnotherMongoClientURIWithDifferentStringValues(final MongoClientURI uri1, final MongoClientURI uri2) {
        assertNotEquals(uri1, uri2);
        assertNotEquals(uri1.hashCode(), uri2.hashCode());
    }

    private static Stream<Object[]> notEqualURIArgs() {
        return Stream.of(
                new Object[]{
                        new MongoClientURI("mongodb://user:pass@host1:1/"),
                        new MongoClientURI("mongodb://user:pass@host1:2/")
                },
                new Object[]{
                        new MongoClientURI("mongodb://user:pass@host1:1,host2:2,host3:3/bar"),
                        new MongoClientURI("mongodb://user:pass@host1:1,host2:2,host4:4/bar")
                },
                new Object[]{
                        new MongoClientURI("mongodb://localhost/?readPreference=secondaryPreferred"),
                        new MongoClientURI("mongodb://localhost/?readPreference=secondary")
                },
                new Object[]{
                        new MongoClientURI("mongodb://localhost/"
                                + "?readPreference=secondaryPreferred"
                                + "&readPreferenceTags=dc:ny,rack:1"
                                + "&readPreferenceTags=dc:ny"
                                + "&readPreferenceTags="
                                + "&maxConnecting=1"),
                        new MongoClientURI("mongodb://localhost/"
                                + "?readPreference=secondaryPreferred"
                                + "&readPreferenceTags=dc:ny"
                                + "&readPreferenceTags=dc:ny, rack:1"
                                + "&readPreferenceTags="
                                + "&maxConnecting=2")
                },
                new Object[]{
                        new MongoClientURI("mongodb://ross:123@localhost/?authMechanism=SCRAM-SHA-1"),
                        new MongoClientURI("mongodb://ross:123@localhost/?authMechanism=GSSAPI")
                }
        );
    }

    @Test
    @DisplayName("should be equal to another MongoClientURI with options")
    void shouldBeEqualToAnotherMongoClientURIWithOptions() {
        MongoClientURI uri1 = new MongoClientURI("mongodb://user:pass@host1:1,host2:2,host3:3/bar?"
                + "maxPoolSize=10;waitQueueTimeoutMS=150;"
                + "minPoolSize=7;maxIdleTimeMS=1000;maxLifeTimeMS=2000;maxConnecting=1;"
                + "replicaSet=test;"
                + "connectTimeoutMS=2500;socketTimeoutMS=5500;autoConnectRetry=true;"
                + "readPreference=secondaryPreferred;safe=false;w=1;wtimeout=2600");

        MongoClientOptions.Builder builder = MongoClientOptions.builder()
                .connectionsPerHost(10)
                .maxWaitTime(150)
                .minConnectionsPerHost(7)
                .maxConnectionIdleTime(1000)
                .maxConnectionLifeTime(2000)
                .maxConnecting(1)
                .requiredReplicaSetName("test")
                .connectTimeout(2500)
                .socketTimeout(5500)
                .readPreference(secondaryPreferred())
                .writeConcern(new WriteConcern(1, 2600));

        MongoClientOptions options = builder.build();

        assertEquals(options, uri1.getOptions());

        MongoClientURI uri2 = new MongoClientURI("mongodb://user:pass@host3:3,host1:1,host2:2/bar?", builder);

        assertEquals(uri1, uri2);
        assertEquals(uri1.hashCode(), uri2.hashCode());
    }
}
