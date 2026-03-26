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

import com.mongodb.spi.dns.DnsClient;
import org.bson.UuidRepresentation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static com.mongodb.MongoCompressor.LEVEL;
import static com.mongodb.MongoCompressor.createZlibCompressor;
import static com.mongodb.MongoCompressor.createZstdCompressor;
import static com.mongodb.MongoCredential.createCredential;
import static com.mongodb.MongoCredential.createGSSAPICredential;
import static com.mongodb.MongoCredential.createMongoX509Credential;
import static com.mongodb.MongoCredential.createPlainCredential;
import static com.mongodb.MongoCredential.createScramSha1Credential;
import static com.mongodb.MongoCredential.createScramSha256Credential;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.ReadPreference.secondary;
import static com.mongodb.ReadPreference.secondaryPreferred;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectionStringSpecificationTest {

    @ParameterizedTest
    @MethodSource("parseComponentsData")
    @DisplayName("should parse into correct components")
    void shouldParseIntoCorrectComponents(final ConnectionString connectionString, final int num,
                                           final List<String> hosts, final String database, final String collection,
                                           final String username, final char[] password) {
        assertEquals(num, connectionString.getHosts().size());
        assertEquals(hosts, connectionString.getHosts());
        assertEquals(database, connectionString.getDatabase());
        assertEquals(collection, connectionString.getCollection());
        assertEquals(username, connectionString.getUsername());
        if (password == null) {
            assertNull(connectionString.getPassword());
        } else {
            assertArrayEquals(password, connectionString.getPassword());
        }
    }

    static Stream<Object[]> parseComponentsData() {
        return Stream.of(
                new Object[]{new ConnectionString("mongodb://db.example.com"), 1,
                        Collections.singletonList("db.example.com"), null, null, null, null},
                new Object[]{new ConnectionString("mongodb://10.0.0.1"), 1,
                        Collections.singletonList("10.0.0.1"), null, null, null, null},
                new Object[]{new ConnectionString("mongodb://[::1]"), 1,
                        Collections.singletonList("[::1]"), null, null, null, null},
                new Object[]{new ConnectionString("mongodb://%2Ftmp%2Fmongodb-27017.sock"), 1,
                        Collections.singletonList("/tmp/mongodb-27017.sock"), null, null, null, null},
                new Object[]{new ConnectionString("mongodb://foo/bar"), 1,
                        Collections.singletonList("foo"), "bar", null, null, null},
                new Object[]{new ConnectionString("mongodb://10.0.0.1/bar"), 1,
                        Collections.singletonList("10.0.0.1"), "bar", null, null, null},
                new Object[]{new ConnectionString("mongodb://[::1]/bar"), 1,
                        Collections.singletonList("[::1]"), "bar", null, null, null},
                new Object[]{new ConnectionString("mongodb://%2Ftmp%2Fmongodb-27017.sock/bar"), 1,
                        Collections.singletonList("/tmp/mongodb-27017.sock"), "bar", null, null, null},
                new Object[]{new ConnectionString("mongodb://localhost/test.my.coll"), 1,
                        Collections.singletonList("localhost"), "test", "my.coll", null, null},
                new Object[]{new ConnectionString("mongodb://foo/bar.goo"), 1,
                        Collections.singletonList("foo"), "bar", "goo", null, null},
                new Object[]{new ConnectionString("mongodb://foo/s,db"), 1,
                        Collections.singletonList("foo"), "s,db", null, null, null},
                new Object[]{new ConnectionString("mongodb://foo/s%2Cdb"), 1,
                        Collections.singletonList("foo"), "s,db", null, null, null},
                new Object[]{new ConnectionString("mongodb://user:pass@host/bar"), 1,
                        Collections.singletonList("host"), "bar", null, "user", "pass".toCharArray()},
                new Object[]{new ConnectionString("mongodb://user:pass@host:27011/bar"), 1,
                        Collections.singletonList("host:27011"), "bar", null, "user", "pass".toCharArray()},
                new Object[]{new ConnectionString("mongodb://user:pass@10.0.0.1:27011/bar"), 1,
                        Collections.singletonList("10.0.0.1:27011"), "bar", null, "user", "pass".toCharArray()},
                new Object[]{new ConnectionString("mongodb://user:pass@[::1]:27011/bar"), 1,
                        Collections.singletonList("[::1]:27011"), "bar", null, "user", "pass".toCharArray()},
                new Object[]{new ConnectionString("mongodb://user:pass@host:7,host2:8,host3:9/bar"), 3,
                        Arrays.asList("host2:8", "host3:9", "host:7"), "bar", null, "user", "pass".toCharArray()},
                new Object[]{new ConnectionString("mongodb://user:pass@10.0.0.1:7,[::1]:8,host3:9/bar"), 3,
                        Arrays.asList("10.0.0.1:7", "[::1]:8", "host3:9"), "bar", null, "user", "pass".toCharArray()},
                new Object[]{new ConnectionString("mongodb://user:pass@%2Ftmp%2Fmongodb-27017.sock,"
                        + "%2Ftmp%2Fmongodb-27018.sock,%2Ftmp%2Fmongodb-27019.sock/bar"), 3,
                        Arrays.asList("/tmp/mongodb-27017.sock", "/tmp/mongodb-27018.sock",
                                "/tmp/mongodb-27019.sock"), "bar", null, "user", "pass".toCharArray()}
        );
    }

    @Test
    @DisplayName("should throw exception if mongod+srv host contains a port")
    void shouldThrowForSrvWithPort() {
        assertThrows(IllegalArgumentException.class, () -> new ConnectionString("mongodb+srv://host1:27017"));
    }

    @Test
    @DisplayName("should throw exception if mongod+srv contains multiple hosts")
    void shouldThrowForSrvWithMultipleHosts() {
        assertThrows(IllegalArgumentException.class, () -> new ConnectionString("mongodb+srv://host1,host2"));
    }

    @Test
    @DisplayName("should throw exception if direct connection when using mongodb+srv")
    void shouldThrowForSrvWithDirectConnection() {
        assertThrows(IllegalArgumentException.class,
                () -> new ConnectionString("mongodb+srv://test5.test.build.10gen.cc/?directConnection=true"));
    }

    @ParameterizedTest
    @MethodSource("writeConcernData")
    @DisplayName("should correctly parse different write concerns")
    void shouldParseWriteConcerns(final ConnectionString uri, final WriteConcern writeConcern) {
        assertEquals(writeConcern, uri.getWriteConcern());
    }

    static Stream<Object[]> writeConcernData() {
        return Stream.of(
                new Object[]{new ConnectionString("mongodb://localhost"), null},
                new Object[]{new ConnectionString("mongodb://localhost/?safe=true"), WriteConcern.ACKNOWLEDGED},
                new Object[]{new ConnectionString("mongodb://localhost/?safe=false"), WriteConcern.UNACKNOWLEDGED},
                new Object[]{new ConnectionString("mongodb://localhost/?wTimeout=5"),
                        WriteConcern.ACKNOWLEDGED.withWTimeout(5, MILLISECONDS)},
                new Object[]{new ConnectionString("mongodb://localhost/?journal=true"),
                        WriteConcern.ACKNOWLEDGED.withJournal(true)},
                new Object[]{new ConnectionString("mongodb://localhost/?w=2&wtimeout=5&journal=true"),
                        new WriteConcern(2, 5).withJournal(true)},
                new Object[]{new ConnectionString("mongodb://localhost/?w=majority&wtimeout=5&j=true"),
                        new WriteConcern("majority").withWTimeout(5, MILLISECONDS).withJournal(true)}
        );
    }

    @ParameterizedTest
    @MethodSource("trailingSlashData")
    @DisplayName("should treat trailing slash before query parameters as optional")
    void shouldTreatTrailingSlashAsOptional(final ConnectionString uri, final String appName, final String db) {
        assertEquals(appName, uri.getApplicationName());
        assertEquals(db, uri.getDatabase());
    }

    static Stream<Object[]> trailingSlashData() {
        return Stream.of(
                new Object[]{new ConnectionString("mongodb://mongodb.com"), null, null},
                new Object[]{new ConnectionString("mongodb://mongodb.com?"), null, null},
                new Object[]{new ConnectionString("mongodb://mongodb.com/"), null, null},
                new Object[]{new ConnectionString("mongodb://mongodb.com/?"), null, null},
                new Object[]{new ConnectionString("mongodb://mongodb.com/test"), null, "test"},
                new Object[]{new ConnectionString("mongodb://mongodb.com/test?"), null, "test"},
                new Object[]{new ConnectionString("mongodb://mongodb.com/?appName=a1"), "a1", null},
                new Object[]{new ConnectionString("mongodb://mongodb.com?appName=a1"), "a1", null},
                new Object[]{new ConnectionString("mongodb://mongodb.com/?appName=a1/a2"), "a1/a2", null},
                new Object[]{new ConnectionString("mongodb://mongodb.com?appName=a1/a2"), "a1/a2", null},
                new Object[]{new ConnectionString("mongodb://mongodb.com/test?appName=a1"), "a1", "test"},
                new Object[]{new ConnectionString("mongodb://mongodb.com/test?appName=a1/a2"), "a1/a2", "test"}
        );
    }

    @ParameterizedTest
    @MethodSource("uuidRepresentationData")
    @DisplayName("should correctly parse different UUID representations")
    void shouldParseUuidRepresentations(final ConnectionString uri, final UuidRepresentation uuidRepresentation) {
        assertEquals(uuidRepresentation, uri.getUuidRepresentation());
    }

    static Stream<Object[]> uuidRepresentationData() {
        return Stream.of(
                new Object[]{new ConnectionString("mongodb://localhost"), null},
                new Object[]{new ConnectionString("mongodb://localhost/?uuidRepresentation=unspecified"),
                        UuidRepresentation.UNSPECIFIED},
                new Object[]{new ConnectionString("mongodb://localhost/?uuidRepresentation=javaLegacy"),
                        UuidRepresentation.JAVA_LEGACY},
                new Object[]{new ConnectionString("mongodb://localhost/?uuidRepresentation=csharpLegacy"),
                        UuidRepresentation.C_SHARP_LEGACY},
                new Object[]{new ConnectionString("mongodb://localhost/?uuidRepresentation=pythonLegacy"),
                        UuidRepresentation.PYTHON_LEGACY},
                new Object[]{new ConnectionString("mongodb://localhost/?uuidRepresentation=standard"),
                        UuidRepresentation.STANDARD}
        );
    }

    @ParameterizedTest
    @MethodSource("retryWritesData")
    @DisplayName("should correctly parse retryWrites")
    void shouldParseRetryWrites(final ConnectionString uri, final Boolean retryWritesValue) {
        assertEquals(retryWritesValue, uri.getRetryWritesValue());
    }

    static Stream<Object[]> retryWritesData() {
        return Stream.of(
                new Object[]{new ConnectionString("mongodb://localhost/"), null},
                new Object[]{new ConnectionString("mongodb://localhost/?retryWrites=false"), false},
                new Object[]{new ConnectionString("mongodb://localhost/?retryWrites=true"), true},
                new Object[]{new ConnectionString("mongodb://localhost/?retryWrites=foos"), null}
        );
    }

    @ParameterizedTest
    @MethodSource("booleanData")
    @DisplayName("should parse range of boolean values")
    void shouldParseBooleanValues(final ConnectionString uri, final Boolean value) {
        assertEquals(value, uri.getSslEnabled());
    }

    static Stream<Object[]> booleanData() {
        return Stream.of(
                new Object[]{new ConnectionString("mongodb://localhost/?tls=true"), true},
                new Object[]{new ConnectionString("mongodb://localhost/?tls=yes"), true},
                new Object[]{new ConnectionString("mongodb://localhost/?tls=1"), true},
                new Object[]{new ConnectionString("mongodb://localhost/?tls=false"), false},
                new Object[]{new ConnectionString("mongodb://localhost/?tls=no"), false},
                new Object[]{new ConnectionString("mongodb://localhost/?tls=0"), false},
                new Object[]{new ConnectionString("mongodb://localhost/?tls=foo"), null},
                new Object[]{new ConnectionString("mongodb://localhost"), null}
        );
    }

    @ParameterizedTest
    @MethodSource("retryReadsData")
    @DisplayName("should correct parse retryReads")
    void shouldParseRetryReads(final ConnectionString uri, final Boolean retryReads) {
        assertEquals(retryReads, uri.getRetryReads());
    }

    static Stream<Object[]> retryReadsData() {
        return Stream.of(
                new Object[]{new ConnectionString("mongodb://localhost/"), null},
                new Object[]{new ConnectionString("mongodb://localhost/?retryReads=false"), false},
                new Object[]{new ConnectionString("mongodb://localhost/?retryReads=true"), true},
                new Object[]{new ConnectionString("mongodb://localhost/?retryReads=foos"), null}
        );
    }

    @ParameterizedTest
    @MethodSource("uriOptionsData")
    @DisplayName("should correctly parse URI options")
    void shouldParseUriOptions(final ConnectionString connectionString) {
        assertEquals(Integer.valueOf(5), connectionString.getMinConnectionPoolSize());
        assertEquals(Integer.valueOf(10), connectionString.getMaxConnectionPoolSize());
        assertEquals(Integer.valueOf(150), connectionString.getMaxWaitTime());
        assertEquals(Integer.valueOf(200), connectionString.getMaxConnectionIdleTime());
        assertEquals(Integer.valueOf(300), connectionString.getMaxConnectionLifeTime());
        assertEquals(Integer.valueOf(1), connectionString.getMaxConnecting());
        assertEquals(Integer.valueOf(2500), connectionString.getConnectTimeout());
        assertEquals(Integer.valueOf(5500), connectionString.getSocketTimeout());
        assertEquals(new WriteConcern(1, 2500), connectionString.getWriteConcern());
        assertEquals(primary(), connectionString.getReadPreference());
        assertEquals("test", connectionString.getRequiredReplicaSetName());
        assertTrue(connectionString.getSslEnabled());
        assertTrue(connectionString.getSslInvalidHostnameAllowed());
        assertEquals(Integer.valueOf(25000), connectionString.getServerSelectionTimeout());
        assertEquals(Integer.valueOf(30), connectionString.getLocalThreshold());
        assertEquals(Integer.valueOf(20000), connectionString.getHeartbeatFrequency());
        assertEquals("app1", connectionString.getApplicationName());
    }

    static Stream<ConnectionString> uriOptionsData() {
        return Stream.of(
                new ConnectionString("mongodb://localhost/?minPoolSize=5&maxPoolSize=10&waitQueueTimeoutMS=150&"
                        + "maxIdleTimeMS=200&maxLifeTimeMS=300&maxConnecting=1&replicaSet=test&"
                        + "connectTimeoutMS=2500&socketTimeoutMS=5500&"
                        + "safe=false&w=1&wtimeout=2500&readPreference=primary&ssl=true&"
                        + "sslInvalidHostNameAllowed=true&"
                        + "serverSelectionTimeoutMS=25000&"
                        + "localThresholdMS=30&"
                        + "heartbeatFrequencyMS=20000&"
                        + "appName=app1"),
                new ConnectionString("mongodb://localhost/?minPoolSize=5;maxPoolSize=10;waitQueueTimeoutMS=150;"
                        + "maxIdleTimeMS=200;maxLifeTimeMS=300;maxConnecting=1;replicaSet=test;"
                        + "connectTimeoutMS=2500;socketTimeoutMS=5500;"
                        + "safe=false;w=1;wtimeout=2500;readPreference=primary;ssl=true;"
                        + "sslInvalidHostNameAllowed=true;"
                        + "serverSelectionTimeoutMS=25000;"
                        + "localThresholdMS=30;"
                        + "heartbeatFrequencyMS=20000;"
                        + "appName=app1"),
                new ConnectionString("mongodb://localhost/test?minPoolSize=5;maxPoolSize=10;waitQueueTimeoutMS=150;"
                        + "maxIdleTimeMS=200&maxLifeTimeMS=300&maxConnecting=1&replicaSet=test;"
                        + "connectTimeoutMS=2500;"
                        + "socketTimeoutMS=5500&"
                        + "safe=false&w=1;wtimeout=2500;readPreference=primary;ssl=true;"
                        + "sslInvalidHostNameAllowed=true;"
                        + "serverSelectionTimeoutMS=25000&"
                        + "localThresholdMS=30;"
                        + "heartbeatFrequencyMS=20000&"
                        + "appName=app1")
        );
    }

    @Test
    @DisplayName("should parse options to enable TLS")
    void shouldParseOptionsToEnableTls() {
        assertEquals(false, new ConnectionString("mongodb://localhost/?ssl=false").getSslEnabled());
        assertTrue(new ConnectionString("mongodb://localhost/?ssl=true").getSslEnabled());
        assertNull(new ConnectionString("mongodb://localhost/?ssl=foo").getSslEnabled());
        assertEquals(false, new ConnectionString("mongodb://localhost/?tls=false").getSslEnabled());
        assertTrue(new ConnectionString("mongodb://localhost/?tls=true").getSslEnabled());
        assertNull(new ConnectionString("mongodb://localhost/?tls=foo").getSslEnabled());
        assertThrows(IllegalArgumentException.class,
                () -> new ConnectionString("mongodb://localhost/?tls=true&ssl=false"));
        assertThrows(IllegalArgumentException.class,
                () -> new ConnectionString("mongodb://localhost/?tls=false&ssl=true"));
    }

    @Test
    @DisplayName("should parse options to enable TLS invalid host names")
    void shouldParseOptionsToEnableTlsInvalidHostNames() {
        assertEquals(false, new ConnectionString("mongodb://localhost/?ssl=true&sslInvalidHostNameAllowed=false")
                .getSslInvalidHostnameAllowed());
        assertTrue(new ConnectionString("mongodb://localhost/?ssl=true&sslInvalidHostNameAllowed=true")
                .getSslInvalidHostnameAllowed());
        assertNull(new ConnectionString("mongodb://localhost/?ssl=true&sslInvalidHostNameAllowed=foo")
                .getSslInvalidHostnameAllowed());
        assertEquals(false, new ConnectionString("mongodb://localhost/?tls=true&tlsAllowInvalidHostnames=false")
                .getSslInvalidHostnameAllowed());
        assertTrue(new ConnectionString("mongodb://localhost/?tls=true&tlsAllowInvalidHostnames=true")
                .getSslInvalidHostnameAllowed());
        assertNull(new ConnectionString("mongodb://localhost/?tls=true&tlsAllowInvalidHostnames=foo")
                .getSslInvalidHostnameAllowed());
        assertEquals(false, new ConnectionString(
                "mongodb://localhost/?tls=true&tlsAllowInvalidHostnames=false&sslInvalidHostNameAllowed=true")
                .getSslInvalidHostnameAllowed());
        assertTrue(new ConnectionString(
                "mongodb://localhost/?tls=true&tlsAllowInvalidHostnames=true&sslInvalidHostNameAllowed=false")
                .getSslInvalidHostnameAllowed());
    }

    @Test
    @DisplayName("should parse options to enable unsecured TLS")
    void shouldParseOptionsToEnableUnsecuredTls() {
        assertTrue(new ConnectionString("mongodb://localhost/?tls=true&tlsInsecure=true")
                .getSslInvalidHostnameAllowed());
        assertEquals(false, new ConnectionString("mongodb://localhost/?tls=true&tlsInsecure=false")
                .getSslInvalidHostnameAllowed());
        assertEquals(false, new ConnectionString("mongodb://localhost/?tls=true&tlsAllowInvalidHostnames=false")
                .getSslInvalidHostnameAllowed());
        assertTrue(new ConnectionString("mongodb://localhost/?tls=true&tlsAllowInvalidHostnames=true")
                .getSslInvalidHostnameAllowed());
    }

    @ParameterizedTest
    @MethodSource("invalidProxyData")
    @DisplayName("should throw IllegalArgumentException when the proxy settings are invalid")
    void shouldThrowForInvalidProxy(final String cause, final String connectionString) {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new ConnectionString(connectionString));
        assertEquals(cause, exception.getMessage());
    }

    static Stream<Object[]> invalidProxyData() {
        String longString = new String(new char[256]).replace('\0', (char) 1);
        return Stream.of(
                new Object[]{"proxyPort can only be specified with proxyHost",
                        "mongodb://localhost:27017/?proxyPort=1"},
                new Object[]{"proxyPort should be within the valid range (0 to 65535)",
                        "mongodb://localhost:27017/?proxyHost=a&proxyPort=-1"},
                new Object[]{"proxyPort should be within the valid range (0 to 65535)",
                        "mongodb://localhost:27017/?proxyHost=a&proxyPort=65536"},
                new Object[]{"proxyUsername can only be specified with proxyHost",
                        "mongodb://localhost:27017/?proxyUsername=1"},
                new Object[]{"proxyUsername cannot be empty",
                        "mongodb://localhost:27017/?proxyHost=a&proxyUsername="},
                new Object[]{"proxyPassword can only be specified with proxyHost",
                        "mongodb://localhost:27017/?proxyPassword=1"},
                new Object[]{"proxyPassword cannot be empty",
                        "mongodb://localhost:27017/?proxyHost=a&proxyPassword="},
                new Object[]{"username's length in bytes cannot be greater than 255",
                        "mongodb://localhost:27017/?proxyHost=a&proxyUsername=" + longString},
                new Object[]{"password's length in bytes cannot be greater than 255",
                        "mongodb://localhost:27017/?proxyHost=a&proxyPassword=" + longString},
                new Object[]{"Both proxyUsername and proxyPassword must be set together. They cannot be set individually",
                        "mongodb://localhost:27017/?proxyHost=a&proxyPassword=1"}
        );
    }

    @ParameterizedTest
    @MethodSource("validProxySocketData")
    @DisplayName("should create connection string with valid proxy socket settings")
    void shouldCreateWithValidProxySocket(final String uri, final String proxyHost) {
        ConnectionString connectionString = new ConnectionString(uri);
        assertEquals(proxyHost, connectionString.getProxyHost());
        assertEquals(Integer.valueOf(1081), connectionString.getProxyPort());
    }

    static Stream<Object[]> validProxySocketData() {
        return Stream.of(
                new Object[]{"mongodb://localhost:27017/?proxyHost=2001:db8:85a3::8a2e:370:7334&proxyPort=1081",
                        "2001:db8:85a3::8a2e:370:7334"},
                new Object[]{"mongodb://localhost:27017/?proxyHost=::5000&proxyPort=1081", "::5000"},
                new Object[]{"mongodb://localhost:27017/?proxyHost=%3A%3A5000&proxyPort=1081", "::5000"},
                new Object[]{"mongodb://localhost:27017/?proxyHost=0::1&proxyPort=1081", "0::1"},
                new Object[]{"mongodb://localhost:27017/?proxyHost=hyphen-domain.com&proxyPort=1081",
                        "hyphen-domain.com"},
                new Object[]{"mongodb://localhost:27017/?proxyHost=sub.domain.c.com.com&proxyPort=1081",
                        "sub.domain.c.com.com"},
                new Object[]{"mongodb://localhost:27017/?proxyHost=192.168.0.1&proxyPort=1081", "192.168.0.1"}
        );
    }

    @ParameterizedTest
    @MethodSource("validProxyCredentialsData")
    @DisplayName("should create connection string with valid proxy credentials settings")
    void shouldCreateWithValidProxyCredentials(final String uri, final String proxyPassword,
                                               final String proxyUsername) {
        ConnectionString connectionString = new ConnectionString(uri);
        assertEquals(proxyPassword, connectionString.getProxyPassword());
        assertEquals(proxyUsername, connectionString.getProxyUsername());
    }

    static Stream<Object[]> validProxyCredentialsData() {
        return Stream.of(
                new Object[]{
                        "mongodb://localhost:27017/?proxyHost=test4&proxyPassword=pass%21wor%24&proxyUsername=user%21name",
                        "pass!wor$", "user!name"},
                new Object[]{
                        "mongodb://localhost:27017/?proxyHost=::5000&proxyPassword=pass!wor$&proxyUsername=user!name",
                        "pass!wor$", "user!name"}
        );
    }

    @Test
    @DisplayName("should set proxy settings properties")
    void shouldSetProxySettingsProperties() {
        ConnectionString connectionString = new ConnectionString("mongodb+srv://test5.cc/?"
                + "proxyPort=1080"
                + "&proxyHost=proxy.com"
                + "&proxyUsername=username"
                + "&proxyPassword=password");

        assertEquals("proxy.com", connectionString.getProxyHost());
        assertEquals(Integer.valueOf(1080), connectionString.getProxyPort());
        assertEquals("username", connectionString.getProxyUsername());
        assertEquals("password", connectionString.getProxyPassword());
    }

    @ParameterizedTest
    @MethodSource("invalidConnectionStringData")
    @DisplayName("should throw IllegalArgumentException for invalid connection strings")
    void shouldThrowForInvalidConnectionStrings(final String cause, final String connectionString) {
        assertThrows(IllegalArgumentException.class, () -> new ConnectionString(connectionString));
    }

    static Stream<Object[]> invalidConnectionStringData() {
        return Stream.of(
                new Object[]{"is not a connection string", "hello world"},
                new Object[]{"is missing a host", "mongodb://"},
                new Object[]{"has an empty host", "mongodb://localhost:27017,,localhost:27019"},
                new Object[]{"has an malformed IPv6 host", "mongodb://[::1"},
                new Object[]{"has unescaped colons", "mongodb://locahost::1"},
                new Object[]{"contains an invalid port string", "mongodb://localhost:twenty"},
                new Object[]{"contains an invalid port negative", "mongodb://localhost:-1"},
                new Object[]{"contains an invalid port out of range", "mongodb://localhost:1000000"},
                new Object[]{"contains multiple at-signs", "mongodb://user@123:password@localhost"},
                new Object[]{"contains multiple colons", "mongodb://user:123:password@localhost"},
                new Object[]{"invalid integer in options", "mongodb://localhost/?wTimeout=five"},
                new Object[]{"has incomplete options", "mongodb://localhost/?wTimeout"},
                new Object[]{"has an unknown auth mechanism", "mongodb://user:password@localhost/?authMechanism=postItNote"},
                new Object[]{"invalid readConcern", "mongodb://localhost:27017/?readConcernLevel=pickThree"},
                new Object[]{"contains tags but no mode", "mongodb://localhost:27017/?readPreferenceTags=dc:ny"},
                new Object[]{"contains max staleness but no mode",
                        "mongodb://localhost:27017/?maxStalenessSeconds=100.5"},
                new Object[]{"contains tags and primary mode",
                        "mongodb://localhost:27017/?readPreference=primary&readPreferenceTags=dc:ny"},
                new Object[]{"contains max staleness and primary mode",
                        "mongodb://localhost:27017/?readPreference=primary&maxStalenessSeconds=100"},
                new Object[]{"contains non-integral max staleness",
                        "mongodb://localhost:27017/?readPreference=secondary&maxStalenessSeconds=100.0"},
                new Object[]{"contains GSSAPI mechanism with no user",
                        "mongodb://localhost:27017/?authMechanism=GSSAPI"},
                new Object[]{"contains SCRAM mechanism with no user",
                        "mongodb://localhost:27017/?authMechanism=SCRAM-SHA-1"},
                new Object[]{"contains MONGODB mechanism with no user",
                        "mongodb://localhost:27017/?authMechanism=MONGODB-CR"},
                new Object[]{"contains PLAIN mechanism with no user",
                        "mongodb://localhost:27017/?authMechanism=PLAIN"},
                new Object[]{"contains multiple hosts and directConnection",
                        "mongodb://localhost:27017,localhost:27018/?directConnection=true"}
        );
    }

    @Test
    @DisplayName("should have correct defaults for options")
    void shouldHaveCorrectDefaults() {
        ConnectionString connectionString = new ConnectionString("mongodb://localhost");

        assertNull(connectionString.getMaxConnectionPoolSize());
        assertNull(connectionString.getMaxWaitTime());
        assertNull(connectionString.getMaxConnecting());
        assertNull(connectionString.getConnectTimeout());
        assertNull(connectionString.getSocketTimeout());
        assertNull(connectionString.getWriteConcern());
        assertNull(connectionString.getReadConcern());
        assertNull(connectionString.getReadPreference());
        assertNull(connectionString.getRequiredReplicaSetName());
        assertNull(connectionString.getSslEnabled());
        assertNull(connectionString.getSslInvalidHostnameAllowed());
        assertNull(connectionString.getApplicationName());
        assertEquals(Collections.emptyList(), connectionString.getCompressorList());
        assertNull(connectionString.getRetryWritesValue());
        assertNull(connectionString.getRetryReads());
    }

    @ParameterizedTest
    @MethodSource("credentialTypesData")
    @DisplayName("should support all credential types")
    void shouldSupportAllCredentialTypes(final ConnectionString uri, final MongoCredential credential) {
        assertEquals(credential, uri.getCredential());
    }

    static Stream<Object[]> credentialTypesData() {
        return Stream.of(
                new Object[]{new ConnectionString("mongodb://jeff:123@localhost"),
                        createCredential("jeff", "admin", "123".toCharArray())},
                new Object[]{new ConnectionString("mongodb://jeff:123@localhost/?&authSource=test"),
                        createCredential("jeff", "test", "123".toCharArray())},
                new Object[]{new ConnectionString("mongodb://jeff:123@localhost/?authMechanism=MONGODB-CR"),
                        createCredential("jeff", "admin", "123".toCharArray())},
                new Object[]{new ConnectionString("mongodb://jeff:123@localhost/?authMechanism=MONGODB-CR&authSource=test"),
                        createCredential("jeff", "test", "123".toCharArray())},
                new Object[]{new ConnectionString("mongodb://jeff:123@localhost/?authMechanism=SCRAM-SHA-1"),
                        createScramSha1Credential("jeff", "admin", "123".toCharArray())},
                new Object[]{new ConnectionString("mongodb://jeff:123@localhost/?authMechanism=SCRAM-SHA-1&authSource=test"),
                        createScramSha1Credential("jeff", "test", "123".toCharArray())},
                new Object[]{new ConnectionString("mongodb://jeff:123@localhost/?authMechanism=SCRAM-SHA-256"),
                        createScramSha256Credential("jeff", "admin", "123".toCharArray())},
                new Object[]{new ConnectionString("mongodb://jeff:123@localhost/?authMechanism=SCRAM-SHA-256&authSource=test"),
                        createScramSha256Credential("jeff", "test", "123".toCharArray())},
                new Object[]{new ConnectionString("mongodb://jeff@localhost/?authMechanism=GSSAPI"),
                        createGSSAPICredential("jeff")},
                new Object[]{new ConnectionString("mongodb://jeff:123@localhost/?authMechanism=PLAIN"),
                        createPlainCredential("jeff", "$external", "123".toCharArray())},
                new Object[]{new ConnectionString("mongodb://jeff@localhost/?authMechanism=MONGODB-X509"),
                        createMongoX509Credential("jeff")},
                new Object[]{new ConnectionString("mongodb://localhost/?authMechanism=MONGODB-X509"),
                        createMongoX509Credential()},
                new Object[]{new ConnectionString("mongodb://jeff@localhost/?authMechanism=GSSAPI&gssapiServiceName=foo"),
                        createGSSAPICredential("jeff").withMechanismProperty("SERVICE_NAME", "foo")},
                new Object[]{new ConnectionString("mongodb://jeff@localhost/?authMechanism=GSSAPI"
                        + "&authMechanismProperties=SERVICE_NAME:foo"),
                        createGSSAPICredential("jeff").withMechanismProperty("SERVICE_NAME", "foo")},
                new Object[]{new ConnectionString("mongodb://jeff@localhost/?authMechanism=GSSAPI"
                        + "&authMechanismProperties=SERVICE_NAME :foo"),
                        createGSSAPICredential("jeff").withMechanismProperty("SERVICE_NAME", "foo")},
                new Object[]{new ConnectionString("mongodb://jeff@localhost/?authMechanism=GSSAPI"
                        + "&authMechanismProperties=SERVICE_NAME:foo,CANONICALIZE_HOST_NAME:true,SERVICE_REALM:AWESOME"),
                        createGSSAPICredential("jeff")
                                .withMechanismProperty("SERVICE_NAME", "foo")
                                .withMechanismProperty("CANONICALIZE_HOST_NAME", true)
                                .withMechanismProperty("SERVICE_REALM", "AWESOME")}
        );
    }

    @Test
    @DisplayName("should ignore authSource if there is no credential")
    void shouldIgnoreAuthSourceWithNoCredential() {
        assertNull(new ConnectionString("mongodb://localhost/?authSource=test").getCredential());
    }

    @Test
    @DisplayName("should ignore authMechanismProperties if there is no credential")
    void shouldIgnoreAuthMechanismPropertiesWithNoCredential() {
        assertNull(new ConnectionString("mongodb://localhost/?&authMechanismProperties=SERVICE_REALM:AWESOME")
                .getCredential());
    }

    @Test
    @DisplayName("should support thrown an IllegalArgumentException when given invalid authMechanismProperties")
    void shouldThrowForInvalidAuthMechanismProperties() {
        assertThrows(IllegalArgumentException.class, () -> new ConnectionString("mongodb://jeff@localhost/?"
                + "authMechanism=GSSAPI"
                + "&authMechanismProperties=SERVICE_NAME=foo,CANONICALIZE_HOST_NAME=true,SERVICE_REALM=AWESOME"));
        assertThrows(IllegalArgumentException.class, () -> new ConnectionString("mongodb://jeff@localhost/?"
                + "authMechanism=GSSAPI"
                + "&authMechanismProperties=SERVICE_NAMEbar"));
    }

    @ParameterizedTest
    @MethodSource("readPreferenceData")
    @DisplayName("should correct parse read preference")
    void shouldParseReadPreference(final ConnectionString uri, final ReadPreference readPreference) {
        assertEquals(readPreference, uri.getReadPreference());
    }

    static Stream<Object[]> readPreferenceData() {
        return Stream.of(
                new Object[]{new ConnectionString("mongodb://localhost"), null},
                new Object[]{new ConnectionString("mongodb://localhost/?readPreference=primary"), primary()},
                new Object[]{new ConnectionString("mongodb://localhost/?readPreference=secondary"), secondary()},
                new Object[]{new ConnectionString("mongodb://localhost/?readPreference=secondaryPreferred"),
                        secondaryPreferred()},
                new Object[]{new ConnectionString("mongodb://localhost/?readPreference=secondaryPreferred"
                        + "&readPreferenceTags=dc:ny,rack:1&readPreferenceTags=dc:ny&readPreferenceTags="),
                        secondaryPreferred(asList(new TagSet(asList(new Tag("dc", "ny"), new Tag("rack", "1"))),
                                new TagSet(asList(new Tag("dc", "ny"))), new TagSet()))},
                new Object[]{new ConnectionString("mongodb://localhost/?readPreference=secondary&maxStalenessSeconds=120"),
                        secondary(120000, MILLISECONDS)},
                new Object[]{new ConnectionString("mongodb://localhost/?readPreference=secondary&maxStalenessSeconds=0"),
                        secondary(0, MILLISECONDS)},
                new Object[]{new ConnectionString("mongodb://localhost/?readPreference=secondary&maxStalenessSeconds=-1"),
                        secondary()},
                new Object[]{new ConnectionString("mongodb://localhost/?readPreference=primary&maxStalenessSeconds=-1"),
                        primary()}
        );
    }

    @ParameterizedTest
    @MethodSource("readConcernData")
    @DisplayName("should correct parse read concern")
    void shouldParseReadConcern(final ConnectionString uri, final ReadConcern readConcern) {
        assertEquals(readConcern, uri.getReadConcern());
    }

    static Stream<Object[]> readConcernData() {
        return Stream.of(
                new Object[]{new ConnectionString("mongodb://localhost/"), null},
                new Object[]{new ConnectionString("mongodb://localhost/?readConcernLevel=local"), ReadConcern.LOCAL},
                new Object[]{new ConnectionString("mongodb://localhost/?readConcernLevel=majority"), ReadConcern.MAJORITY}
        );
    }

    @ParameterizedTest
    @MethodSource("compressorsData")
    @DisplayName("should parse compressors")
    void shouldParseCompressors(final ConnectionString uri, final MongoCompressor compressor) {
        assertEquals(Collections.singletonList(compressor), uri.getCompressorList());
    }

    static Stream<Object[]> compressorsData() {
        return Stream.of(
                new Object[]{new ConnectionString("mongodb://localhost/?compressors=zlib"), createZlibCompressor()},
                new Object[]{new ConnectionString("mongodb://localhost/?compressors=zlib&zlibCompressionLevel=5"),
                        createZlibCompressor().withProperty(LEVEL, 5)},
                new Object[]{new ConnectionString("mongodb://localhost/?compressors=zstd"), createZstdCompressor()}
        );
    }

    @ParameterizedTest
    @MethodSource("equalConnectionStringsData")
    @DisplayName("should be equal to another instance with the same string values")
    void shouldBeEqual(final ConnectionString uri1, final ConnectionString uri2) {
        assertEquals(uri1, uri2);
        assertEquals(uri1.hashCode(), uri2.hashCode());
    }

    static Stream<Object[]> equalConnectionStringsData() {
        return Stream.of(
                new Object[]{new ConnectionString("mongodb://user:pass@host1:1/"),
                        new ConnectionString("mongodb://user:pass@host1:1/")},
                new Object[]{new ConnectionString("mongodb://user:pass@host1:1,host2:2,host3:3/bar"),
                        new ConnectionString("mongodb://user:pass@host3:3,host1:1,host2:2/bar")},
                new Object[]{new ConnectionString("mongodb://localhost/?readPreference=secondaryPreferred"
                        + "&readPreferenceTags=dc:ny,rack:1&readPreferenceTags=dc:ny&readPreferenceTags="),
                        new ConnectionString("mongodb://localhost/?readPreference=secondaryPreferred"
                                + "&readPreferenceTags=dc:ny, rack:1&readPreferenceTags=dc:ny&readPreferenceTags=")},
                new Object[]{new ConnectionString("mongodb://localhost/?readPreference=secondaryPreferred"),
                        new ConnectionString("mongodb://localhost/?readPreference=secondaryPreferred")},
                new Object[]{new ConnectionString("mongodb://ross:123@localhost/?authMechanism=SCRAM-SHA-1"),
                        new ConnectionString("mongodb://ross:123@localhost/?authMechanism=SCRAM-SHA-1")},
                new Object[]{new ConnectionString("mongodb://ross:123@localhost/?"
                        + "proxyHost=proxy.com&proxyPort=1080&proxyUsername=username&proxyPassword=password"),
                        new ConnectionString("mongodb://ross:123@localhost/?"
                                + "proxyHost=proxy.com&proxyPort=1080&proxyUsername=username&proxyPassword=password")},
                new Object[]{new ConnectionString("mongodb://localhost/db.coll"
                        + "?minPoolSize=5;maxPoolSize=10;waitQueueTimeoutMS=150;maxIdleTimeMS=200;"
                        + "maxLifeTimeMS=300;replicaSet=test;maxConnecting=1;connectTimeoutMS=2500;"
                        + "socketTimeoutMS=5500;safe=false;w=1;wtimeout=2500;fsync=true;readPreference=primary;"
                        + "directConnection=true;ssl=true"),
                        new ConnectionString("mongodb://localhost/db.coll?minPoolSize=5;maxPoolSize=10;"
                                + "waitQueueTimeoutMS=150;maxIdleTimeMS=200&maxLifeTimeMS=300&replicaSet=test;"
                                + "maxConnecting=1;connectTimeoutMS=2500;socketTimeoutMS=5500&safe=false&w=1;"
                                + "wtimeout=2500;fsync=true&directConnection=true&readPreference=primary;ssl=true")}
        );
    }

    @ParameterizedTest
    @MethodSource("notEqualConnectionStringsData")
    @DisplayName("should be not equal to another ConnectionString with the different string values")
    void shouldNotBeEqual(final ConnectionString uri1, final ConnectionString uri2) {
        assertNotEquals(uri1, uri2);
        assertNotEquals(uri1.hashCode(), uri2.hashCode());
    }

    static Stream<Object[]> notEqualConnectionStringsData() {
        return Stream.of(
                new Object[]{new ConnectionString("mongodb://user:pass@host1:1/"),
                        new ConnectionString("mongodb://user:pass@host1:2/")},
                new Object[]{new ConnectionString("mongodb://user:pass@host1:1,host2:2,host3:3/bar"),
                        new ConnectionString("mongodb://user:pass@host1:1,host2:2,host4:4/bar")},
                new Object[]{new ConnectionString("mongodb://localhost/?readPreference=secondaryPreferred"),
                        new ConnectionString("mongodb://localhost/?readPreference=secondary")},
                new Object[]{new ConnectionString("mongodb://localhost/?readPreference=secondaryPreferred"
                        + "&readPreferenceTags=dc:ny,rack:1&readPreferenceTags=dc:ny&readPreferenceTags=&maxConnecting=1"),
                        new ConnectionString("mongodb://localhost/?readPreference=secondaryPreferred"
                                + "&readPreferenceTags=dc:ny&readPreferenceTags=dc:ny, rack:1&readPreferenceTags="
                                + "&maxConnecting=2")},
                new Object[]{new ConnectionString("mongodb://ross:123@localhost/?authMechanism=SCRAM-SHA-1"),
                        new ConnectionString("mongodb://ross:123@localhost/?authMechanism=GSSAPI")},
                new Object[]{new ConnectionString("mongodb://ross:123@localhost/?proxyHost=proxy.com"),
                        new ConnectionString("mongodb://ross:123@localhost/?proxyHost=1proxy.com")},
                new Object[]{new ConnectionString("mongodb://ross:123@localhost/?proxyHost=proxy.com&proxyPort=1080"),
                        new ConnectionString("mongodb://ross:123@localhost/?proxyHost=proxy.com1.com&proxyPort=1081")},
                new Object[]{new ConnectionString("mongodb://ross:123@localhost/?"
                        + "proxyHost=proxy.com&proxyPassword=password&proxyUsername=username"),
                        new ConnectionString("mongodb://ross:123@localhost/?"
                                + "proxyHost=proxy.com&proxyPassword=password1&proxyUsername=username")}
        );
    }

    @Test
    @DisplayName("should recognize SRV protocol")
    void shouldRecognizeSrvProtocol() {
        ConnectionString connectionString = new ConnectionString("mongodb+srv://test5.test.build.10gen.cc");
        assertTrue(connectionString.isSrvProtocol());
        assertEquals(Collections.singletonList("test5.test.build.10gen.cc"), connectionString.getHosts());
    }

    @ParameterizedTest
    @MethodSource("srvSslData")
    @DisplayName("should set sslEnabled property with SRV protocol")
    void shouldSetSslEnabledWithSrv(final ConnectionString connectionString, final Boolean sslEnabled) {
        assertEquals(sslEnabled, connectionString.getSslEnabled());
    }

    static Stream<Object[]> srvSslData() {
        return Stream.of(
                new Object[]{new ConnectionString("mongodb+srv://test5.test.build.10gen.cc"), true},
                new Object[]{new ConnectionString("mongodb+srv://test5.test.build.10gen.cc/?tls=true"), true},
                new Object[]{new ConnectionString("mongodb+srv://test5.test.build.10gen.cc/?ssl=true"), true},
                new Object[]{new ConnectionString("mongodb+srv://test5.test.build.10gen.cc/?tls=false"), false},
                new Object[]{new ConnectionString("mongodb+srv://test5.test.build.10gen.cc/?ssl=false"), false}
        );
    }

    @Test
    @DisplayName("should use authSource from TXT record")
    void shouldUseAuthSourceFromTxtRecord() {
        ConnectionString uri = new ConnectionString("mongodb+srv://bob:pwd@test5.test.build.10gen.cc/");
        assertEquals(createCredential("bob", "thisDB", "pwd".toCharArray()), uri.getCredential());
    }

    @Test
    @DisplayName("should override authSource from TXT record with authSource from connectionString")
    void shouldOverrideAuthSourceFromTxtRecord() {
        ConnectionString uri = new ConnectionString("mongodb+srv://bob:pwd@test5.test.build.10gen.cc/?authSource=otherDB");
        assertEquals(createCredential("bob", "otherDB", "pwd".toCharArray()), uri.getCredential());
    }

    @Test
    @DisplayName("should use DnsClient to resolve TXT record")
    void shouldUseDnsClientToResolveTxtRecord() {
        DnsClient dnsClient = (name, type) -> Collections.singletonList("replicaSet=java");
        ConnectionString connectionString = new ConnectionString("mongodb+srv://free-java.mongodb-dev.net", dnsClient);
        assertEquals("java", connectionString.getRequiredReplicaSetName());
    }
}
