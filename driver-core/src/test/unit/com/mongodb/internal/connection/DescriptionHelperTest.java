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

import com.mongodb.MongoClientException;
import com.mongodb.ServerAddress;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import com.mongodb.connection.TopologyVersion;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.connection.DescriptionHelper.createConnectionDescription;
import static com.mongodb.internal.connection.DescriptionHelper.createServerDescription;
import static com.mongodb.internal.connection.MessageHelper.LEGACY_HELLO_LOWER;
import static org.bson.BsonDocument.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DescriptionHelperTest {

    private final ServerAddress serverAddress = new ServerAddress("localhost", 27018);
    private final int roundTripTime = 5000;

    @BeforeEach
    void setUp() {
        Time.makeTimeConstant();
    }

    @AfterEach
    void tearDown() {
        Time.makeTimeMove();
    }

    @Test
    void connectionDescriptionShouldReflectHelloResult() {
        ConnectionId connectionId = new ConnectionId(new ServerId(new ClusterId(), serverAddress));

        assertEquals(
                new ConnectionDescription(connectionId, 6, ServerType.STANDALONE, 1000, 16777216, 48000000,
                        Arrays.asList()),
                createConnectionDescription(ClusterConnectionMode.SINGLE, connectionId,
                        parse("{"
                                + LEGACY_HELLO_LOWER + ": true,"
                                + "maxBsonObjectSize : 16777216,"
                                + "maxMessageSizeBytes : 48000000,"
                                + "maxWriteBatchSize : 1000,"
                                + "localTime : ISODate(\"2015-03-04T23:03:45.848Z\"),"
                                + "maxWireVersion : 6,"
                                + "minWireVersion : 0,"
                                + "ok : 1"
                                + "}")));

        assertEquals(
                new ConnectionDescription(connectionId, 6, ServerType.STANDALONE, 1000, 16777216, 48000000,
                        Arrays.asList())
                        .withConnectionId(connectionId.withServerValue(1004)),
                createConnectionDescription(ClusterConnectionMode.SINGLE, connectionId,
                        parse("{"
                                + LEGACY_HELLO_LOWER + ": true,"
                                + "maxBsonObjectSize : 16777216,"
                                + "maxMessageSizeBytes : 48000000,"
                                + "maxWriteBatchSize : 1000,"
                                + "localTime : ISODate(\"2015-03-04T23:03:45.848Z\"),"
                                + "maxWireVersion : 6,"
                                + "minWireVersion : 0,"
                                + "connectionId : 1004,"
                                + "ok : 1"
                                + "}")));
    }

    @Test
    void connectionDescriptionShouldReflectLegacyHelloResultFromLoadBalancer() {
        ConnectionId connectionId = new ConnectionId(new ServerId(new ClusterId(), serverAddress));
        ObjectId serviceId = new ObjectId();

        assertEquals(
                new ConnectionDescription(connectionId, 13, ServerType.SHARD_ROUTER, 1000, 16777216, 48000000,
                        Arrays.asList())
                        .withConnectionId(connectionId.withServerValue(1004))
                        .withServiceId(serviceId),
                createConnectionDescription(ClusterConnectionMode.LOAD_BALANCED, connectionId,
                        parse("{"
                                + LEGACY_HELLO_LOWER + ": true,"
                                + "msg : \"isdbgrid\","
                                + "maxBsonObjectSize : 16777216,"
                                + "maxMessageSizeBytes : 48000000,"
                                + "maxWriteBatchSize : 1000,"
                                + "localTime : ISODate(\"2015-03-04T23:55:18.505Z\"),"
                                + "maxWireVersion : 13,"
                                + "minWireVersion : 0,"
                                + "connectionId : 1004,"
                                + "serviceId: {$oid : \"" + serviceId.toHexString() + "\"},"
                                + "ok : 1"
                                + "}")));

        MongoClientException e = assertThrows(MongoClientException.class, () ->
                createConnectionDescription(ClusterConnectionMode.LOAD_BALANCED, connectionId,
                        parse("{"
                                + LEGACY_HELLO_LOWER + ": true,"
                                + "msg : \"isdbgrid\","
                                + "maxBsonObjectSize : 16777216,"
                                + "maxMessageSizeBytes : 48000000,"
                                + "maxWriteBatchSize : 1000,"
                                + "localTime : ISODate(\"2015-03-04T23:55:18.505Z\"),"
                                + "maxWireVersion : 13,"
                                + "minWireVersion : 0,"
                                + "connectionId : 1004,"
                                + "ok : 1"
                                + "}")));
        assertEquals("Driver attempted to initialize in load balancing mode, but the server does not support this mode",
                e.getMessage());
    }

    @Test
    void connectionDescriptionShouldReflectLegacyHelloResultWithCompressors() {
        ConnectionId connectionId = new ConnectionId(new ServerId(new ClusterId(), serverAddress));

        assertEquals(
                new ConnectionDescription(connectionId, 6, ServerType.STANDALONE, 1000, 16777216, 48000000,
                        Arrays.asList("zlib", "snappy")),
                createConnectionDescription(ClusterConnectionMode.SINGLE, connectionId,
                        parse("{"
                                + LEGACY_HELLO_LOWER + ": true,"
                                + "maxBsonObjectSize : 16777216,"
                                + "maxMessageSizeBytes : 48000000,"
                                + "maxWriteBatchSize : 1000,"
                                + "localTime : ISODate(\"2015-03-04T23:03:45.848Z\"),"
                                + "maxWireVersion : 6,"
                                + "minWireVersion : 0,"
                                + "compression : [\"zlib\", \"snappy\"],"
                                + "ok : 1"
                                + "}")));
    }

    @Test
    void serverDescriptionShouldReflectNotOkLegacyHelloResult() {
        assertEquals(
                ServerDescription.builder()
                        .ok(false)
                        .address(serverAddress)
                        .state(ServerConnectionState.CONNECTED)
                        .type(ServerType.UNKNOWN)
                        .build(),
                createServerDescription(serverAddress, parse("{ok : 0}"), roundTripTime, 0));
    }

    @Test
    void serverDescriptionShouldReflectLastUpdateTime() {
        assertEquals(Time.CONSTANT_TIME,
                createServerDescription(serverAddress, parse("{ ok : 1 }"), roundTripTime, 0)
                        .getLastUpdateTime(TimeUnit.NANOSECONDS));
    }

    @Test
    void serverDescriptionShouldReflectRoundTripNanos() {
        assertEquals(
                ServerDescription.builder()
                        .ok(true)
                        .address(serverAddress)
                        .state(ServerConnectionState.CONNECTED)
                        .maxWireVersion(3)
                        .maxDocumentSize(16777216)
                        .type(ServerType.STANDALONE)
                        .roundTripTime(roundTripTime, TimeUnit.NANOSECONDS)
                        .build().getRoundTripTimeNanos(),
                createServerDescription(serverAddress,
                        parse("{"
                                + LEGACY_HELLO_LOWER + ": true,"
                                + "maxBsonObjectSize : 16777216,"
                                + "maxMessageSizeBytes : 48000000,"
                                + "maxWriteBatchSize : 1000,"
                                + "localTime : ISODate(\"2015-03-04T23:03:45.848Z\"),"
                                + "maxWireVersion : 3,"
                                + "minWireVersion : 0,"
                                + "ok : 1"
                                + "}"), roundTripTime, 0).getRoundTripTimeNanos());
    }

    @Test
    void serverDescriptionShouldReflectLegacyHelloResultFromStandalone() {
        assertEquals(
                ServerDescription.builder()
                        .ok(true)
                        .address(serverAddress)
                        .state(ServerConnectionState.CONNECTED)
                        .maxWireVersion(3)
                        .maxDocumentSize(16777216)
                        .type(ServerType.STANDALONE)
                        .build(),
                createServerDescription(serverAddress,
                        parse("{"
                                + LEGACY_HELLO_LOWER + ": true,"
                                + "maxBsonObjectSize : 16777216,"
                                + "maxMessageSizeBytes : 48000000,"
                                + "maxWriteBatchSize : 1000,"
                                + "localTime : ISODate(\"2015-03-04T23:03:45.848Z\"),"
                                + "maxWireVersion : 3,"
                                + "minWireVersion : 0,"
                                + "ok : 1"
                                + "}"), roundTripTime, 0));
    }

    @Test
    void serverDescriptionShouldReflectLegacyHelloResultFromSecondary() {
        assertEquals(
                ServerDescription.builder()
                        .ok(true)
                        .address(new ServerAddress("localhost", 27018))
                        .state(ServerConnectionState.CONNECTED)
                        .maxWireVersion(3)
                        .maxDocumentSize(16777216)
                        .type(ServerType.REPLICA_SET_SECONDARY)
                        .setName("replset")
                        .canonicalAddress("localhost:27017")
                        .hosts(new HashSet<>(Arrays.asList("localhost:27017", "localhost:27018", "localhost:27019")))
                        .arbiters(new HashSet<>(Arrays.asList("localhost:27020")))
                        .build(),
                createServerDescription(new ServerAddress("localhost", 27018),
                        parse("{"
                                + "\"setName\" : \"replset\","
                                + "\"" + LEGACY_HELLO_LOWER + "\": false,"
                                + "\"secondary\" : true,"
                                + "\"hosts\" : ["
                                + "\"localhost:27017\","
                                + "\"localhost:27019\","
                                + "\"localhost:27018\""
                                + "],"
                                + "\"arbiters\" : ["
                                + "\"localhost:27020\""
                                + "],"
                                + "\"me\" : \"localhost:27017\","
                                + "\"maxBsonObjectSize\" : 16777216,"
                                + "\"maxMessageSizeBytes\" : 48000000,"
                                + "\"maxWriteBatchSize\" : 1000,"
                                + "\"localTime\" : ISODate(\"2015-03-04T23:14:07.338Z\"),"
                                + "\"maxWireVersion\" : 3,"
                                + "\"minWireVersion\" : 0,"
                                + "\"ok\" : 1"
                                + "}"), roundTripTime, 0));
    }

    @Test
    void serverDescriptionShouldReflectLegacyHelloResultWithLastWriteDate() {
        assertEquals(
                ServerDescription.builder()
                        .ok(true)
                        .address(new ServerAddress("localhost", 27018))
                        .state(ServerConnectionState.CONNECTED)
                        .maxWireVersion(5)
                        .lastWriteDate(new Date(1457133247338L))
                        .maxDocumentSize(16777216)
                        .type(ServerType.REPLICA_SET_SECONDARY)
                        .setName("replset")
                        .canonicalAddress("localhost:27017")
                        .hosts(new HashSet<>(Arrays.asList("localhost:27017", "localhost:27018", "localhost:27019")))
                        .arbiters(new HashSet<>(Arrays.asList("localhost:27020")))
                        .build(),
                createServerDescription(new ServerAddress("localhost", 27018),
                        parse("{"
                                + "\"setName\" : \"replset\","
                                + "\"" + LEGACY_HELLO_LOWER + "\" : false,"
                                + "\"secondary\" : true,"
                                + "\"hosts\" : ["
                                + "\"localhost:27017\","
                                + "\"localhost:27019\","
                                + "\"localhost:27018\""
                                + "],"
                                + "\"arbiters\" : ["
                                + "\"localhost:27020\""
                                + "],"
                                + "\"me\" : \"localhost:27017\","
                                + "\"maxBsonObjectSize\" : 16777216,"
                                + "\"maxMessageSizeBytes\" : 48000000,"
                                + "\"maxWriteBatchSize\" : 1000,"
                                + "\"localTime\" : ISODate(\"2015-03-04T23:14:07.338Z\"),"
                                + "\"maxWireVersion\" : 5,"
                                + "\"minWireVersion\" : 0,"
                                + "\"lastWrite\" : { \"lastWriteDate\" : ISODate(\"2016-03-04T23:14:07.338Z\") },"
                                + "\"ok\" : 1"
                                + "}"), roundTripTime, 0));
    }

    @Test
    void serverDescriptionShouldReflectLegacyHelloResultFromPrimary() {
        ObjectId electionId = new ObjectId();
        ObjectId topologyVersionProcessId = new ObjectId();

        ServerDescription serverDescription = createServerDescription(serverAddress,
                parse("{"
                        + "\"setName\" : \"replset\","
                        + "\"setVersion\" : 1,"
                        + "\"" + LEGACY_HELLO_LOWER + "\" : true,"
                        + "\"secondary\" : false,"
                        + "\"hosts\" : ["
                        + "\"localhost:27017\","
                        + "\"localhost:27019\","
                        + "\"localhost:27018\""
                        + "],"
                        + "\"arbiters\" : ["
                        + "\"localhost:27020\""
                        + "],"
                        + "\"primary\" : \"localhost:27017\","
                        + "\"me\" : \"localhost:27017\","
                        + "\"maxBsonObjectSize\" : 16777216,"
                        + "\"maxMessageSizeBytes\" : 48000000,"
                        + "\"maxWriteBatchSize\" : 1000,"
                        + "\"localTime\" : ISODate(\"2015-03-04T23:24:18.452Z\"),"
                        + "\"maxWireVersion\" : 3,"
                        + "\"minWireVersion\" : 0,"
                        + "\"electionId\" : {$oid : \"" + electionId.toHexString() + "\" },"
                        + "\"topologyVersion\" : {"
                        + "   processId: {$oid : \"" + topologyVersionProcessId.toHexString() + "\"},"
                        + "   counter: {$numberLong : \"42\"}"
                        + "},"
                        + "\"setVersion\" : 2,"
                        + "tags : { \"dc\" : \"east\", \"use\" : \"production\" },"
                        + "\"ok\" : 1"
                        + "}"), roundTripTime, 0);

        assertEquals(
                ServerDescription.builder()
                        .ok(true)
                        .address(serverAddress)
                        .state(ServerConnectionState.CONNECTED)
                        .maxWireVersion(3)
                        .maxDocumentSize(16777216)
                        .electionId(electionId)
                        .setVersion(2)
                        .topologyVersion(new TopologyVersion(topologyVersionProcessId, 42))
                        .type(ServerType.REPLICA_SET_PRIMARY)
                        .setName("replset")
                        .primary("localhost:27017")
                        .canonicalAddress("localhost:27017")
                        .hosts(new HashSet<>(Arrays.asList("localhost:27017", "localhost:27018", "localhost:27019")))
                        .arbiters(new HashSet<>(Arrays.asList("localhost:27020")))
                        .tagSet(new TagSet(Arrays.asList(new Tag("dc", "east"), new Tag("use", "production"))))
                        .build(),
                serverDescription);
    }

    @Test
    void serverDescriptionShouldReflectLegacyHelloResultFromArbiter() {
        assertEquals(
                ServerDescription.builder()
                        .ok(true)
                        .address(serverAddress)
                        .state(ServerConnectionState.CONNECTED)
                        .maxWireVersion(3)
                        .maxDocumentSize(16777216)
                        .type(ServerType.REPLICA_SET_ARBITER)
                        .setName("replset")
                        .primary("localhost:27017")
                        .canonicalAddress("localhost:27020")
                        .hosts(new HashSet<>(Arrays.asList("localhost:27017", "localhost:27018", "localhost:27019")))
                        .arbiters(new HashSet<>(Arrays.asList("localhost:27020")))
                        .build(),
                createServerDescription(serverAddress,
                        parse("{"
                                + "\"setName\" : \"replset\","
                                + "\"" + LEGACY_HELLO_LOWER + "\": false,"
                                + "\"secondary\" : false,"
                                + "\"hosts\" : ["
                                + "\"localhost:27019\","
                                + "\"localhost:27018\","
                                + "\"localhost:27017\""
                                + "],"
                                + "\"arbiters\" : ["
                                + "\"localhost:27020\""
                                + "],"
                                + "\"primary\" : \"localhost:27017\","
                                + "\"arbiterOnly\" : true,"
                                + "\"me\" : \"localhost:27020\","
                                + "\"maxBsonObjectSize\" : 16777216,"
                                + "\"maxMessageSizeBytes\" : 48000000,"
                                + "\"maxWriteBatchSize\" : 1000,"
                                + "\"localTime\" : ISODate(\"2015-03-04T23:27:55.568Z\"),"
                                + "\"maxWireVersion\" : 3,"
                                + "\"minWireVersion\" : 0,"
                                + "\"ok\" : 1"
                                + "}"), roundTripTime, 0));
    }

    @Test
    void serverDescriptionShouldReflectLegacyHelloResultFromOther() {
        ServerAddress serverAddressOfHidden = new ServerAddress("localhost", 27020);

        ServerDescription serverDescription = createServerDescription(serverAddressOfHidden,
                parse("{"
                        + "\"setName\" : \"replset\","
                        + "\"" + LEGACY_HELLO_LOWER + "\": false,"
                        + "\"secondary\" : false,"
                        + "\"hosts\" : ["
                        + "\"localhost:27019\","
                        + "\"localhost:27018\","
                        + "\"localhost:27017\""
                        + "],"
                        + "\"arbiters\" : ["
                        + "\"localhost:27021\""
                        + "],"
                        + "\"primary\" : \"localhost:27017\","
                        + "\"arbiterOnly\" : false,"
                        + "\"me\" : \"localhost:27020\","
                        + "\"maxBsonObjectSize\" : 16777216,"
                        + "\"maxMessageSizeBytes\" : 48000000,"
                        + "\"maxWriteBatchSize\" : 1000,"
                        + "\"localTime\" : ISODate(\"2015-03-04T23:27:55.568Z\"),"
                        + "\"maxWireVersion\" : 3,"
                        + "\"minWireVersion\" : 0,"
                        + "\"ok\" : 1"
                        + "}"), roundTripTime, 0);

        assertEquals(
                ServerDescription.builder()
                        .ok(true)
                        .address(serverAddressOfHidden)
                        .state(ServerConnectionState.CONNECTED)
                        .maxWireVersion(3)
                        .maxDocumentSize(16777216)
                        .type(ServerType.REPLICA_SET_OTHER)
                        .setName("replset")
                        .primary("localhost:27017")
                        .canonicalAddress("localhost:27020")
                        .hosts(new HashSet<>(Arrays.asList("localhost:27017", "localhost:27018", "localhost:27019")))
                        .arbiters(new HashSet<>(Arrays.asList("localhost:27021")))
                        .build(),
                serverDescription);
    }

    @Test
    void serverDescriptionShouldReflectLegacyHelloResultFromHidden() {
        ServerAddress serverAddressOfHidden = new ServerAddress("localhost", 27020);

        assertEquals(
                ServerDescription.builder()
                        .ok(true)
                        .address(serverAddressOfHidden)
                        .state(ServerConnectionState.CONNECTED)
                        .maxWireVersion(3)
                        .maxDocumentSize(16777216)
                        .type(ServerType.REPLICA_SET_OTHER)
                        .setName("replset")
                        .primary("localhost:27017")
                        .canonicalAddress("localhost:27020")
                        .hosts(new HashSet<>(Arrays.asList("localhost:27017", "localhost:27018", "localhost:27019")))
                        .arbiters(new HashSet<>(Arrays.asList("localhost:27021")))
                        .build(),
                createServerDescription(serverAddressOfHidden,
                        parse("{"
                                + "\"setName\" : \"replset\","
                                + "\"" + LEGACY_HELLO_LOWER + "\": false,"
                                + "\"secondary\" : true,"
                                + "\"hidden\" : true,"
                                + "\"hosts\" : ["
                                + "\"localhost:27019\","
                                + "\"localhost:27018\","
                                + "\"localhost:27017\""
                                + "],"
                                + "\"arbiters\" : ["
                                + "\"localhost:27021\""
                                + "],"
                                + "\"primary\" : \"localhost:27017\","
                                + "\"arbiterOnly\" : false,"
                                + "\"me\" : \"localhost:27020\","
                                + "\"maxBsonObjectSize\" : 16777216,"
                                + "\"maxMessageSizeBytes\" : 48000000,"
                                + "\"maxWriteBatchSize\" : 1000,"
                                + "\"localTime\" : ISODate(\"2015-03-04T23:27:55.568Z\"),"
                                + "\"maxWireVersion\" : 3,"
                                + "\"minWireVersion\" : 0,"
                                + "\"ok\" : 1"
                                + "}"), roundTripTime, 0));
    }

    @Test
    void serverDescriptionShouldReflectLegacyHelloResultFromGhost() {
        assertEquals(
                ServerDescription.builder()
                        .ok(true)
                        .address(serverAddress)
                        .state(ServerConnectionState.CONNECTED)
                        .canonicalAddress("localhost:27020")
                        .maxWireVersion(3)
                        .maxDocumentSize(16777216)
                        .type(ServerType.REPLICA_SET_GHOST)
                        .setName("replset")
                        .build(),
                createServerDescription(serverAddress,
                        parse("{"
                                + "\"setName\" : \"replset\","
                                + "\"" + LEGACY_HELLO_LOWER + "\": false,"
                                + "\"secondary\" : false,"
                                + "\"arbiterOnly\" : false,"
                                + "\"me\" : \"localhost:27020\","
                                + "\"maxBsonObjectSize\" : 16777216,"
                                + "\"maxMessageSizeBytes\" : 48000000,"
                                + "\"maxWriteBatchSize\" : 1000,"
                                + "\"localTime\" : ISODate(\"2015-03-04T23:27:55.568Z\"),"
                                + "\"maxWireVersion\" : 3,"
                                + "\"minWireVersion\" : 0,"
                                + "\"ok\" : 1"
                                + "}"), roundTripTime, 0));
    }

    @Test
    void serverDescriptionShouldReflectLegacyHelloResultFromShardRouter() {
        assertEquals(
                ServerDescription.builder()
                        .ok(true)
                        .address(serverAddress)
                        .state(ServerConnectionState.CONNECTED)
                        .maxWireVersion(3)
                        .maxDocumentSize(16777216)
                        .type(ServerType.SHARD_ROUTER)
                        .build(),
                createServerDescription(serverAddress,
                        parse("{"
                                + "\"" + LEGACY_HELLO_LOWER + "\": true,"
                                + "\"msg\" : \"isdbgrid\","
                                + "\"maxBsonObjectSize\" : 16777216,"
                                + "\"maxMessageSizeBytes\" : 48000000,"
                                + "\"maxWriteBatchSize\" : 1000,"
                                + "\"localTime\" : ISODate(\"2015-03-04T23:55:18.505Z\"),"
                                + "\"maxWireVersion\" : 3,"
                                + "\"minWireVersion\" : 0,"
                                + "\"ok\" : 1"
                                + "}"), roundTripTime, 0));
    }
}
