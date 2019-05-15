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

import com.mongodb.connection.ServerVersion;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.List;

import static com.mongodb.ClusterFixture.getVersionList;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.isStandalone;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public final class JsonTestServerVersionChecker {

    private static final List<String> TOPOLOGY_TYPES = asList("sharded", "replicaset", "single");
    private static ServerVersion serverVersion;

    public static boolean canRunTests(final BsonDocument document) {
        ServerVersion serverVersion = getServerVersion();
        if (document.containsKey("minServerVersion")
                && serverVersion.compareTo(getServerVersionForField("minServerVersion", document)) < 0) {
            return false;
        }
        if (document.containsKey("maxServerVersion")
                && serverVersion.compareTo(getServerVersionForField("maxServerVersion", document)) > 0) {
            return false;
        }
        if (document.containsKey("topology")) {
            BsonArray topologyTypes = document.getArray("topology");
            for (BsonValue type : topologyTypes) {
                String typeString = type.asString().getValue();
                if (typeString.equals("sharded") && isSharded()) {
                    return true;
                } else if (typeString.equals("replicaset") && isDiscoverableReplicaSet()) {
                    return true;
                } else if (typeString.equals("single") && isStandalone()) {
                    return true;
                } else if (!TOPOLOGY_TYPES.contains(typeString)) {
                    throw new IllegalArgumentException(format("Unexpected topology type: '%s'", typeString));
                }
            }
            return false;
        }

        if (document.containsKey("runOn")) {
            return canRunTests(document.getArray("runOn"));
        }
        return true;
    }

    public static boolean canRunTests(final BsonArray runOn) {
        boolean topologyFound = false;
        for (BsonValue info : runOn) {
            topologyFound = canRunTests(info.asDocument());
            if (topologyFound) {
                break;
            }
        }
        return topologyFound;
    }

    private static ServerVersion getServerVersion() {
        if (serverVersion == null) {
            serverVersion = ClusterFixture.getServerVersion();
        }
        return serverVersion;
    }

    private static ServerVersion getServerVersionForField(final String fieldName, final BsonDocument document) {
        return new ServerVersion(getVersionList(document.getString(fieldName).getValue()));
    }


    private JsonTestServerVersionChecker() {
    }
}
