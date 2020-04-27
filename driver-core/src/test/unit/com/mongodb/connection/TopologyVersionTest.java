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

package com.mongodb.connection;

import org.bson.BsonDocument;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TopologyVersionTest {

    @Test
    public void testComparison() {
        TopologyVersion topologyVersion = new TopologyVersion(
                BsonDocument.parse("{processId: {$oid: '000000000000000000000001'}, counter: {'$numberLong': '1'}}"));
        TopologyVersion topologyVersion2 = new TopologyVersion(
                BsonDocument.parse("{processId: {$oid: '000000000000000000000001'}, counter: {'$numberLong': '2'}}"));
        TopologyVersion topologyVersion3 = new TopologyVersion(
                BsonDocument.parse("{processId: {$oid: '000000000000000000000003'}, counter: {'$numberLong': '1'}}"));

        assertEquals(-1, topologyVersion.compareTo(topologyVersion2));
        assertEquals(0, topologyVersion.compareTo(topologyVersion));
        assertEquals(1, topologyVersion.compareTo(null));
        assertEquals(1, topologyVersion2.compareTo(topologyVersion));
        assertEquals(1, topologyVersion3.compareTo(topologyVersion));
    }
}
