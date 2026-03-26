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

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ClusterClockTest {

    @Test
    void shouldAdvanceClusterTime() {
        BsonDocument firstClusterTime = new BsonDocument("clusterTime", new BsonTimestamp(42, 1));
        BsonDocument secondClusterTime = new BsonDocument("clusterTime", new BsonTimestamp(52, 1));
        BsonDocument olderClusterTime = new BsonDocument("clusterTime", new BsonTimestamp(22, 1));

        ClusterClock clock = new ClusterClock();

        assertNull(clock.getCurrent());

        clock.advance(null);

        assertNull(clock.getCurrent());
        assertEquals(firstClusterTime, clock.greaterOf(firstClusterTime));

        clock.advance(firstClusterTime);

        assertEquals(firstClusterTime, clock.getCurrent());
        assertEquals(secondClusterTime, clock.greaterOf(secondClusterTime));

        clock.advance(secondClusterTime);

        assertEquals(secondClusterTime, clock.getCurrent());
        assertEquals(secondClusterTime, clock.greaterOf(olderClusterTime));

        clock.advance(olderClusterTime);

        assertEquals(secondClusterTime, clock.getCurrent());
    }
}
