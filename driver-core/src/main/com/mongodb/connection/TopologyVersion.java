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

import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonNumber;
import org.bson.BsonObjectId;
import org.bson.types.ObjectId;

import java.util.Objects;

/**
 * A class that represents a MongoDB topology version
 *
 * @mongodb.server.release 4.4
 * @since 4.1
 */
public class TopologyVersion implements Comparable<TopologyVersion> {
    private static final BsonObjectId DEFAULT_OBJECT_ID = new BsonObjectId(new ObjectId("000000000000000000000000"));
    private final BsonObjectId processId;
    private final BsonInt64 counter;

    /**
     * Construct an instance
     * @param topologyVersion the topology version document
     */
    public TopologyVersion(final BsonDocument topologyVersion) {
        this.processId = topologyVersion.getObjectId("processId", DEFAULT_OBJECT_ID);
        BsonNumber counter = topologyVersion.getNumber("counter", new BsonInt64(0));
        this.counter = counter.isInt64() ? counter.asInt64() : new BsonInt64(counter.longValue());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TopologyVersion that = (TopologyVersion) o;
        return processId.equals(that.processId) && counter.equals(that.counter);
    }

    @Override
    public String toString() {
        return "TopologyVersion{"
                + "processId=" + processId
                + ", counter=" + counter
                + '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(processId, counter);
    }

    @Override
    public int compareTo(@Nullable final TopologyVersion that) {
        // Assume greater
        if (that == null) {
            return 1;
        }

        if (processId.equals(that.processId)) {
            return counter.compareTo(that.counter);
        }

        // Assume greater
        return 1;
    }
}
