/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

import org.bson.BsonDocument;
import org.bson.BsonString;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * The readConcern option allows clients to choose a level of isolation for their reads.
 *
 * The WiredTiger storage engine, introduces the readConcern option for replica sets and replica set shards.  Allowing clients to choose a
 * level of isolation for their reads.
 *
 * @mongodb.server.release 3.2
 * @since 3.2
 */
public final class ReadConcern {
    private final ReadConcernLevel readConcernLevel;

    /**
     * Construct a new read concern
     *
     * @param readConcernLevel the read concern level
     */
    public ReadConcern(final ReadConcernLevel readConcernLevel) {
        this.readConcernLevel = notNull("readConcernLevel", readConcernLevel);
    }

    /**
     * Use the servers default read concern.
     */
    public static final ReadConcern DEFAULT = new ReadConcern(ReadConcernLevel.DEFAULT);

    /**
     * Return the node's most recent copy of data. Provides no guarantee that the data has been written to a majority of the nodes.
     */
    public static final ReadConcern LOCAL = new ReadConcern(ReadConcernLevel.LOCAL);

    /**
     * Return the node's most recent copy of the data confirmed as having been written to a majority of the nodes.
     */
    public static final ReadConcern MAJORITY = new ReadConcern(ReadConcernLevel.MAJORITY);


    /**
     * @return true if this is the server default read concern
     */
    public boolean isServerDefault() {
        return readConcernLevel.equals(ReadConcernLevel.DEFAULT);
    }

    /**
     * Gets this read concern as a document.
     *
     * @return The read concern as a BsonDocument
     */
    public BsonDocument asDocument() {
        BsonDocument readConcern = new BsonDocument();
        if (!isServerDefault()){
            readConcern.put("level", new BsonString(readConcernLevel.getValue()));
        }
        return new BsonDocument("readConcern", readConcern);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        ReadConcern that = (ReadConcern) o;
        if (readConcernLevel != that.readConcernLevel) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return readConcernLevel != null ? readConcernLevel.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "ReadConcern{"
                + "readConcernLevel=" + readConcernLevel
                + "}";
    }
}
