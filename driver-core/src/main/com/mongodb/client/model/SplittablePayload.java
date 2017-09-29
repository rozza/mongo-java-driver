/*
 * Copyright 2017 MongoDB, Inc.
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

package com.mongodb.client.model;

import org.bson.BsonDocument;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * A Splittable payload for write commands.
 *
 * <p>The command will consume as much of the payload as possible. The {@link #hasAnotherSplit()} method will return true if there is
 * another split to consume, {@link #getNextSplit} method will return the next SplittablePayload.</p>
 *
 * @see com.mongodb.connection.Connection#command(String, BsonDocument, SplittablePayload, org.bson.FieldNameValidator,
 * org.bson.codecs.Decoder, com.mongodb.connection.SessionContext)
 * @see com.mongodb.connection.AsyncConnection#commandAsync(String, BsonDocument, SplittablePayload, org.bson.FieldNameValidator,
 * org.bson.codecs.Decoder, com.mongodb.connection.SessionContext, com.mongodb.async.SingleResultCallback)
 * @since 3.6
 */
public final class SplittablePayload {
    private final String payloadName;
    private final List<BsonDocument> payload;
    private int position = 0;

    /**
     * Create a new instance
     *
     * @param payloadName the payload name eg: (documents, updates, deletes)
     * @param payload the payload
     */
    public SplittablePayload(final String payloadName, final List<BsonDocument> payload) {
        this.payloadName = notNull("payloadName", payloadName);
        this.payload = notNull("payload", payload);
    }

    /**
     * @return the payload name
     */
    public String getPayloadName() {
        return payloadName;
    }

    /**
     * @return the payload
     */
    public List<BsonDocument> getPayload() {
        return payload;
    }

    /**
     * @return the current position in the payload
     */
    public int getPosition() {
        return position;
    }

    /**
     * Sets the current position in the payload
     * @param position the position
     */
    public void setPosition(final int position) {
        this.position = position;
    }

    /**
     * @return true if there are more values after the current position
     */
    public boolean hasAnotherSplit() {
        return payload.size() > position;
    }

    /**
     * @return a new SplittablePayload containing only the values after the current position.
     */
    public SplittablePayload getNextSplit() {
        isTrue("hasAnotherSplit", hasAnotherSplit());
        List<BsonDocument> nextPayLoad = payload.subList(position, payload.size());
        return new SplittablePayload(payloadName, nextPayLoad);
    }

    /**
     * @return true if the payload is empty
     */
    public boolean isEmpty() {
        return payload.isEmpty();
    }
}
