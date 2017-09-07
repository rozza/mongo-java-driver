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

package com.mongodb.client.model.changestream;

import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.codecs.Decoder;

import java.io.Serializable;

import static org.bson.assertions.Assertions.notNull;

/**
 * The resume token for a {@code $changeStream} operation.
 *
 * @since 3.6
 */
public final class ResumeToken implements Serializable {
    private static final long serialVersionUID = 4672638002757950176L;
    private final RawBsonDocument resumeToken;

    /**
     * Parses a string in MongoDB Extended JSON format to a {@code RawBsonDocument}
     *
     * @param json the JSON string
     * @return a corresponding {@code RawBsonDocument} object
     * @see org.bson.json.JsonReader
     * @mongodb.driver.manual reference/mongodb-extended-json/ MongoDB Extended JSON
     */
    public static ResumeToken parse(final String json) {
        notNull("json", json);
        return new ResumeToken(RawBsonDocument.parse(json));
    }

    /**
     * Constructs a new instance with the given byte array.  Note that it does not make a copy of the array, so do not modify it after
     * passing it to this constructor.
     *
     * @param bytes the bytes representing a BSON document.  Note that the byte array is NOT copied, so care must be taken not to modify it
     *              after passing it to this construction, unless of course that is your intention.
     */
    public ResumeToken(final byte[] bytes) {
        this(new RawBsonDocument(bytes));
    }

    ResumeToken(final RawBsonDocument resumeToken) {
        this.resumeToken = resumeToken;
    }

    /**
     * Returns the resumeToken
     *
     * @return the resumeToken
     */
    public BsonDocument getResumeToken() {
        return resumeToken;
    }

    /**
     * Decode this into a document.
     *
     * @param decoder the decoder to facilitate the transformation
     * @param <T>   the BSON type that the codec encodes/decodes
     * @return the decoded document
     * @since 3.6
     */
    public <T> T decode(final Decoder<T> decoder) {
        return resumeToken.decode(decoder);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ResumeToken that = (ResumeToken) o;

        if (resumeToken != null ? !resumeToken.equals(that.resumeToken) : that.resumeToken != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return resumeToken != null ? resumeToken.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "ChangeStreamResumeToken{"
                + "resumeToken=" + resumeToken.toJson()
                + "}";
    }
}
