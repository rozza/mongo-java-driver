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

package com.mongodb.internal.bulk;

import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A representation of a document to insert.
 *
 * @since 3.0
 */
public final class InsertRequest extends WriteRequest {
    private final BsonDocument document;
    private BsonValue id;

    /**
     * Construct an instance with the given document.
     *
     * @param document the document, which may not be null
     */
    public InsertRequest(final BsonDocument document) {
        this.document = notNull("document", document);
    }

    /**
     * Gets the document to insert.
     *
     * @return the document
     */
    public BsonDocument getDocument() {
        return document;
    }

    @Override
    public Type getType() {
        return Type.INSERT;
    }

    /**
     * Internal method
     * @return the _id of the insert
     */
    @Nullable
    public BsonValue getId() {
        return id;
    }

    /**
     * Internal method
     * @param id the _id of the inserted document
     */
    public void setId(@Nullable final BsonValue id) {
        this.id = id;
    }
}

