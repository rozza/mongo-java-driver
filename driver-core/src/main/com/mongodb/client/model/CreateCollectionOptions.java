/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.client.model;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Options for creating a collection
 *
 * @mongodb.driver.manual reference/method/db.createCollection/ Create Collection
 * @since 3.0
 */
public class CreateCollectionOptions {
    private boolean autoIndex = true;
    private long maxDocuments;
    private boolean capped;
    private long sizeInBytes;
    private Boolean usePowerOf2Sizes;

    /**
     * Construct a new instance.
     *
     * @param capped whether the collection is capped
     * @param sizeInBytes the maximum size of the collection in bytes.  Only applies to capped collections.
     * @param autoIndex whether the _id field of the collection is indexed.  Only applies to capped collections
     * @param maxDocuments the maximum number of documents in the collection.  Only applies to capped collections
     * @param usePowerOf2Sizes use the usePowerOf2Sizes allocation strategy for this collection.
     *
     * @mongodb.driver.manual manual/reference/command/collMod/#usePowerOf2Sizes usePowerOf2Sizes
     * @mongodb.server.release 2.6
     */
    public void CreateCollectionOptions1(final boolean capped, final long sizeInBytes, final boolean autoIndex,
                                   final long maxDocuments, final Boolean usePowerOf2Sizes) {
        this.capped = capped;
        this.sizeInBytes = sizeInBytes;
        this.autoIndex = autoIndex;
        this.maxDocuments = maxDocuments;
        this.usePowerOf2Sizes = usePowerOf2Sizes;
    }

    /**
     * Gets if auto-index is enabled
     *
     * @return true if auto-index is enabled
     */
    public boolean isAutoIndex() {
        return autoIndex;
    }

    /**
     * Gets if auto-index is to be enabled on the collection
     *
     * @param autoIndex true if auto-index is enabled
     * @return this
     */
    public CreateCollectionOptions autoIndex(final boolean autoIndex) {
        this.autoIndex = autoIndex;
        return this;
    }

    /**
     * Gets the maximum number of documents allowed in the collection.
     *
     * @return max number of documents in the collection
     */
    public long getMaxDocuments() {
        return maxDocuments;
    }

    /**
     * Sets the maximum number of documents allowed in the collection.
     *
     * @param maxDocuments the maximum number of documents allowed in the collection
     * @return this
     */
    public CreateCollectionOptions maxDocuments(final long maxDocuments) {
        this.maxDocuments = maxDocuments;
        return this;
    }

    /**
     * Gets whether the collection is capped.
     *
     * @return whether the collection is capped
     */
    public boolean isCapped() {
        return capped;
    }


    /**
     * sets whether the collection is capped.
     *
     * @param capped whether the collection is capped
     * @return this
     */
    public CreateCollectionOptions capped(final boolean capped) {
        this.capped = capped;
        return this;
    }

    /**
     * Gets the maximum size of the collection in bytes.
     *
     * @return the maximum size of the collection
     */
    public long getSizeInBytes() {
        return sizeInBytes;
    }

    /**
     * Gets the maximum size of the collection in bytes.
     *
     * @param sizeInBytes the maximum size of the collection
     * @return this
     */
    public CreateCollectionOptions sizeInBytes(final long sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
        return this;
    }

    /**
     * Gets whether the usePowerOf2Sizes allocation strategy is turned on for this collection.
     *
     * @return true if the usePowerOf2Sizes allocation strategy is turned on for this collection
     * @mongodb.driver.manual manual/reference/command/collMod/#usePowerOf2Sizes usePowerOf2Sizes
     * @mongodb.server.release 2.6
     */
    public Boolean isUsePowerOf2Sizes() {
        return usePowerOf2Sizes;
    }

    /**
     * Sets whether the usePowerOf2Sizes allocation strategy is turned on for this collection.
     *
     * @param usePowerOf2Sizes true if the usePowerOf2Sizes allocation strategy is turned on for this collection
     * @return this
     * @mongodb.driver.manual manual/reference/command/collMod/#usePowerOf2Sizes usePowerOf2Sizes
     * @mongodb.server.release 2.6
     */
    public CreateCollectionOptions usePowerOf2Sizes(final Boolean usePowerOf2Sizes) {
        this.usePowerOf2Sizes = usePowerOf2Sizes;
        return this;
    }
}
