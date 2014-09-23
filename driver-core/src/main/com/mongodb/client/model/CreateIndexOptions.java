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

/**
 * The options to apply to an operation that atomically finds a document and deletes it.
 *
 * @since 3.0
 * @mongodb.driver.manual reference/method/db.collection.ensureIndex/#options Index options
 */
public class CreateIndexOptions {
    private boolean background;
    private boolean unique;
    private String name;
    private boolean dropDups;
    private boolean sparse;
    private Integer expireAfterSeconds;
    private Object extra;

    /**
     * Create the index in the background
     *
     * @return true if should create the index in the background
     */
    public boolean isBackground() {
        return background;
    }

    /**
     * Sets if the index should be created in the background
     *
     * @param background true if should create the index in the background
     * @return this
     */
    public CreateIndexOptions background(final boolean background) {
        this.background = background;
        return this;
    }

    /**
     * Gets if the index should be unique.
     *
     * @return true if the index should be unique
     */
    public boolean isUnique() {
        return unique;
    }

    /**
     * Sets if the index should be unique.
     *
     * @param unique if the index should be unique
     * @return this
     */
    public CreateIndexOptions unique(final boolean unique) {
        this.unique = unique;
        return this;
    }

    /**
     * Gets the name of the index.
     *
     * @return the name of the index
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the index.
     *
     * @param name of the index
     * @return this
     */
    public CreateIndexOptions name(final String name) {
        this.name = name;
        return this;
    }

    /**
     * CreatE a unique index on a field that may have duplicates.
     *
     * @return true creating a unique index on a field that may have duplicates
     */
    public boolean isDropDups() {
        return dropDups;
    }

    /**
     * Create a unique index on a field that may have duplicates.
     *
     * <p>MongoDB will silently drop duplicates silently when creating and only the first will be kept</p>
     *
     *
     * @param dropDups create a unique index on a field that may have duplicates
     * @return this
     */
    public CreateIndexOptions dropDups(final boolean dropDups) {
        this.dropDups = dropDups;
        return this;
    }

    /**
     * If true, the index only references documents with the specified field
     *
     * @return if the index should only reference documents with the specified field
     */
    public boolean isSparse() {
        return sparse;
    }

    /**
     * Should the index only references documents with the specified field
     *
     * @param sparse if true, the index only references documents with the specified field
     * @return this
     */
    public CreateIndexOptions sparse(final boolean sparse) {
        this.sparse = sparse;
        return this;
    }

    /**
     * Gets the time to live for documents in the collection
     *
     * @return the time to live for documents in the collection
     * @mongodb.driver.manual manual/tutorial/expire-data TTL
     */
    public Integer getExpireAfterSeconds() {
        return expireAfterSeconds;
    }

    /**
     * Sets the time to live for documents in the collection
     *
     * @param expireAfterSeconds the time to live for documents in the collection
     * @return this
     * @mongodb.driver.manual manual/tutorial/expire-data TTL
     */
    public CreateIndexOptions expireAfterSeconds(final Integer expireAfterSeconds) {
        this.expireAfterSeconds = expireAfterSeconds;
        return this;
    }

    /**
     * Gets any set extra index options
     *
     * <p>This can be of any type for which a {@code Codec} is registered</p>
     *
     * @return any extra index options
     * @mongodb.driver.manual reference/method/db.collection.ensureIndex/#options Index options
     */
    public Object getExtraIndexOptions() {
        return extra;
    }

    /**
     * Sets any specific index options for the different index types.
     *
     * <p>This can be of any type for which a {@code Codec} is registered</p>
     *
     * @param extraIndexOptions any extra index creation options
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.ensureIndex/#options Index options
     */
    public CreateIndexOptions extraIndexOptions(final Object extraIndexOptions) {
        this.extra = extraIndexOptions;
        return this;
    }

}
