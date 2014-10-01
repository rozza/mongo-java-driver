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
 * A model describing a collection rename operation.
 *
 * @mongodb.driver.manual reference/command/renameCollection renameCollection
 * @since 3.0
 */
public class RenameCollectionModel {
    private final String originalCollectionName;
    private final String newCollectionName;
    private final RenameCollectionOptions options;

    /**
     * Construct an instance.
     *
     * @param originalCollectionName the name of the collection to rename
     * @param newCollectionName      the desired new name for the collection
     */
    public RenameCollectionModel(final String originalCollectionName, final String newCollectionName) {
        this(originalCollectionName, newCollectionName, new RenameCollectionOptions());
    }

    /**
     * Construct an instance.
     *
     * @param originalCollectionName the name of the collection to rename
     * @param newCollectionName      the desired new name for the collection
     * @param renameCollectionOptions the options
     */
    public RenameCollectionModel(final String originalCollectionName, final String newCollectionName,
                                 final RenameCollectionOptions renameCollectionOptions) {
        this.originalCollectionName = notNull("originalCollectionName", originalCollectionName);
        this.newCollectionName = notNull("newCollectionName", newCollectionName);
        this.options = notNull("renameCollectionOptions", renameCollectionOptions);
    }

    /**
     * Gets the name of the collection to be renamed.
     *
     * @return the name of the collection to be renamed.
     */
    public String getOriginalCollectionName() {
        return originalCollectionName;
    }

    /**
     * Gets the new collection name.
     *
     * @return the new collection name.
     */
    public String getNewCollectionName() {
        return newCollectionName;
    }

    /**
     * The options to apply.
     *
     * @return the options
     */
    public RenameCollectionOptions getOptions() {
        return options;
    }
}
