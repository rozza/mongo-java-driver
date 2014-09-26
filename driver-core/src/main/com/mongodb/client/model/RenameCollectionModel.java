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

import com.mongodb.MongoNamespace;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A model describing a collection rename operation.
 *
 * @mongodb.driver.manual reference/command/renameCollection renameCollection
 * @since 3.0
 */
public class RenameCollectionModel {
    private final MongoNamespace originalNamespace;
    private final MongoNamespace newNamespace;
    private final RenameCollectionOptions options;

    /**
     * Construct an instance.
     *
     * @param originalNamespace the namespace to rename
     * @param newNamespace      the desired new namespace
     */
    public RenameCollectionModel(final MongoNamespace originalNamespace, final MongoNamespace newNamespace) {
        this(originalNamespace, newNamespace, new RenameCollectionOptions());
    }

    /**
     * Construct an instance.
     *
     * @param originalNamespace the namespace to rename
     * @param newNamespace      the desired new namespace
     * @param renameCollectionOptions the options
     */
    public RenameCollectionModel(final MongoNamespace originalNamespace, final MongoNamespace newNamespace,
                                 final RenameCollectionOptions renameCollectionOptions) {
        this.originalNamespace = notNull("originalNamespace", originalNamespace);
        this.newNamespace = notNull("newNamespace", newNamespace);
        this.options = notNull("renameCollectionOptions", renameCollectionOptions);
    }

    /**
     * Gets the namespace to be renamed.
     *
     * @return the namespace to be renamed.
     */
    public MongoNamespace getOriginalNamespace() {
        return originalNamespace;
    }

    /**
     * Gets the new namespace.
     *
     * @return the new namespace.
     */
    public MongoNamespace getNewNamespace() {
        return newNamespace;
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
