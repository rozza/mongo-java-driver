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
 * A model describing a create collection operation.
 *
 * @since 3.0
 * @mongodb.driver.manual reference/method/db.createCollection createCollection
 */
public class CreateCollectionModel {
    private final String collectionName;
    private final CreateCollectionOptions options;

    /**
     * Construct an instance.
     *
     * @param collectionName the collection name.
     */
    public CreateCollectionModel(final String collectionName) {
        this(collectionName, new CreateCollectionOptions());
    }

    /**
     * Construct an instance.
     *
     * @param collectionName the collection name.
     * @param createCollectionOptions the options
     */
    public CreateCollectionModel(final String collectionName, final CreateCollectionOptions createCollectionOptions) {
        this.collectionName = notNull("collectionName", collectionName);
        this.options = notNull("createCollectionOptions", createCollectionOptions);
    }

    /**
     * Gets the collection name to be created.
     *
     * @return the collection name to be created.
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * The options to apply.
     *
     * @return the options
     */
    public CreateCollectionOptions getOptions() {
        return options;
    }
}
