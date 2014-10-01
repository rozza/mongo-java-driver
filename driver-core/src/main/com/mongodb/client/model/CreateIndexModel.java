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
 * A model describing a distinct operation.
 *
 * @since 3.0
 * @mongodb.driver.manual reference/method/db.collection.ensureIndex
 */
public class CreateIndexModel {
    private final Object keys;
    private final CreateIndexOptions options;

    /**
     * Construct a new instance.
     *
     * @param keys An object that contains the field and value pairs where the field is the index key and the value describes the type of
     *             index for the field(s).
     */
    public CreateIndexModel(final Object keys) {
        this(keys, new CreateIndexOptions());
    }

    /**
     * Construct a new instance.
     *
     * @param keys An object that contains the field and value pairs where the field is the index key and the value describes the type of
     *             index for the field(s).
     * @param options the optional index creation options
     */
    public CreateIndexModel(final Object keys, final CreateIndexOptions options) {
        this.keys = notNull("keys", keys);
        this.options = notNull("options", options);
    }

    /**
     * The keys for the index.
     *
     * <p>An object that contains the field and value pairs where the field is the index key and the value describes the type of index for
     * the field(s).</p>
     *
     * @return the index keys
     */
    public Object getKeys() {
        return keys;
    }

    /**
     * Gets the optional index creation options.
     *
     * @return the optional index creation options
     */
    public CreateIndexOptions getOptions() {
        return options;
    }
}