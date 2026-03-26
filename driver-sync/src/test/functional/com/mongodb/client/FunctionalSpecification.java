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

package com.mongodb.client;

import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class FunctionalSpecification {
    protected MongoDatabase database;
    protected MongoCollection<Document> collection;

    @BeforeEach
    public void setUp() {
        database = Fixture.getMongoClient().getDatabase(Fixture.getDefaultDatabaseName());
        collection = database.getCollection(getClass().getName());
        collection.drop();
    }

    @AfterEach
    public void tearDown() {
        if (collection != null) {
            collection.drop();
        }
    }

    protected String getDatabaseName() {
        return Fixture.getDefaultDatabaseName();
    }

    protected String getCollectionName() {
        return collection.getNamespace().getCollectionName();
    }
}
