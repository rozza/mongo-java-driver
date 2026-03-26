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

package com.mongodb.reactivestreams.client;

import com.mongodb.MongoNamespace;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.reactivestreams.client.Fixture.drop;
import static com.mongodb.reactivestreams.client.Fixture.dropDatabase;
import static com.mongodb.reactivestreams.client.Fixture.getDefaultDatabase;
import static com.mongodb.reactivestreams.client.Fixture.initializeCollection;
import static com.mongodb.reactivestreams.client.Fixture.waitForLastServerSessionPoolRelease;

public class FunctionalSpecification {
    //For ease of use and readability, in this specific case we'll allow protected variables
    //CHECKSTYLE:OFF
    protected MongoDatabase database;
    protected MongoCollection<Document> collection;
    //CHECKSTYLE:ON

    @BeforeAll
    public static void setupSpec() {
        dropDatabase(getDefaultDatabaseName());
    }

    @AfterAll
    public static void cleanupSpec() {
        dropDatabase(getDefaultDatabaseName());
    }

    @BeforeEach
    public void setUp() {
        database = getDefaultDatabase();
        collection = initializeCollection(new MongoNamespace(database.getName(), getClass().getName()));
        drop(collection.getNamespace());
    }

    @AfterEach
    public void tearDown() {
        if (collection != null) {
            drop(collection.getNamespace());
        }
        waitForLastServerSessionPoolRelease();
    }

    protected String getDatabaseName() {
        return database.getName();
    }

    protected String getCollectionName() {
        return collection.getNamespace().getCollectionName();
    }

    protected MongoNamespace getNamespace() {
        return new MongoNamespace(getDatabaseName(), getCollectionName());
    }
}
