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

import com.mongodb.ClusterFixture;
import com.mongodb.MongoException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.naming.Reference;
import javax.naming.StringRefAddr;
import java.util.Hashtable;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MongoClientFactoryTest extends FunctionalSpecification {
    private final MongoClientFactory mongoClientFactory = new MongoClientFactory();

    @Test
    @DisplayName("should create MongoClient from environment")
    void shouldCreateMongoClientFromEnvironment() throws Exception {
        Hashtable<String, String> environment = new Hashtable<>();
        environment.put("connectionString", ClusterFixture.getConnectionString().getConnectionString());

        MongoClient client = null;
        try {
            client = (MongoClient) mongoClientFactory.getObjectInstance(null, null, null, environment);
            assertNotNull(client);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    @Test
    @DisplayName("should create MongoClient from obj that is of type Reference")
    void shouldCreateMongoClientFromReference() throws Exception {
        Hashtable<String, String> environment = new Hashtable<>();
        Reference reference = new Reference(null, new StringRefAddr("connectionString",
                ClusterFixture.getConnectionString().getConnectionString()));

        MongoClient client = null;
        try {
            client = (MongoClient) mongoClientFactory.getObjectInstance(reference, null, null, environment);
            assertNotNull(client);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    @Test
    @DisplayName("should throw if no connection string is provided")
    void shouldThrowIfNoConnectionStringProvided() {
        Hashtable<String, String> environment = new Hashtable<>();
        assertThrows(MongoException.class, () ->
                mongoClientFactory.getObjectInstance(null, null, null, environment));
    }
}
