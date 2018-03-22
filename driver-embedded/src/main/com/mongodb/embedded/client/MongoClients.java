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

package com.mongodb.embedded.client;

import com.mongodb.MongoClientException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.connection.Cluster;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;

import static java.lang.String.format;

/**
 * A factory for {@link MongoClient} instances.
 *
 * @see MongoClient
 * @since 3.8
 */
public final class MongoClients {
    private static final String NATIVE_LIBRARY_NAME = "mongo_embedded_capi";
    private static MongoDBCAPI mongoDBCAPI;

    /**
     * Initializes the mongod library for use.
     *
     * <p>The library must be called at most once per process before calling {@link #create(MongoClientSettings)}.</p>
     * @param mongoEmbeddedSettings the settings for the embedded driver.
     */
    public static void init(final MongoEmbeddedSettings mongoEmbeddedSettings) {
        if (mongoDBCAPI != null) {
            throw new MongoClientException("MongoDBCAPI has been initialized but not closed");
        }

        if (mongoEmbeddedSettings.getLibraryPath() != null) {
            NativeLibrary.addSearchPath(NATIVE_LIBRARY_NAME, mongoEmbeddedSettings.getLibraryPath());
        }
        try {
            mongoDBCAPI = Native.loadLibrary(NATIVE_LIBRARY_NAME, MongoDBCAPI.class);
        } catch (UnsatisfiedLinkError e) {
            throw new MongoClientException(format("Failed to load the mongodb library: '%s'."
                    + "%n %s %n"
                    + "%n Please set the library location by either:"
                    + "%n - Adding it to the classpath."
                    + "%n - Setting 'jna.library.path' system property"
                    + "%n - Configuring it in the 'MongoClientSettings.builder().libraryPath' method."
                    + "%n", NATIVE_LIBRARY_NAME, e.getMessage()), e);
        }
        // TODO - support yaml
        mongoDBCAPI.libmongodbcapi_init("");
    }

    /**
     * Creates a new client.
     *
     * @param mongoClientSettings the mongoClientSettings
     * @return the client
     */
    public static MongoClient create(final MongoClientSettings mongoClientSettings) {
        if (mongoDBCAPI == null) {
            init(MongoEmbeddedSettings.builder().build());
        }
        Cluster cluster = new EmbeddedCluster(mongoDBCAPI, mongoClientSettings);
        return new MongoClientImpl(cluster, mongoClientSettings.getWrappedMongoClientSettings(), null);
    }

    /**
     * Closes down the mongod library
     */
    public static void close() {
        if (mongoDBCAPI != null) {
            mongoDBCAPI.libmongodbcapi_fini();
            mongoDBCAPI = null;
        }
    }

    private MongoClients() {
    }
}
