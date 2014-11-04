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

package com.mongodb.async.rx.client;

import com.mongodb.annotations.Immutable;
import com.mongodb.async.client.MongoCollectionOptions;
import com.mongodb.async.client.MongoDatabaseOptions;
import org.bson.Document;
import org.bson.codecs.Codec;
import rx.Observable;

/**
 * A representation of a logical MongoDB database, which contains zero or more collections.  Instances of this class serve as factories
 * for representations of MongoDB collections contained in this database.
 * <p>
 * All methods on this class either complete immediately (without any I/O) or else execute asynchronously. The asynchronous operations
 * are adapted to work <a href="https://github.com/Netflix/RxJava">RxJava</a> and return {@code rx.Observable<T>}.
 * </p>
 *
 * @since 3.0
 */
@Immutable
public interface MongoDatabase {
    /**
     * Gets the name of the database.
     *
     * @return the name
     */
    String getName();

    /**
     * Gets a collection with the given name
     *
     * @param name the collection name
     * @return the collection
     */
    MongoCollection<Document> getCollection(String name);

    /**
     * Gets a collection with the given name and options
     *
     * @param name                   the collection name
     * @param mongoCollectionOptions the options to apply
     * @return the collection
     */
    MongoCollection<Document> getCollection(String name, MongoCollectionOptions mongoCollectionOptions);

    /**
     * Gets a collection with the given name, codec, and options.
     *
     * @param name the collection name
     * @param codec the codec to use to encode and decode documents in the collection
     * @param options the options to apply
     * @param <T> the document type
     * @return the collection
     */
    <T> MongoCollection<T> getCollection(String name, Codec<T> codec, MongoCollectionOptions options);

    /**
     * Asynchronously execute the command described by the given document.
     *
     * @param commandDocument the document describing the command to execute.
     * @return an Observable representing the completion of the command. It will report exactly one event when the command completes
     * successfully.
     */
    Observable<Document> executeCommand(Document commandDocument);

    /**
     * Gets the options that are used with the database.
     *
     * <p>Note: {@link com.mongodb.async.client.MongoDatabaseOptions} is immutable.</p>
     *
     * @return the options
     */
    MongoDatabaseOptions getOptions();

    /**
     * @return the DatabaseAdministration that provides admin methods that can be performed
     */
    DatabaseAdministration tools();
}
