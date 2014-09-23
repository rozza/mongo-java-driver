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
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import org.mongodb.Document;
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
     * @param name the collection name
     * @param options the options to apply
     * @return the collection
     */
    MongoCollection<Document> getCollection(String name, MongoCollectionOptions options);

    /**
     * Gets a collection with the given name, codec, and options.
     *
     * @param name the collection name
     * @param clazz the class of the document type to use
     * @param options the options to apply
     * @param <T> the document type
     * @return the collection
     */
    <T> MongoCollection<T> getCollection(String name, Class<T> clazz, MongoCollectionOptions options);

    /**
     * Asynchronously execute the command described by the given document.
     *
     * @param commandDocument the document describing the command to execute.
     * @return an Observable representing the completion of the command. It will report exactly one event when the command completes
     * successfully.
     */
    Observable<Document> executeCommand(Document commandDocument);

    /**
     * Drops this database.
     *
     * @return an observable that indicates when operation is complete
     * @mongodb.driver.manual reference/commands/dropDatabase/#dbcmd.dropDatabase Drop database
     */
    Observable<Void> dropDatabase();

    /**
     * Gets the names of all the collections in this database.
     *
     * @return an observable list of the names of all the collections in this database
     */
    Observable<String> getCollectionNames();

    /**
     * Create a new collection with the given name.
     *
     * @param collectionName the name for the new collection to create
     * @return an observable that indicates when operation is complete
     * @mongodb.driver.manual reference/commands/create Create Command
     */
    Observable<Void> createCollection(String collectionName);

    /**
     * Create a new collection with the selected options
     *
     * @param collectionName          the name for the new collection to create
     * @param createCollectionOptions various options for creating the collection
     * @return an observable that indicates when operation is complete
     * @mongodb.driver.manual reference/commands/create Create Command
     */
    Observable<Void> createCollection(String collectionName, CreateCollectionOptions createCollectionOptions);

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param oldCollectionName the collection to rename
     * @param newCollectionName the name the collection will be renamed to
     * @return an observable that indicates when operation is complete
     * @throws com.mongodb.MongoServerException if you provide a newCollectionName that is the name of an existing collection, or if the
     *                                          oldCollectionName is the name of a collection that doesn't exist
     * @mongodb.driver.manual reference/commands/renameCollection Rename collection
     */
    Observable<Void> renameCollection(String oldCollectionName, String newCollectionName);

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param oldCollectionName       the collection to rename
     * @param newCollectionName       the name the collection will be renamed to
     * @param renameCollectionOptions the options for renaming a collection
     * @return an observable that indicates when operation is complete
     * @throws com.mongodb.MongoServerException if you provide a newCollectionName that is the name of an existing collection and dropTarget
     *                                          is false, or if the oldCollectionName is the name of a collection that doesn't exist
     * @mongodb.driver.manual reference/commands/renameCollection Rename collection
     */
    Observable<Void> renameCollection(String oldCollectionName, String newCollectionName, RenameCollectionOptions renameCollectionOptions);

}
