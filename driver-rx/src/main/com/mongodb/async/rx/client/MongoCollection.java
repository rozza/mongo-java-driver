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

import com.mongodb.MongoNamespace;
import com.mongodb.async.client.MongoCollectionOptions;
import com.mongodb.client.model.CreateIndexModel;
import com.mongodb.client.model.CreateIndexOptions;
import org.mongodb.Document;
import org.mongodb.WriteResult;
import rx.Observable;

import java.util.List;

public interface MongoCollection<T> {
    /**
     * Gets the name of this collection.  This is the simple name of the collection and is not prefixed with the database name.
     *
     * @return the collection name
     */
    String getName();

    /**
     * Gets the namespace of this collection.
     *
     * @return the namespace
     */
    MongoNamespace getNamespace();

    /**
     * Gets the options applied to operations on this collection.
     *
     * @return the options
     */
    MongoCollectionOptions getOptions();

    /**
     * Create a view on the collection with the given filter. This method does not do any I/O.
     *
     * @param filter the filter
     * @return a view on this collection with the given filter
     */
    MongoView<T> find(Document filter);

    /**
     * Insert a document into the collection.
     *
     * @param document the document to insert
     * @return an Observable representing the completion of the insert. It will report exactly one event when the command completes
     * successfully.
     */
    Observable<WriteResult> insert(T document);

    /**
     * Insert the documents into the collection.
     *
     * @param documents the documents to insert
     * @return an Observable representing the completion of the insert. It will report exactly one event when the command completes
     * successfully.
     */
    Observable<WriteResult> insert(List<T> documents);

    /**
     * Drops this collection from the Database.
     *
     * @return a future that indicates when operation is complete
     * @mongodb.driver.manual reference/command/drop/ Drop Collection
     */
    Observable<Void> dropCollection();

    /**
     * @param key an object describing the index key(s), which may not be null. This can be of any type for which a {@code Codec} is
     *            registered
     * @return a future that indicates when operation is complete
     * @mongodb.driver.manual reference/method/db.collection.ensureIndex Ensure Index
     */
    Observable<Void> createIndex(Object key);

    /**
     * @param key an object describing the index key(s), which may not be null. This can be of any type for which a {@code Codec} is
     *            registered
     * @param createIndexOptions the options for the index
     * @return a future that indicates when operation is complete
     * @mongodb.driver.manual reference/method/db.collection.ensureIndex Ensure Index
     */
    Observable<Void> createIndex(Object key, CreateIndexOptions createIndexOptions);

    /**
     * Builds one or more indexes on a collection.
     *
     * @param indexModels a list of models representing indexes
     * @return a future that indicates when operation is complete
     * @mongodb.driver.manual reference/command/createIndexes createIndexes
     */
    Observable<Void> createIndexes(List<CreateIndexModel> indexModels);

    /**
     * @return all the indexes on this collection
     * @mongodb.driver.manual reference/method/db.collection.getIndexes/ getIndexes
     */
    Observable<Document> getIndexes();

    /**
     * @param clazz the class to decode each document into
     * @return all the indexes on this collection
     * @mongodb.driver.manual reference/method/db.collection.getIndexes/ getIndexes
     */
    <C> Observable<C> getIndexes(Class<C> clazz);

    /**
     * Drops the given index.
     *
     * @param indexName the name of the index to remove
     * @return a future that indicates when operation is complete
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop Indexes
     */
    Observable<Void> dropIndex(String indexName);

    /**
     * Drop all the indexes on this collection, except for the default on _id.
     *
     * @return a future that indicates when operation is complete
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop Indexes
     */
    Observable<Void> dropIndexes();
}
