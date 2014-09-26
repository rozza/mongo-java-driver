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

package com.mongodb.client.test;

import com.mongodb.CommandFailureException;
import com.mongodb.MongoCursor;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.codecs.DocumentCodec;
import com.mongodb.operation.CountOperation;
import com.mongodb.operation.CreateCollectionOperation;
import com.mongodb.operation.CreateIndexOperation;
import com.mongodb.operation.DropCollectionOperation;
import com.mongodb.operation.DropDatabaseOperation;
import com.mongodb.operation.FindOperation;
import com.mongodb.operation.InsertOperation;
import com.mongodb.operation.InsertRequest;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.mongodb.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.ClusterFixture.getBinding;
import static java.util.Arrays.asList;

public final class CollectionHelper<T> {

    private Codec<T> codec;
    private MongoNamespace namespace;

    public CollectionHelper(final Codec<T> codec, final MongoNamespace namespace) {
        this.codec = codec;
        this.namespace = namespace;
    }

    public static void drop(final MongoNamespace namespace) {
        new DropCollectionOperation(namespace).execute(getBinding());
    }

    public static void dropDatabase(final String name) {
        if (name == null) {
            return;
        }
        try {
            new DropDatabaseOperation(name).execute(getBinding());
        } catch (CommandFailureException e) {
            if (!e.getErrorMessage().contains("ns not found")) {
                throw e;
            }
        }
    }

    public void create(final String collectionName, final CreateCollectionOptions options) {
        drop(namespace);
        new CreateCollectionOperation(namespace.getDatabaseName(), collectionName)
            .capped(options.isCapped())
            .sizeInBytes(options.getSizeInBytes())
            .autoIndex(options.isAutoIndex())
            .maxDocuments(options.getMaxDocuments())
            .usePowerOf2Sizes(options.isUsePowerOf2Sizes()).execute(getBinding());
    }

    @SuppressWarnings("unchecked")
    public void insertDocuments(final BsonDocument... documents) {
        for (BsonDocument document : documents) {
            new InsertOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                asList(new InsertRequest(document))).execute(getBinding());
        }
    }

    @SuppressWarnings("unchecked")
    public void insertDocuments(final List<BsonDocument> documents) {
        for (BsonDocument document : documents) {
            new InsertOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                asList(new InsertRequest(document))).execute(getBinding());
        }
    }

    @SuppressWarnings("unchecked")
    public <I> void insertDocuments(final Codec<I> iCodec, final I... documents) {
        for (I document : documents) {
            new InsertOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                asList(new InsertRequest(new BsonDocumentWrapper<I>(document, iCodec)))).execute(getBinding());
        }
    }

    public <I> void insertDocuments(final Codec<I> iCodec, final List<I> documents) {
        for (I document : documents) {
            new InsertOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                asList(new InsertRequest(new BsonDocumentWrapper<I>(document, iCodec)))).execute(getBinding());
        }
    }

    public List<T> find() {
        return find(codec);
    }

    public <D> List<D> find(final Codec<D> codec) {
        MongoCursor<D> cursor = new FindOperation<D>(namespace, codec).execute(getBinding());
        List<D> results = new ArrayList<D>();
        while (cursor.hasNext()) {
            results.add(cursor.next());
        }
        return results;
    }

    public List<T> find(final Document filter) {
        return find(new BsonDocumentWrapper<Document>(filter, new DocumentCodec()), codec);
    }

    public <D> List<D> find(final BsonDocument filter, final Decoder<D> decoder) {
        MongoCursor<D> cursor = new FindOperation<D>(namespace, decoder).criteria(filter).execute(getBinding());
        List<D> results = new ArrayList<D>();
        while (cursor.hasNext()) {
            results.add(cursor.next());
        }
        return results;
    }

    public long count() {
        return new CountOperation(namespace).execute(getBinding());
    }

    public long count(final Document criteria) {
        return new CountOperation(namespace).criteria(wrap(criteria)).execute(getBinding());
    }

    public BsonDocument wrap(final Document document) {
        return new BsonDocumentWrapper<Document>(document, new DocumentCodec());
    }

    public void createIndex(final BsonDocument key) {
        new CreateIndexOperation(namespace, key).execute(getBinding());
    }
}
