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

package com.mongodb.async.client;

import com.mongodb.Block;
import com.mongodb.Function;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.async.MongoAsyncCursor;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.client.model.CreateIndexModel;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.FindModel;
import com.mongodb.codecs.CollectibleCodec;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.CountOperation;
import com.mongodb.operation.CreateIndexesOperation;
import com.mongodb.operation.DeleteOperation;
import com.mongodb.operation.DeleteRequest;
import com.mongodb.operation.DropCollectionOperation;
import com.mongodb.operation.DropIndexOperation;
import com.mongodb.operation.FindOperation;
import com.mongodb.operation.GetIndexesOperation;
import com.mongodb.operation.InsertOperation;
import com.mongodb.operation.InsertRequest;
import com.mongodb.operation.UpdateOperation;
import com.mongodb.operation.UpdateRequest;
import com.mongodb.operation.WriteRequest;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.codecs.Codec;
import org.mongodb.ConvertibleToDocument;
import org.mongodb.Document;
import org.mongodb.WriteResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class MongoCollectionImpl<T> implements MongoCollection<T> {
    private final MongoNamespace namespace;
    private final Class<T> clazz;
    private final MongoCollectionOptions options;
    private final AsyncOperationExecutor executor;

    public MongoCollectionImpl(final MongoNamespace namespace, final Class<T> clazz, final MongoCollectionOptions options,
                               final AsyncOperationExecutor executor) {
        this.namespace = namespace;
        this.clazz = clazz;
        this.options = options;
        this.executor = executor;
    }

    @Override
    public String getName() {
        return namespace.getCollectionName();
    }

    @Override
    public MongoNamespace getNamespace() {
        return namespace;
    }

    @Override
    public MongoCollectionOptions getOptions() {
        return options;
    }


    private Codec<T> getCodec() {
        return getCodec(clazz);
    }

    private <C> Codec<C> getCodec(final Class<C> clazz) {
        return options.getCodecRegistry().get(clazz);
    }

    @Override
    public MongoView<T> find(final Document filter) {
        return new MongoCollectionView().find(filter);
    }

    @Override
    @SuppressWarnings("unchecked")
    public MongoFuture<WriteResult> insert(final T document) {
        notNull("document", document);
        return insert(asList(document));
    }

    @Override
    public MongoFuture<WriteResult> insert(final List<T> documents) {
        notNull("documents", documents);
        List<InsertRequest> insertRequests = new ArrayList<InsertRequest>();
        for (T document : documents) {
            if (getCodec() instanceof CollectibleCodec) {
                ((CollectibleCodec<T>) getCodec()).generateIdIfAbsentFromDocument(document);
            }
            insertRequests.add(new InsertRequest(new BsonDocumentWrapper<T>(document, getCodec())));
        }
        return executor.execute(new InsertOperation(getNamespace(), true, options.getWriteConcern(), insertRequests));
    }

    @Override
    public MongoFuture<Void> dropCollection() {
        return executor.execute(new DropCollectionOperation(namespace));
    }

    @Override
    public MongoFuture<Void> createIndex(final Object key) {
        return createIndexes(asList(new CreateIndexModel(key, new CreateIndexOptions())));
    }

    @Override
    public MongoFuture<Void> createIndex(final Object key, final CreateIndexOptions createIndexOptions) {
        return createIndexes(asList(new CreateIndexModel(key, createIndexOptions)));
    }

    @Override
    public MongoFuture<Void> createIndexes(final List<CreateIndexModel> indexModels) {
        List<BsonDocument> indexes = new ArrayList<BsonDocument>();
        for (CreateIndexModel index : indexModels) {
            indexes.add(createIndexModelToBsonDocument(index));
        }
        return executor.execute(new CreateIndexesOperation(namespace, indexes));
    }

    private BsonDocument createIndexModelToBsonDocument(final CreateIndexModel index) {
        CreateIndexOptions options = index.getOptions();
        BsonDocument indexDetails = new BsonDocument();
        if (options.getName() != null) {
            indexDetails.append("name", new BsonString(options.getName()));
        }
        indexDetails.append("key", asBson(index.getKeys()));
        if (options.isUnique()) {
            indexDetails.append("unique", BsonBoolean.TRUE);
        }
        if (options.isSparse()) {
            indexDetails.append("sparse", BsonBoolean.TRUE);
        }
        if (options.isDropDups()) {
            indexDetails.append("dropDups", BsonBoolean.TRUE);
        }
        if (options.isBackground()) {
            indexDetails.append("background", BsonBoolean.TRUE);
        }
        if (options.getExpireAfterSeconds() != null) {
            indexDetails.append("expireAfterSeconds", new BsonInt32(options.getExpireAfterSeconds()));
        }
        Object extraIndexOptions = options.getExtraIndexOptions();
        if (extraIndexOptions != null) {
            indexDetails.putAll(asBson(extraIndexOptions));
        }
        indexDetails.put("ns", new BsonString(namespace.getFullName()));
        return indexDetails;
    }

    @Override
    public MongoFuture<List<Document>> getIndexes() {
        return executor.execute(new GetIndexesOperation<Document>(namespace, getCodec(Document.class)), options.getReadPreference());
    }

    @Override
    public MongoFuture<Void> dropIndex(final String indexName) {
        return executor.execute(new DropIndexOperation(namespace, indexName));
    }

    @Override
    public MongoFuture<Void> dropIndexes() {
        return dropIndex("*");
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private BsonDocument asBson(final Object document) {
        if (document == null) {
            return null;
        }
        if (document instanceof BsonDocument) {
            return (BsonDocument) document;
        } else if (document instanceof Document) {
            return new BsonDocumentWrapper(document, getCodec());
        } else {
            throw new IllegalArgumentException("No encoder for class " + document.getClass());
        }

    }

    private class MongoCollectionView implements MongoView<T> {
        private final FindModel findModel = new FindModel();
        private final ReadPreference readPreference = options.getReadPreference();
        private boolean upsert;

        @Override
        public MongoFuture<T> one() {
            FindOperation<T> findOperation = createQueryOperation().batchSize(-1);

            final SingleResultFuture<T> retVal = new SingleResultFuture<T>();
            executor.execute(findOperation, readPreference)
                    .register(new
                                  SingleResultCallback<MongoAsyncCursor<T>>() {
                                      @Override
                                      public void onResult(final MongoAsyncCursor<T> cursor, final MongoException e) {
                                          if (e != null) {
                                              retVal.init(null, e);
                                          } else {
                                              cursor.forEach(new Block<T>() {
                                                  @Override
                                                  public void apply(final T t) {
                                                      retVal.init(t, null);
                                                  }
                                              }).register(new SingleResultCallback<Void>() {
                                                  @Override
                                                  public void onResult(final Void result, final MongoException e) {
                                                      if (!retVal.isDone()) {
                                                          retVal.init(null, e);
                                                      }
                                                  }
                                              });
                                          }
                                      }
                                  });
            return retVal;
        }

        @Override
        public MongoFuture<Long> count() {
            CountOperation operation = new CountOperation(getNamespace())
                                           .criteria((BsonDocument) findModel.getOptions().getCriteria())
                                           .skip(findModel.getOptions().getSkip())
                                           .limit(findModel.getOptions().getLimit())
                                           .maxTime(findModel.getOptions().getMaxTime(MILLISECONDS), MILLISECONDS);
            return executor.execute(operation, readPreference);
        }

        @Override
        public MongoView<T> find(final Document filter) {
            findModel.getOptions().criteria(new BsonDocumentWrapper<Document>(filter, getCodec(Document.class)));
            return this;
        }

        @Override
        public MongoView<T> find(final ConvertibleToDocument filter) {
            return find(filter.toDocument());
        }

        MongoView<T> find(final BsonDocument filter) {
            findModel.getOptions().criteria(filter);
            return this;
        }

        @Override
        public MongoView<T> sort(final Document sortCriteria) {
            findModel.getOptions().sort(new BsonDocumentWrapper<Document>(sortCriteria, getCodec(Document.class)));
            return this;
        }

        @Override
        public MongoView<T> sort(final ConvertibleToDocument sortCriteria) {
            return sort(sortCriteria.toDocument());
        }

        @Override
        public MongoView<T> skip(final int skip) {
            findModel.getOptions().skip(skip);
            return this;
        }

        @Override
        public MongoView<T> limit(final int limit) {
            findModel.getOptions().limit(limit);
            return this;
        }

        @Override
        public MongoView<T> fields(final Document selector) {
            findModel.getOptions().projection(new BsonDocumentWrapper<Document>(selector, getCodec(Document.class)));
            return this;
        }

        @Override
        public MongoView<T> fields(final ConvertibleToDocument selector) {
            return fields(selector.toDocument());
        }

        @Override
        public MongoView<T> upsert() {
            upsert = true;
            return this;
        }

        @Override
        public MongoFuture<Void> forEach(final Block<? super T> block) {
            final SingleResultFuture<Void> retVal = new SingleResultFuture<Void>();
            executor.execute(createQueryOperation(), readPreference)
                    .register(new
                                  SingleResultCallback<MongoAsyncCursor<T>>() {
                                      @Override
                                      public void onResult(final MongoAsyncCursor<T> cursor, final MongoException e) {
                                          if (e != null) {
                                              retVal.init(null, e);
                                          } else {
                                              cursor.forEach(new Block<T>() {
                                                  @Override
                                                  public void apply(final T t) {
                                                      block.apply(t);
                                                  }
                                              }).register(new SingleResultCallback<Void>() {
                                                  @Override
                                                  public void onResult(final Void result, final MongoException e) {
                                                      if (e != null) {
                                                          retVal.init(null, e);
                                                      } else {
                                                          retVal.init(null, null);
                                                      }
                                                  }
                                              });
                                          }
                                      }
                                  });
            return retVal;
        }

        @Override
        public <A extends Collection<? super T>> MongoFuture<A> into(final A target) {
            final SingleResultFuture<A> future = new SingleResultFuture<A>();
            forEach(new Block<T>() {
                @Override
                public void apply(final T t) {
                    target.add(t);
                }
            }).register(new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final MongoException e) {
                    if (e != null) {
                        future.init(null, e);
                    } else {
                        future.init(target, null);
                    }
                }
            });
            return future;
        }

        @Override
        public <U> MongoIterable<U> map(final Function<T, U> mapper) {
            return new MappingIterable<T, U>(this, mapper);
        }

        @Override
        @SuppressWarnings("unchecked")
        public MongoFuture<WriteResult> replace(final T replacement) {
            notNull("replacement", replacement);
            return executor.execute(new UpdateOperation(getNamespace(), true, options.getWriteConcern(),
                                                        asList(new UpdateRequest((BsonDocument) findModel.getOptions().getCriteria(),
                                                                                 asBson(replacement), WriteRequest.Type.REPLACE)
                                                                   .upsert(upsert))
            ));
        }

        @Override
        public MongoFuture<WriteResult> update(final Document updateOperations) {
            notNull("updateOperations", updateOperations);
            return executor.execute(new UpdateOperation(getNamespace(), true, options.getWriteConcern(),
                                                        asList(new UpdateRequest((BsonDocument) findModel.getOptions().getCriteria(),
                                                                                 new BsonDocumentWrapper<Document>(updateOperations,
                                                                                                                   getCodec(Document
                                                                                                                                .class)),
                                                                                 WriteRequest.Type.UPDATE)
                                                                   .upsert(upsert).multi(true))
            ));
        }

        @Override
        public MongoFuture<WriteResult> updateOne(final Document updateOperations) {
            notNull("updateOperations", updateOperations);
            return executor.execute(new UpdateOperation(getNamespace(), true, options.getWriteConcern(),
                                                        asList(new UpdateRequest((BsonDocument) findModel.getOptions().getCriteria(),
                                                                                 new BsonDocumentWrapper<Document>(updateOperations,
                                                                                                                   getCodec(Document
                                                                                                                                .class)),
                                                                                 WriteRequest.Type.UPDATE)
                                                                   .upsert(upsert).multi(false))
            ));
        }

        @Override
        public MongoFuture<WriteResult> remove() {
            return executor.execute(new DeleteOperation(getNamespace(), true, options.getWriteConcern(),
                                                        asList(new DeleteRequest((BsonDocument) findModel.getOptions().getCriteria())
                                                                   .multi(true))));
        }

        @Override
        public MongoFuture<WriteResult> removeOne() {
            return executor.execute(new DeleteOperation(getNamespace(), true, options.getWriteConcern(),
                                                        asList(new DeleteRequest((BsonDocument) findModel.getOptions().getCriteria())
                                                                   .multi(false))));
        }

        private FindOperation<T> createQueryOperation() {
            return new FindOperation<T>(getNamespace(), getCodec())
                       .criteria(asBson(findModel.getOptions().getCriteria()))
                       .batchSize(findModel.getOptions().getBatchSize())
                       .skip(findModel.getOptions().getSkip())
                       .limit(findModel.getOptions().getLimit())
                       .maxTime(findModel.getOptions().getMaxTime(MILLISECONDS), MILLISECONDS)
                       .modifiers(asBson(findModel.getOptions().getModifiers()))
                       .projection(asBson(findModel.getOptions().getProjection()))
                       .sort(asBson(findModel.getOptions().getSort()));
        }
    }
}
