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

package com.mongodb;

import com.mongodb.client.AggregateFluent;
import com.mongodb.client.MongoCollectionOptions;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.AggregateOptions;
import com.mongodb.client.model.FindOptions;
import com.mongodb.operation.AggregateOperation;
import com.mongodb.operation.AggregateToCollectionOperation;
import com.mongodb.operation.OperationExecutor;
import com.mongodb.operation.ReadOperation;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecConfigurationException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.bson.assertions.Assertions.isTrue;
import static org.bson.assertions.Assertions.notNull;

class AggregateFluentImpl<T> implements AggregateFluent<T> {
    private final MongoNamespace namespace;
    private final MongoCollectionOptions options;
    private final OperationExecutor executor;
    private final List<BsonDocument> pipeline;
    private final AggregateOptions aggregateOptions;
    private final Class<T> clazz;

    AggregateFluentImpl(final MongoNamespace namespace, final MongoCollectionOptions options, final OperationExecutor executor,
                        final List<?> pipeline, final AggregateOptions aggregateOptions, final Class<T> clazz) {
        this.namespace = notNull("namespace", namespace);
        this.options = notNull("options", options);
        this.executor = notNull("executor", executor);
        this.pipeline = notNull("pipeline", createBsonDocumentList(pipeline));
        this.aggregateOptions = notNull("aggregateOptions", aggregateOptions);
        this.clazz = notNull("clazz", clazz);
    }

    @Override
    public AggregateFluent<T> geoNear(final Object geoNear) {
        notNull("geoNear", geoNear);
        pipeline.add(new BsonDocument("$geoNear", asBson(geoNear)));
        return this;
    }

    @Override
    public AggregateFluent<T> group(final Object group) {
        notNull("group", group);
        pipeline.add(new BsonDocument("$group", asBson(group)));
        return this;
    }

    @Override
    public AggregateFluent<T> limit(final int limit) {
        isTrue("limit must be a positive integer", limit > 0);
        pipeline.add(new BsonDocument("$limit", new BsonInt32(limit)));
        return this;
    }

    @Override
    public AggregateFluent<T> match(final Object match) {
        notNull("match", match);
        pipeline.add(new BsonDocument("$match", asBson(match)));
        return this;
    }

    @Override
    public AggregateFluent<T> out(final String collectionName) {
        notNull("collectionName", collectionName);
        pipeline.add(new BsonDocument("$out", new BsonString(collectionName)));
        return this;
    }

    @Override
    public AggregateFluent<T> project(final Object project) {
        notNull("project", project);
        pipeline.add(new BsonDocument("$project", asBson(project)));
        return this;
    }

    @Override
    public AggregateFluent<T> redact(final Object redact) {
        notNull("redact", redact);
        pipeline.add(new BsonDocument("$redact", asBson(redact)));
        return this;
    }

    @Override
    public AggregateFluent<T> skip(final int skip) {
        isTrue("skip must be a positive integer", skip > 0);
        pipeline.add(new BsonDocument("$skip", new BsonInt32(skip)));
        return this;
    }

    @Override
    public AggregateFluent<T> sort(final Object sort) {
        notNull("sort", sort);
        pipeline.add(new BsonDocument("$sort", asBson(sort)));
        return this;
    }

    @Override
    public AggregateFluent<T> unwind(final String fieldPath) {
        notNull("fieldPath", fieldPath);
        pipeline.add(new BsonDocument("$unwind", new BsonString(fieldPath)));
        return this;
    }

    @Override
    public MongoCursor<T> iterator() {
        return execute().iterator();
    }

    @Override
    public T first() {
        return execute().first();
    }

    @Override
    public <U> MongoIterable<U> map(final Function<T, U> mapper) {
        return execute().map(mapper);
    }

    @Override
    public void forEach(final Block<? super T> block) {
        execute().forEach(block);
    }

    @Override
    public <A extends Collection<? super T>> A into(final A target) {
        return execute().into(target);
    }

    private <C> Codec<C> getCodec(final Class<C> clazz) {
        return options.getCodecRegistry().get(clazz);
    }

    private List<BsonDocument> createBsonDocumentList(final List<?> pipeline) {
        List<BsonDocument> aggregateList = new ArrayList<BsonDocument>(pipeline.size());
        for (Object obj : pipeline) {
            aggregateList.add(asBson(obj));
        }
        return aggregateList;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private BsonDocument asBson(final Object document) {
        if (document == null) {
            return null;
        }
        if (document instanceof BsonDocument) {
            return (BsonDocument) document;
        } else {
            try {
                Codec<? extends Object> codec = getCodec(document.getClass());
                return new BsonDocumentWrapper(document, codec);
            } catch (CodecConfigurationException e) {
                throw new CodecConfigurationException(format("%s %s", e.getMessage(), document));
            }
        }
    }

    private MongoIterable<T> execute() {
        BsonValue outCollection = pipeline.size() == 0 ? null : pipeline.get(pipeline.size() - 1).get("$out");

        if (outCollection != null) {
            AggregateToCollectionOperation operation = new AggregateToCollectionOperation(namespace, pipeline)
                                                           .maxTime(aggregateOptions.getMaxTime(MILLISECONDS), MILLISECONDS)
                                                           .allowDiskUse(aggregateOptions.getAllowDiskUse());
            executor.execute(operation);
            return new FindFluentImpl<T>(new MongoNamespace(namespace.getDatabaseName(), outCollection.asString().getValue()),
                                         this.options, executor, new BsonDocument(),
                                         new FindOptions().batchSize(aggregateOptions.getBatchSize())
                                                          .maxTime(aggregateOptions.getMaxTime(MILLISECONDS), MILLISECONDS), clazz);
        } else {
            return new OperationIterable<T>(new AggregateOperation<T>(namespace, pipeline, getCodec(clazz))
                                                .maxTime(aggregateOptions.getMaxTime(MILLISECONDS), MILLISECONDS)
                                                .allowDiskUse(aggregateOptions.getAllowDiskUse())
                                                .batchSize(aggregateOptions.getBatchSize())
                                                .useCursor(aggregateOptions.getUseCursor()),
                                            options.getReadPreference());
        }
    }

    private final class OperationIterable<D> implements MongoIterable<D> {
        private final ReadOperation<? extends MongoCursor<D>> operation;
        private final ReadPreference readPreference;

        private OperationIterable(final ReadOperation<? extends MongoCursor<D>> operation, final ReadPreference readPreference) {
            this.operation = operation;
            this.readPreference = readPreference;
        }

        @Override
        public <U> MongoIterable<U> map(final Function<D, U> mapper) {
            return new MappingIterable<D, U>(this, mapper);
        }

        @Override
        public MongoCursor<D> iterator() {
            return executor.execute(operation, readPreference);
        }

        @Override
        public D first() {
            MongoCursor<D> iterator = iterator();
            if (!iterator.hasNext()) {
                return null;
            }
            return iterator.next();
        }

        @Override
        public void forEach(final Block<? super D> block) {
            MongoCursor<D> cursor = iterator();
            try {
                while (cursor.hasNext()) {
                    block.apply(cursor.next());
                }
            } finally {
                cursor.close();
            }
        }

        @Override
        public <A extends Collection<? super D>> A into(final A target) {
            forEach(new Block<D>() {
                @Override
                public void apply(final D document) {
                    target.add(document);
                }
            });
            return target;
        }

    }
}
