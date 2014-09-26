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

package com.mongodb.operation;

import com.mongodb.CommandFailureException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.connection.Connection;
import com.mongodb.protocol.InsertProtocol;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.mongodb.WriteResult;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnection;
import static com.mongodb.operation.OperationHelper.CallableWithConnection;
import static com.mongodb.operation.OperationHelper.DUPLICATE_KEY_ERROR_CODES;
import static com.mongodb.operation.OperationHelper.serverIsAtLeastVersionTwoDotSix;
import static com.mongodb.operation.OperationHelper.withConnection;
import static java.util.Arrays.asList;

/**
 * An operation that creates one or more indexes.
 *
 * @since 3.0
 */
public class CreateIndexesOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final MongoNamespace namespace;
    private final List<BsonDocument> indexes;
    private final MongoNamespace systemIndexes;

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param indexes the indexes to create.
     */
    public CreateIndexesOperation(final MongoNamespace namespace, final List<BsonDocument> indexes) {
        this.namespace = notNull("namespace", namespace);
        this.indexes = notNull("indexes", indexes);
        this.systemIndexes = new MongoNamespace(namespace.getDatabaseName(), "system.indexes");
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(binding, new CallableWithConnection<Void>() {
            @Override
            public Void call(final Connection connection) {
                if (serverIsAtLeastVersionTwoDotSix(connection)) {
                    try {
                        executeWrappedCommandProtocol(namespace.getDatabaseName(), getCommand(), connection);
                    } catch (CommandFailureException e) {
                        throw checkForDuplicateKeyError(e);
                    }
                } else {
                    for (BsonDocument index : indexes) {
                        asInsertProtocol(index).execute(connection);
                    }
                }
                return null;
            }
        });
    }

    @Override
    public MongoFuture<Void> executeAsync(final AsyncWriteBinding binding) {
        return withConnection(binding, new AsyncCallableWithConnection<Void>() {
            @Override
            public MongoFuture<Void> call(final Connection connection) {
                final SingleResultFuture<Void> future = new SingleResultFuture<Void>();
                if (serverIsAtLeastVersionTwoDotSix(connection)) {
                    executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), getCommand(), connection)
                    .register(new SingleResultCallback<BsonDocument>() {
                        @Override
                        public void onResult(final BsonDocument result, final MongoException e) {
                            future.init(null, translateException(e));
                        }
                    });
                } else {
                    executeInsertProtocolAsync(indexes, connection, future);
                }
                return future;
            }
        });
    }

    private void executeInsertProtocolAsync(final List<BsonDocument> indexesRemaining, final Connection connection,
                                            final SingleResultFuture<Void> future) {
        BsonDocument index = indexesRemaining.remove(0);
        asInsertProtocol(index).executeAsync(connection)
                               .register(new SingleResultCallback<WriteResult>() {
                                   @Override
                                   public void onResult(final WriteResult result, final MongoException e) {
                                       MongoException translatedException = translateException(e);
                                       if (translatedException != null) {
                                           future.init(null, translatedException);
                                       } else if (indexesRemaining.isEmpty()) {
                                           future.init(null, null);
                                       } else {
                                           executeInsertProtocolAsync(indexesRemaining, connection, future);
                                       }
                                   }
                               });
    }

    private BsonDocument getCommand() {
        BsonDocument command = new BsonDocument("createIndexes", new BsonString(namespace.getCollectionName()));
        BsonArray array = new BsonArray();
        for (BsonDocument index : indexes) {
            if (!index.containsKey("key")) {
                throw new IllegalStateException("The index document does not contain an key");
            }
            if (!index.containsKey("name")) {
                index.put("name", generateIndexName(index));
            }
            array.add(index);
        }
        command.put("indexes", array);
        return command;
    }

    @SuppressWarnings("unchecked")
    private InsertProtocol asInsertProtocol(final BsonDocument index) {
        return new InsertProtocol(systemIndexes, true, WriteConcern.ACKNOWLEDGED, asList(new InsertRequest(index)));
    }

    private MongoException translateException(final MongoException e) {
        return (e instanceof CommandFailureException) ? checkForDuplicateKeyError((CommandFailureException) e) : e;
    }

    private MongoException checkForDuplicateKeyError(final CommandFailureException e) {
        if (DUPLICATE_KEY_ERROR_CODES.contains(e.getCode())) {
            return new MongoException.DuplicateKey(e.getResponse(), e.getServerAddress(), new com.mongodb.WriteResult(0, false, null));
        } else {
            return e;
        }
    }

    /**
     * Convenience method to generate an index name from the set of fields it is over.
     *
     * @return a string representation of this index's fields
     */
    private BsonString generateIndexName(final BsonDocument index) {
        StringBuilder indexName = new StringBuilder();
        for (final String keyNames : index.getDocument("key").keySet()) {
            if (indexName.length() != 0) {
                indexName.append('_');
            }
            indexName.append(keyNames).append('_');
            BsonValue ascOrDescValue = index.getDocument("key").get(keyNames);
            if (ascOrDescValue instanceof BsonInt32) {
                indexName.append(((BsonInt32) ascOrDescValue).getValue());
            } else if (ascOrDescValue instanceof BsonString) {
                indexName.append(((BsonString) ascOrDescValue).getValue().replace(' ', '_'));
            }
        }
        return new BsonString(indexName.toString());
    }
}
