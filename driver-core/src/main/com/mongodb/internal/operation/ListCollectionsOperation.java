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

package com.mongodb.internal.operation;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.ClientSideOperationTimeoutFactory;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.QueryResult;
import com.mongodb.internal.operation.AsyncCommandOperationHelper.CommandReadTransformerAsync;
import com.mongodb.internal.operation.SyncCommandOperationHelper.CommandReadTransformer;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonInt32;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncCommandOperationHelper.executeCommandAsyncWithConnection;
import static com.mongodb.internal.operation.AsyncOperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.internal.operation.AsyncOperationHelper.createEmptyAsyncBatchCursor;
import static com.mongodb.internal.operation.AsyncOperationHelper.cursorDocumentToAsyncBatchCursor;
import static com.mongodb.internal.operation.AsyncOperationHelper.releasingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.withAsyncReadConnection;
import static com.mongodb.internal.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.internal.operation.CommandOperationHelper.rethrowIfNotNamespaceError;
import static com.mongodb.internal.operation.CursorHelper.getCursorDocumentFromBatchSize;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.DocumentHelper.putIfTrue;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotZero;
import static com.mongodb.internal.operation.SyncCommandOperationHelper.executeCommandWithConnection;
import static com.mongodb.internal.operation.SyncOperationHelper.CallableWithSource;
import static com.mongodb.internal.operation.SyncOperationHelper.createEmptyBatchCursor;
import static com.mongodb.internal.operation.SyncOperationHelper.cursorDocumentToBatchCursor;
import static com.mongodb.internal.operation.SyncOperationHelper.withReadConnectionSource;
import static java.lang.String.format;
import static java.util.Arrays.asList;

/**
 * An operation that provides a cursor allowing iteration through the metadata of all the collections in a database.  This operation
 * ensures that the value of the {@code name} field of each returned document is the simple name of the collection rather than the full
 * namespace.
 *
 * @param <T> the document type
 * @since 3.0
 */
public class ListCollectionsOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory;
    private final String databaseName;
    private final Decoder<T> decoder;
    private boolean retryReads;
    private BsonDocument filter;
    private int batchSize;
    private boolean nameOnly;

    /**
     * Construct a new instance.
     *
     * @param clientSideOperationTimeoutFactory the client side operation timeout factory
     * @param databaseName the name of the database for the operation.
     * @param decoder the decoder to use for the results
     */
    public ListCollectionsOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory, final String databaseName,
                                    final Decoder<T> decoder) {
        this.clientSideOperationTimeoutFactory = notNull("clientSideOperationTimeoutFactory", clientSideOperationTimeoutFactory);
        this.databaseName = notNull("databaseName", databaseName);
        this.decoder = notNull("decoder", decoder);
    }

    /**
     * Gets the query filter.
     *
     * @return the query filter
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public BsonDocument getFilter() {
        return filter;
    }

    /**
     * Gets whether only the collection names should be returned.
     *
     * @return true if only the collection names should be returned
     * @since 3.8
     * @mongodb.server.release 4.0
     */
    public boolean isNameOnly() {
        return nameOnly;
    }

    /**
     * Sets the query filter to apply to the query.
     *
     * @param filter the filter, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public ListCollectionsOperation<T> filter(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Sets the query filter to apply to the query.
     * <p>
     *     Note: this is advisory only, and should be considered an optimization.  Server versions prior to MongoDB 4.0 will ignore
     *     this request.
     * </p>
     *
     * @param nameOnly true if only the collection names should be requested from the server
     * @return this
     * @since 3.8
     * @mongodb.server.release 4.0
     */
    public ListCollectionsOperation<T> nameOnly(final boolean nameOnly) {
        this.nameOnly = nameOnly;
        return this;
    }

    /**
     * Gets the number of documents to return per batch.
     *
     * @return the batch size
     * @mongodb.server.release 3.0
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public Integer getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.server.release 3.0
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public ListCollectionsOperation<T> batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Enables retryable reads if a read fails due to a network error.
     *
     * @param retryReads true if reads should be retried
     * @return this
     * @since 3.11
     */
    public ListCollectionsOperation<T> retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    /**
     * Gets the value for retryable reads. The default is true.
     *
     * @return the retryable reads value
     * @since 3.11
     */
    public boolean getRetryReads() {
        return retryReads;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return withReadConnectionSource(clientSideOperationTimeoutFactory.create(), binding, new CallableWithSource<BatchCursor<T>>() {
            @Override
            public BatchCursor<T> call(final ClientSideOperationTimeout clientSideOperationTimeout, final ConnectionSource source) {
                Connection connection = source.getConnection();
                if (serverIsAtLeastVersionThreeDotZero(connection.getDescription())) {
                    try {
                        return executeCommandWithConnection(clientSideOperationTimeout, binding, source, databaseName,
                                getCommandCreator(), createCommandDecoder(), commandTransformer(), retryReads, connection);
                    } catch (MongoCommandException e) {
                        return rethrowIfNotNamespaceError(e, createEmptyBatchCursor(clientSideOperationTimeout, createNamespace(), decoder,
                                source.getServerDescription().getAddress(), batchSize));
                    }
                } else {
                    try {
                        return new ProjectingBatchCursor(new QueryBatchCursor<BsonDocument>(clientSideOperationTimeout,
                                connection.query(getNamespace(), asQueryDocument(clientSideOperationTimeout, connection.getDescription(),
                                        binding.getReadPreference()), null, 0, 0, batchSize, binding.getReadPreference().isSlaveOk(),
                                        false, false, false, false, false, new BsonDocumentCodec()), 0, batchSize,
                                new BsonDocumentCodec(), source));
                    } finally {
                        connection.release();
                    }
                }
            }
        });
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        withAsyncReadConnection(clientSideOperationTimeoutFactory.create(), binding, new AsyncCallableWithConnectionAndSource() {
            @Override
            public void call(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncConnectionSource source,
                             final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<AsyncBatchCursor<T>> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    if (serverIsAtLeastVersionThreeDotZero(connection.getDescription())) {
                        executeCommandAsyncWithConnection(clientSideOperationTimeout, binding, source, databaseName, getCommandCreator(),
                                createCommandDecoder(), asyncTransformer(), retryReads, connection,
                                new SingleResultCallback<AsyncBatchCursor<T>>() {
                                    @Override
                                    public void onResult(final AsyncBatchCursor<T> result, final Throwable t) {
                                        if (t != null && !isNamespaceError(t)) {
                                            errHandlingCallback.onResult(null, t);
                                        } else {
                                            errHandlingCallback.onResult(result != null ? result
                                                    : emptyAsyncCursor(clientSideOperationTimeout, source), null);
                                        }
                                    }
                                });
                    } else {
                        final SingleResultCallback<AsyncBatchCursor<T>> wrappedCallback = releasingCallback(errHandlingCallback,
                                source, connection);
                        connection.queryAsync(getNamespace(), asQueryDocument(clientSideOperationTimeout, connection.getDescription(),
                                binding.getReadPreference()),
                                null, 0, 0, batchSize, binding.getReadPreference().isSlaveOk(), false, false, false, false, false,
                                new BsonDocumentCodec(), new SingleResultCallback<QueryResult<BsonDocument>>() {
                                    @Override
                                    public void onResult(final QueryResult<BsonDocument> result, final Throwable t) {
                                        if (t != null) {
                                            wrappedCallback.onResult(null, t);
                                        } else {
                                            wrappedCallback.onResult(new ProjectingAsyncBatchCursor(
                                                    new AsyncQueryBatchCursor<BsonDocument>(clientSideOperationTimeout, result, 0,
                                                            batchSize, new BsonDocumentCodec(), source, connection)
                                            ), null);
                                        }
                                    }
                                });
                    }
                }
            }
        });
    }

    private AsyncBatchCursor<T> emptyAsyncCursor(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                 final AsyncConnectionSource source) {
        return createEmptyAsyncBatchCursor(clientSideOperationTimeout, createNamespace(), source.getServerDescription().getAddress());
    }

    private MongoNamespace createNamespace() {
        return new MongoNamespace(databaseName, "$cmd.listCollections");
    }

    private CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>> asyncTransformer() {
        return new CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>>() {
            @Override
            public AsyncBatchCursor<T> apply(final ClientSideOperationTimeout clientSideOperationTimeout,
                                             final AsyncConnectionSource source, final AsyncConnection connection,
                                             final BsonDocument result) {
                return cursorDocumentToAsyncBatchCursor(clientSideOperationTimeout, result.getDocument("cursor"), decoder, source,
                        connection, batchSize);
            }
        };
    }

    private CommandReadTransformer<BsonDocument, BatchCursor<T>> commandTransformer() {
        return new CommandReadTransformer<BsonDocument, BatchCursor<T>>() {
            @Override
            public BatchCursor<T> apply(final ClientSideOperationTimeout clientSideOperationTimeout, final ConnectionSource source,
                                        final Connection connection, final BsonDocument result) {
                return cursorDocumentToBatchCursor(clientSideOperationTimeout, result.getDocument("cursor"), decoder, source, batchSize);
            }
        };
    }

    private MongoNamespace getNamespace() {
        return new MongoNamespace(databaseName, "system.namespaces");
    }

    private CommandCreator getCommandCreator() {
        return new CommandCreator() {
            @Override
            public BsonDocument create(final ClientSideOperationTimeout clientSideOperationTimeout,
                                       final ServerDescription serverDescription,
                                       final ConnectionDescription connectionDescription) {
                BsonDocument command = new BsonDocument("listCollections", new BsonInt32(1))
                        .append("cursor", getCursorDocumentFromBatchSize(batchSize == 0 ? null : batchSize));
                putIfNotNull(command, "filter", filter);
                putIfTrue(command, "nameOnly", nameOnly);
                putIfNotZero(command, "maxTimeMS", clientSideOperationTimeout.getMaxTimeMS());
                return command;
            }
        };
    }



    private BsonDocument asQueryDocument(final ClientSideOperationTimeout clientSideOperationTimeout,
                                         final ConnectionDescription connectionDescription,
                                         final ReadPreference readPreference) {
        BsonDocument document = new BsonDocument();
        BsonDocument transformedFilter = null;
        if (filter != null) {
            if (filter.containsKey("name")) {
                if (!filter.isString("name")) {
                    throw new IllegalArgumentException("When filtering collections on MongoDB versions < 3.0 the name field "
                                                       + "must be a string");
                }
                transformedFilter = new BsonDocument();
                transformedFilter.putAll(filter);
                transformedFilter.put("name", new BsonString(format("%s.%s", databaseName, filter.getString("name").getValue())));
            } else {
                transformedFilter = filter;
            }
        }
        BsonDocument indexExcludingRegex = new BsonDocument("name", new BsonRegularExpression("^[^$]*$"));
        BsonDocument query = transformedFilter == null ? indexExcludingRegex
                                                       : new BsonDocument("$and", new BsonArray(asList(indexExcludingRegex,
                                                                                                       transformedFilter)));


        document.put("$query", query);
        if (connectionDescription.getServerType() == SHARD_ROUTER && !readPreference.equals(primary())) {
            document.put("$readPreference", readPreference.toDocument());
        }
        putIfNotZero(document, "maxTimeMS", clientSideOperationTimeout.getMaxTimeMS());
        return document;
    }

    private Codec<BsonDocument> createCommandDecoder() {
        return CommandResultDocumentCodec.create(decoder, "firstBatch");
    }

    private final class ProjectingBatchCursor implements BatchCursor<T> {

        private final BatchCursor<BsonDocument> delegate;

        private ProjectingBatchCursor(final BatchCursor<BsonDocument> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void remove() {
            delegate.remove();
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public List<T> next() {
            return projectFromFullNamespaceToCollectionName(delegate.next());
        }

        @Override
        public void setBatchSize(final int batchSize) {
            delegate.setBatchSize(batchSize);
        }

        @Override
        public int getBatchSize() {
            return delegate.getBatchSize();
        }

        @Override
        public List<T> tryNext() {
           return projectFromFullNamespaceToCollectionName(delegate.tryNext());
        }

        @Override
        public ServerCursor getServerCursor() {
            return delegate.getServerCursor();
        }

        @Override
        public ServerAddress getServerAddress() {
            return delegate.getServerAddress();
        }

        @Override
        public ClientSideOperationTimeout getClientSideOperationTimeout() {
            return delegate.getClientSideOperationTimeout();
        }
    }

    private final class ProjectingAsyncBatchCursor implements AsyncBatchCursor<T> {

        private final AsyncBatchCursor<BsonDocument> delegate;

        private ProjectingAsyncBatchCursor(final AsyncBatchCursor<BsonDocument> delegate) {
            this.delegate = delegate;
        }

        @Override
        public ClientSideOperationTimeout getClientSideOperationTimeout() {
            return delegate.getClientSideOperationTimeout();
        }

        @Override
        public void next(final SingleResultCallback<List<T>> callback) {
            delegate.next(new SingleResultCallback<List<BsonDocument>>() {
                @Override
                public void onResult(final List<BsonDocument> result, final Throwable t) {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        callback.onResult(projectFromFullNamespaceToCollectionName(result), null);
                    }
                }
            });
        }

        @Override
        public void tryNext(final SingleResultCallback<List<T>> callback) {
            delegate.tryNext(new SingleResultCallback<List<BsonDocument>>() {
                @Override
                public void onResult(final List<BsonDocument> result, final Throwable t) {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        callback.onResult(projectFromFullNamespaceToCollectionName(result), null);
                    }
                }
            });
        }

        @Override
        public void setBatchSize(final int batchSize) {
            delegate.setBatchSize(batchSize);
        }

        @Override
        public int getBatchSize() {
            return delegate.getBatchSize();
        }

        @Override
        public boolean isClosed() {
            return delegate.isClosed();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private List<T> projectFromFullNamespaceToCollectionName(final List<BsonDocument> unstripped) {
        if (unstripped == null) {
            return null;
        }
        List<T> stripped = new ArrayList<T>(unstripped.size());
        String prefix = databaseName + ".";
        for (BsonDocument cur : unstripped) {
            String name = cur.getString("name").getValue();
            String collectionName = name.substring(prefix.length());
            cur.put("name", new BsonString(collectionName));
            stripped.add(decoder.decode(new BsonDocumentReader(cur), DecoderContext.builder().build()));
        }
        return stripped;
    }
}
