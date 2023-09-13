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

import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.lang.Nullable;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.bson.codecs.Decoder;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.SyncOperationHelper.executeRetryableRead;


/**
 * An operation that provides a cursor allowing iteration through the metadata of all the databases for a MongoClient.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ListDatabasesOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private final Decoder<T> decoder;
    private boolean retryReads;

    private long maxTimeMS;
    private BsonDocument filter;
    private Boolean nameOnly;
    private Boolean authorizedDatabasesOnly;
    private BsonValue comment;

    public ListDatabasesOperation(final Decoder<T> decoder) {
        this.decoder = notNull("decoder", decoder);
    }

    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    public ListDatabasesOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    public ListDatabasesOperation<T> filter(@Nullable final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    public BsonDocument getFilter() {
        return filter;
    }

    public ListDatabasesOperation<T> nameOnly(final Boolean nameOnly) {
        this.nameOnly = nameOnly;
        return this;
    }

    public ListDatabasesOperation<T> authorizedDatabasesOnly(final Boolean authorizedDatabasesOnly) {
        this.authorizedDatabasesOnly = authorizedDatabasesOnly;
        return this;
    }

    public ListDatabasesOperation<T> retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    public boolean getRetryReads() {
        return retryReads;
    }

    public Boolean getNameOnly() {
        return nameOnly;
    }

    public Boolean getAuthorizedDatabasesOnly() {
        return authorizedDatabasesOnly;
    }

    @Nullable
    public BsonValue getComment() {
        return comment;
    }

    public ListDatabasesOperation<T> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return executeRetryableRead(binding, "admin", getCommandCreator(),
                CommandResultDocumentCodec.create(decoder, "databases"),
                SyncOperationHelper.singleBatchCursorTransformer("databases"), retryReads);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        executeRetryableReadAsync(binding, "admin", getCommandCreator(),
                CommandResultDocumentCodec.create(decoder, "databases"),
                AsyncOperationHelper.asyncSingleBatchCursorTransformer("databases"),
                retryReads, errorHandlingCallback(callback, LOGGER));
    }

    private CommandCreator getCommandCreator() {
        return (serverDescription, connectionDescription) -> getCommand();
    }

    private BsonDocument getCommand() {
        BsonDocument command = new BsonDocument("listDatabases", new BsonInt32(1));
        if (maxTimeMS > 0) {
            command.put("maxTimeMS", new BsonInt64(maxTimeMS));
        }
        if (filter != null) {
            command.put("filter", filter);
        }
        if (nameOnly != null) {
            command.put("nameOnly", new BsonBoolean(nameOnly));
        }
        if (authorizedDatabasesOnly != null) {
            command.put("authorizedDatabases", new BsonBoolean(authorizedDatabasesOnly));
        }
        putIfNotNull(command, "comment", comment);
        return command;
    }
}
