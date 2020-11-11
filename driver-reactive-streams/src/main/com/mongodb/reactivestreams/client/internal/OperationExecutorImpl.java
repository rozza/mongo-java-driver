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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.client.AsyncClientSession;
import com.mongodb.internal.async.client.ClientSessionBinding;
import com.mongodb.internal.async.client.OperationExecutor;
import com.mongodb.internal.binding.AsyncClusterAwareReadWriteBinding;
import com.mongodb.internal.binding.AsyncClusterBinding;
import com.mongodb.internal.binding.AsyncReadWriteBinding;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.internal.operation.AsyncWriteOperation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.internal.crypt.Crypt;
import com.mongodb.reactivestreams.client.internal.crypt.CryptBinding;

import static com.mongodb.MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL;
import static com.mongodb.MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;

class OperationExecutorImpl implements OperationExecutor {
    private static final Logger LOGGER = Loggers.getLogger("client");
    private final MongoClientImpl mongoClient;
    private final ClientSessionHelper clientSessionHelper;

    OperationExecutorImpl(final MongoClientImpl mongoClient, final ClientSessionHelper clientSessionHelper) {
        this.mongoClient = mongoClient;
        this.clientSessionHelper = clientSessionHelper;
    }

    @Override
    public <T> void execute(final AsyncReadOperation<T> operation, final ReadPreference readPreference, final ReadConcern readConcern,
                            final SingleResultCallback<T> callback) {
        execute(operation, readPreference, readConcern, null, callback);
    }

    @Override
    public <T> void execute(final AsyncReadOperation<T> operation, final ReadPreference readPreference, final ReadConcern readConcern,
                            @Nullable final AsyncClientSession session, final SingleResultCallback<T> callback) {
        notNull("operation", operation);
        notNull("readPreference", readPreference);
        notNull("callback", callback);
        final SingleResultCallback<T> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        clientSessionHelper.withClientSession(session, this, (clientSession, t) -> {
            if (t != null) {
                errHandlingCallback.onResult(null, t);
            } else {
                getReadWriteBinding(readPreference, readConcern, clientSession,
                                    session == null && clientSession != null,
                                    (binding, t1) -> {
                                        if (t1 != null) {
                                            errHandlingCallback.onResult(null, t1);
                                        } else {
                                            if (session != null && session.hasActiveTransaction()
                                                    && !binding.getReadPreference().equals(primary())) {
                                                binding.release();
                                                errHandlingCallback.onResult(null,
                                                                             new MongoClientException(
                                                                                     "Read preference in a transaction must be primary"));
                                            } else {
                                                operation.executeAsync(binding, (result, t11) -> {
                                                    labelException(session, t11);
                                                    unpinServerAddressOnTransientTransactionError(session, t11);
                                                    binding.release();
                                                    errHandlingCallback.onResult(result, t11);
                                                });
                                            }
                                        }
                                    });
            }
        });
    }

    @Override
    public <T> void execute(final AsyncWriteOperation<T> operation, final ReadConcern readConcern, final SingleResultCallback<T> callback) {
        execute(operation, readConcern, null, callback);
    }

    @Override
    public <T> void execute(final AsyncWriteOperation<T> operation, final ReadConcern readConcern,
                            @Nullable final AsyncClientSession session, final SingleResultCallback<T> callback) {
        notNull("operation", operation);
        notNull("callback", callback);
        final SingleResultCallback<T> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        clientSessionHelper.withClientSession(session, this, (clientSession, t) -> {
            if (t != null) {
                errHandlingCallback.onResult(null, t);
            } else {
                getReadWriteBinding(ReadPreference.primary(), readConcern, clientSession,
                                    session == null && clientSession != null,
                                    (binding, t1) -> {
                                        if (t1 != null) {
                                            errHandlingCallback.onResult(null, t1);
                                        } else {
                                            operation.executeAsync(binding, (result, t11) -> {
                                                labelException(session, t11);
                                                unpinServerAddressOnTransientTransactionError(session, t11);
                                                binding.release();
                                                errHandlingCallback.onResult(result, t11);
                                            });
                                        }
                                    });
            }
        });
    }

    private void labelException(@Nullable final AsyncClientSession session, @Nullable final Throwable t) {
        if (session != null && session.hasActiveTransaction()
                && (t instanceof MongoSocketException || t instanceof MongoTimeoutException
                || (t instanceof MongoQueryException && ((MongoQueryException) t).getErrorCode() == 91))
                && !((MongoException) t).hasErrorLabel(UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
            ((MongoException) t).addLabel(TRANSIENT_TRANSACTION_ERROR_LABEL);
        }
    }

    private void unpinServerAddressOnTransientTransactionError(@Nullable final AsyncClientSession session,
                                                               @Nullable final Throwable throwable) {
        if (session != null && throwable instanceof MongoException
                && ((MongoException) throwable).hasErrorLabel(TRANSIENT_TRANSACTION_ERROR_LABEL)) {
            session.setPinnedServerAddress(null);
        }
    }


    private void getReadWriteBinding(final ReadPreference readPreference, final ReadConcern readConcern,
                                     @Nullable final AsyncClientSession session,
                                     final boolean ownsSession,
                                     final SingleResultCallback<AsyncReadWriteBinding> callback) {
        notNull("readPreference", readPreference);
        AsyncClusterAwareReadWriteBinding readWriteBinding = new AsyncClusterBinding(mongoClient.getCluster(),
                                                                                     getReadPreferenceForBinding(readPreference, session),
                                                                                     readConcern);

        Crypt crypt = mongoClient.getCrypt();
        if (crypt != null) {
            readWriteBinding = new CryptBinding(readWriteBinding, crypt);
        }

        if (session != null) {
            callback.onResult(new ClientSessionBinding(session, ownsSession, readWriteBinding), null);
        } else {
            callback.onResult(readWriteBinding, null);
        }
    }

    private ReadPreference getReadPreferenceForBinding(final ReadPreference readPreference, @Nullable final AsyncClientSession session) {
        if (session == null) {
            return readPreference;
        }
        if (session.hasActiveTransaction()) {
            ReadPreference readPreferenceForBinding = session.getTransactionOptions().getReadPreference();
            if (readPreferenceForBinding == null) {
                throw new MongoInternalException("Invariant violated.  Transaction options read preference can not be null");
            }
            return readPreferenceForBinding;
        }
        return readPreference;
    }
}
