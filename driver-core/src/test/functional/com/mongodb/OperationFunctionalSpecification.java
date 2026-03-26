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

package com.mongodb;

import com.mongodb.async.FutureResultCallback;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.client.test.Worker;
import com.mongodb.client.test.WorkerCodec;
import com.mongodb.internal.binding.AsyncSessionBinding;
import com.mongodb.internal.binding.SessionBinding;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.connection.ServerHelper;
import com.mongodb.internal.operation.MixedBulkWriteOperation;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ClusterFixture.TIMEOUT;
import static com.mongodb.ClusterFixture.checkReferenceCountReachesTarget;
import static com.mongodb.ClusterFixture.executeAsync;
import static com.mongodb.ClusterFixture.getAsyncBinding;
import static com.mongodb.ClusterFixture.getBinding;
import static com.mongodb.ClusterFixture.getPrimary;
import static com.mongodb.ClusterFixture.loopCursor;
import static com.mongodb.WriteConcern.ACKNOWLEDGED;

public class OperationFunctionalSpecification {

    protected static final Collation DEFAULT_COLLATION = Collation.builder()
            .locale("en")
            .caseLevel(true)
            .collationCaseFirst(CollationCaseFirst.OFF)
            .collationStrength(CollationStrength.IDENTICAL)
            .numericOrdering(true)
            .collationAlternate(CollationAlternate.SHIFTED)
            .collationMaxVariable(CollationMaxVariable.SPACE)
            .normalization(true)
            .backwards(true)
            .build();

    protected static final Collation CASE_INSENSITIVE_COLLATION = Collation.builder()
            .locale("en")
            .collationStrength(CollationStrength.SECONDARY)
            .build();

    @BeforeEach
    public void setUp() {
        setupInternal();
    }

    protected void setupInternal() {
        ServerHelper.checkPool(getPrimary());
        CollectionHelper.drop(getNamespace());
    }

    @AfterEach
    public void tearDown() {
        cleanupInternal();
    }

    protected void cleanupInternal() {
        CollectionHelper.drop(getNamespace());
        checkReferenceCountReachesTarget(getBinding(), 1);
        checkReferenceCountReachesTarget(getAsyncBinding(), 1);
        ServerHelper.checkPool(getPrimary());
    }

    protected String getDatabaseName() {
        return ClusterFixture.getDefaultDatabaseName();
    }

    protected String getCollectionName() {
        return getClass().getName();
    }

    protected MongoNamespace getNamespace() {
        return new MongoNamespace(getDatabaseName(), getCollectionName());
    }

    protected CollectionHelper<Document> getCollectionHelper() {
        return getCollectionHelper(getNamespace());
    }

    protected CollectionHelper<Document> getCollectionHelper(final MongoNamespace namespace) {
        return new CollectionHelper<>(new DocumentCodec(), namespace);
    }

    protected CollectionHelper<Worker> getWorkerCollectionHelper() {
        return new CollectionHelper<>(new WorkerCodec(), getNamespace());
    }

    @SuppressWarnings("unchecked")
    protected <T> T execute(final Object operation, final boolean async) {
        if (async) {
            try {
                return (T) ClusterFixture.executeAsync((com.mongodb.internal.operation.ReadOperation<?, T>) operation);
            } catch (ClassCastException e) {
                try {
                    return (T) ClusterFixture.executeAsync((com.mongodb.internal.operation.WriteOperation<T>) operation);
                } catch (Throwable throwable) {
                    throw new RuntimeException(throwable);
                }
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        } else {
            try {
                return (T) ClusterFixture.executeSync((com.mongodb.internal.operation.ReadOperation<T, ?>) operation);
            } catch (ClassCastException e) {
                return (T) ClusterFixture.executeSync((com.mongodb.internal.operation.WriteOperation<T>) operation);
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> T execute(final Object operation) {
        try {
            return (T) ClusterFixture.executeSync((com.mongodb.internal.operation.ReadOperation<T, ?>) operation);
        } catch (ClassCastException e) {
            return (T) ClusterFixture.executeSync((com.mongodb.internal.operation.WriteOperation<T>) operation);
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> T executeWithSession(final Object operation, final boolean async) {
        if (async) {
            try {
                return (T) ClusterFixture.executeAsync((com.mongodb.internal.operation.ReadOperation<?, T>) operation,
                        new AsyncSessionBinding(getAsyncBinding()));
            } catch (ClassCastException e) {
                try {
                    return (T) ClusterFixture.executeAsync((com.mongodb.internal.operation.WriteOperation<T>) operation,
                            new AsyncSessionBinding(getAsyncBinding()));
                } catch (Throwable throwable) {
                    throw new RuntimeException(throwable);
                }
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        } else {
            try {
                return (T) ClusterFixture.executeSync((com.mongodb.internal.operation.ReadOperation<T, ?>) operation,
                        new SessionBinding(getBinding()));
            } catch (ClassCastException e) {
                return (T) ClusterFixture.executeSync((com.mongodb.internal.operation.WriteOperation<T>) operation,
                        new SessionBinding(getBinding()));
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> List<T> executeAndCollectBatchCursorResults(final Object operation, final boolean async) {
        Object cursor = execute(operation, async);
        if (async) {
            try {
                return ClusterFixture.collectCursorResults((com.mongodb.internal.async.AsyncBatchCursor<T>) cursor);
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        } else {
            return ClusterFixture.collectCursorResults((com.mongodb.internal.operation.BatchCursor<T>) cursor);
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> List<T> next(final Object cursor, final boolean async, final int minimumCount) {
        return next(cursor, async, false, minimumCount);
    }

    @SuppressWarnings("unchecked")
    protected <T> List<T> next(final Object cursor, final boolean async, final boolean callHasNextBeforeNext,
                               final int minimumCount) {
        List<T> retVal = new ArrayList<>();
        while (retVal.size() < minimumCount) {
            retVal.addAll(doNext(cursor, async, callHasNextBeforeNext));
        }
        return retVal;
    }

    @SuppressWarnings("unchecked")
    protected <T> List<T> next(final Object cursor, final boolean async) {
        return doNext(cursor, async, false);
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> doNext(final Object cursor, final boolean async, final boolean callHasNextBeforeNext) {
        if (async) {
            FutureResultCallback<List<T>> futureResultCallback = new FutureResultCallback<>();
            try {
                cursor.getClass().getMethod("next", com.mongodb.async.FutureResultCallback.class)
                        .invoke(cursor, futureResultCallback);
            } catch (Exception e) {
                try {
                    cursor.getClass().getMethod("next", com.mongodb.internal.async.SingleResultCallback.class)
                            .invoke(cursor, futureResultCallback);
                } catch (Exception e2) {
                    throw new RuntimeException(e2);
                }
            }
            try {
                return futureResultCallback.get(TIMEOUT, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            try {
                if (callHasNextBeforeNext) {
                    cursor.getClass().getMethod("hasNext").invoke(cursor);
                }
                return (List<T>) cursor.getClass().getMethod("next").invoke(cursor);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
