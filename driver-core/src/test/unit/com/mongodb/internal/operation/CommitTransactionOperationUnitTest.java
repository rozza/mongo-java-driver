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

import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.session.SessionContext;
import org.junit.jupiter.api.Test;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CommitTransactionOperationUnitTest {

    @Test
    void shouldAddUnknownTransactionCommitResultErrorLabelToMongoTimeoutException() {
        SessionContext sessionContext = mock(SessionContext.class);
        when(sessionContext.getReadConcern()).thenReturn(ReadConcern.DEFAULT);
        when(sessionContext.hasActiveTransaction()).thenReturn(true);

        WriteBinding writeBinding = mock(WriteBinding.class);
        when(writeBinding.getWriteConnectionSource(any())).thenThrow(new MongoTimeoutException("Time out!"));

        CommitTransactionOperation operation = new CommitTransactionOperation(WriteConcern.ACKNOWLEDGED);

        MongoTimeoutException e = assertThrows(MongoTimeoutException.class, () ->
                operation.execute(writeBinding, OPERATION_CONTEXT.withSessionContext(sessionContext)));
        assertTrue(e.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL));
    }

    @Test
    void shouldAddUnknownTransactionCommitResultErrorLabelToMongoTimeoutExceptionAsync() {
        SessionContext sessionContext = mock(SessionContext.class);
        when(sessionContext.getReadConcern()).thenReturn(ReadConcern.DEFAULT);
        when(sessionContext.hasActiveTransaction()).thenReturn(true);

        AsyncWriteBinding writeBinding = mock(AsyncWriteBinding.class);
        doAnswer(invocation -> {
            SingleResultCallback<?> callback = invocation.getArgument(1);
            callback.onResult(null, new MongoTimeoutException("Time out!"));
            return null;
        }).when(writeBinding).getWriteConnectionSource(any(OperationContext.class), any(SingleResultCallback.class));

        CommitTransactionOperation operation = new CommitTransactionOperation(WriteConcern.ACKNOWLEDGED);
        FutureResultCallback<Void> callback = new FutureResultCallback<>();

        operation.executeAsync(writeBinding, OPERATION_CONTEXT.withSessionContext(sessionContext), callback);

        MongoTimeoutException e = assertThrows(MongoTimeoutException.class, callback::get);
        assertTrue(e.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL));
    }
}
