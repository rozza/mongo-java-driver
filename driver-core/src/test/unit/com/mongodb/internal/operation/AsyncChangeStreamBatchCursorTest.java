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

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.observability.micrometer.TracingManager;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AsyncChangeStreamBatchCursorTest {

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldCallTheUnderlyingAsyncCommandBatchCursor(final boolean hasTimeoutMS) {
        ChangeStreamOperation<Document> changeStreamOperation = mock(ChangeStreamOperation.class);
        AsyncReadBinding binding = mock(AsyncReadBinding.class);
        OperationContext operationContext = getOperationContext();
        when(operationContext.getTracingManager()).thenReturn(TracingManager.NO_OP);
        when(operationContext.getTimeoutContext().hasTimeoutMS()).thenReturn(hasTimeoutMS);

        SingleResultCallback<List<Document>> callback = mock(SingleResultCallback.class);
        AsyncCursor<RawBsonDocument> wrapped = mock(AsyncCursor.class);
        AsyncChangeStreamBatchCursor<Document> cursor = new AsyncChangeStreamBatchCursor<>(changeStreamOperation,
                wrapped, binding, operationContext, null,
                ServerVersionHelper.FOUR_DOT_FOUR_WIRE_VERSION);

        cursor.setBatchSize(10);
        verify(wrapped).setBatchSize(10);

        doAnswer(invocation -> {
            SingleResultCallback<List<RawBsonDocument>> cb = invocation.getArgument(1);
            cb.onResult(Collections.emptyList(), null);
            return null;
        }).when(wrapped).next(any(OperationContext.class), any());
        cursor.next(callback);
        verify(wrapped).next(any(OperationContext.class), any());

        cursor.close();
        verify(wrapped).close(any());
        verify(binding).release();

        // Second close should not call wrapped.close or binding.release again
        cursor.close();
        verify(wrapped).close(any());
        verify(binding).release();
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldThrowMongoExceptionWhenNextCalledAfterClose(final boolean hasTimeoutMS) {
        ChangeStreamOperation<Document> changeStreamOperation = mock(ChangeStreamOperation.class);
        AsyncReadBinding binding = mock(AsyncReadBinding.class);
        OperationContext operationContext = getOperationContext();
        when(operationContext.getTracingManager()).thenReturn(TracingManager.NO_OP);
        when(operationContext.getTimeoutContext().hasTimeoutMS()).thenReturn(hasTimeoutMS);

        AsyncCursor<RawBsonDocument> wrapped = mock(AsyncCursor.class);
        AsyncChangeStreamBatchCursor<Document> cursor = new AsyncChangeStreamBatchCursor<>(changeStreamOperation,
                wrapped, binding, operationContext, null,
                ServerVersionHelper.FOUR_DOT_FOUR_WIRE_VERSION);

        cursor.close();

        MongoException exception = assertThrows(MongoException.class, () -> nextBatch(cursor));
        assertEquals("next() called after the cursor was closed.", exception.getMessage());
    }

    private List<Document> nextBatch(final AsyncChangeStreamBatchCursor<Document> cursor) throws Exception {
        FutureResultCallback<List<Document>> futureResultCallback = new FutureResultCallback<>();
        cursor.next(futureResultCallback);
        return futureResultCallback.get(1, SECONDS);
    }

    private OperationContext getOperationContext() {
        TimeoutContext timeoutContext = spy(new TimeoutContext(TimeoutSettings.create(
                MongoClientSettings.builder().timeout(3, TimeUnit.SECONDS).build())));
        return spy(new OperationContext(
                IgnorableRequestContext.INSTANCE,
                NoOpSessionContext.INSTANCE,
                timeoutContext, null));
    }
}
