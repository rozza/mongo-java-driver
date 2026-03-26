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
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.internal.connection.OperationContext;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.RawBsonDocument;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChangeStreamBatchCursorUnitTest {

    @SuppressWarnings("unchecked")
    @Test
    void shouldCallTheUnderlyingCommandBatchCursor() {
        ChangeStreamOperation<BsonDocument> changeStreamOperation = mock(ChangeStreamOperation.class);
        ReadBinding binding = mock(ReadBinding.class);
        when(binding.retain()).thenReturn(binding);
        BsonDocument resumeToken = new BsonDocument("_id", new BsonInt32(1));
        OperationContext operationContext = getOperationContext();
        Cursor<RawBsonDocument> wrapped = mock(Cursor.class);

        ChangeStreamBatchCursor<BsonDocument> cursor = new ChangeStreamBatchCursor<>(changeStreamOperation,
                wrapped, binding, operationContext, resumeToken,
                ServerVersionHelper.FOUR_DOT_FOUR_WIRE_VERSION);

        cursor.setBatchSize(10);
        verify(wrapped).setBatchSize(10);

        when(wrapped.tryNext(any(OperationContext.class))).thenReturn(null);
        cursor.tryNext();
        verify(wrapped).tryNext(any(OperationContext.class));
        verify(wrapped).getPostBatchResumeToken();

        when(wrapped.next(any(OperationContext.class))).thenReturn(Collections.emptyList());
        cursor.next();
        verify(wrapped).next(any(OperationContext.class));
        verify(wrapped, times(2)).getPostBatchResumeToken();

        cursor.close();
        verify(wrapped).close(any(OperationContext.class));

        // Second close should not call wrapped.close again
        cursor.close();
        verify(wrapped, times(1)).close(any(OperationContext.class));
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
