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

package com.mongodb.internal.binding;

import com.mongodb.ReadPreference;
import com.mongodb.internal.async.SingleResultCallback;
import org.junit.jupiter.api.Test;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AsyncSessionBindingSpecificationTest {

    @SuppressWarnings("unchecked")
    @Test
    void shouldWrapPassedInAsyncBinding() {
        AsyncReadWriteBinding wrapped = mock(AsyncReadWriteBinding.class);
        when(wrapped.getReadPreference()).thenReturn(ReadPreference.primary());
        AsyncSessionBinding binding = new AsyncSessionBinding(wrapped);

        binding.getCount();
        verify(wrapped).getCount();

        binding.getReadPreference();
        verify(wrapped).getReadPreference();

        binding.retain();
        verify(wrapped).retain();

        binding.release();
        verify(wrapped).release();

        SingleResultCallback<AsyncConnectionSource> callback = mock(SingleResultCallback.class);
        binding.getReadConnectionSource(OPERATION_CONTEXT, callback);
        verify(wrapped).getReadConnectionSource(eq(OPERATION_CONTEXT), any());

        SingleResultCallback<AsyncConnectionSource> callback2 = mock(SingleResultCallback.class);
        binding.getWriteConnectionSource(OPERATION_CONTEXT, callback2);
        verify(wrapped).getWriteConnectionSource(eq(OPERATION_CONTEXT), any());
    }
}
