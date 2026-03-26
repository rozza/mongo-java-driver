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

package com.mongodb.internal.connection;

import com.mongodb.ReadPreference;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.Test;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.internal.connection.MessageHelper.LEGACY_HELLO_LOWER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class DefaultServerConnectionTest {

    private final InternalConnection internalConnection = mock(InternalConnection.class);

    @SuppressWarnings("unchecked")
    @Test
    void shouldExecuteCommandProtocolAsynchronously() {
        BsonDocument command = new BsonDocument(LEGACY_HELLO_LOWER, new BsonInt32(1));
        BsonDocumentCodec codec = new BsonDocumentCodec();
        ProtocolExecutor executor = mock(ProtocolExecutor.class);
        SingleResultCallback<BsonDocument> callback = mock(SingleResultCallback.class);
        DefaultServerConnection connection = new DefaultServerConnection(internalConnection, executor,
                ClusterConnectionMode.MULTIPLE);

        connection.commandAsync("test", command, NoOpFieldNameValidator.INSTANCE, ReadPreference.primary(), codec,
                OPERATION_CONTEXT, callback);

        verify(executor).executeAsync(any(CommandProtocolImpl.class), eq(internalConnection),
                eq(OPERATION_CONTEXT.getSessionContext()), any(SingleResultCallback.class));
    }
}
