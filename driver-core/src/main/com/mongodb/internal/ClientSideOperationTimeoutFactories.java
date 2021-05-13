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
package com.mongodb.internal;

import com.mongodb.lang.Nullable;

/**
 * A factory for creating {@link ClientSideOperationTimeoutFactory} instances
 */
public final class ClientSideOperationTimeoutFactories {

    public static final ClientSideOperationTimeoutFactory NO_TIMEOUT = create(null, 0, 0, 0);

    public static ClientSideOperationTimeoutFactory create(@Nullable final Long timeoutMS) {
        return create(timeoutMS, 0, 0, 0);
    }

    public static ClientSideOperationTimeoutFactory create(@Nullable final Long timeoutMS, final long maxTimeMS) {
        return create(timeoutMS, maxTimeMS, 0, 0);
    }

    public static ClientSideOperationTimeoutFactory create(@Nullable final Long timeoutMS, final long maxTimeMS,
                                                           final long maxAwaitTimeMS) {
        return create(timeoutMS, maxTimeMS, maxAwaitTimeMS, 0);
    }

    public static ClientSideOperationTimeoutFactory create(@Nullable final Long timeoutMS, final long maxTimeMS,
                                                           final long maxAwaitTimeMS, final long maxCommitMS) {
        return new ClientSideOperationTimeoutFactoryImpl(timeoutMS, maxTimeMS, maxAwaitTimeMS, maxCommitMS);
    }

    public static ClientSideOperationTimeoutFactory createMaxCommitMS(@Nullable final Long timeoutMS,
                                                                      @Nullable final Long maxCommitMS) {
        return create(timeoutMS, 0, 0, maxCommitMS != null ? maxCommitMS : 0);
    }

    public static ClientSideOperationTimeoutFactory shared(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory) {
        return new ClientSideOperationTimeoutFactoryShared(clientSideOperationTimeoutFactory);
    }

    private ClientSideOperationTimeoutFactories() {
    }
}
