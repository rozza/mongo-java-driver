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

import com.mongodb.RequestContext;
import com.mongodb.ServerApi;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;

import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class OperationContext {
    private static final AtomicLong NEXT_ID = new AtomicLong(0);
    private final long operationId;
    @Nullable
    private final ServerApi serverApi;
    private final SessionContext sessionContext;
    private final RequestContext requestContext;
    private final TimeoutContext timeoutContext;

    public OperationContext(@Nullable final ServerApi serverApi,
            final SessionContext sessionContext,
            final RequestContext requestContext,
            final TimeoutContext timeoutContext) {
        this.operationId = NEXT_ID.incrementAndGet();
        this.serverApi = serverApi;
        this.sessionContext = sessionContext;
        this.requestContext = requestContext;
        this.timeoutContext = timeoutContext;
    }

    public long getOperationId() {
        return operationId;
    }

    @Nullable
    public ServerApi getServerApi() {
        return serverApi;
    }

    public SessionContext getSessionContext() {
        return sessionContext;
    }

    public RequestContext getRequestContext() {
        return requestContext;
    }
    public TimeoutContext getTimeoutContext() {
        return timeoutContext;
    }
}
