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

import com.mongodb.CursorType;
import com.mongodb.client.model.TimeoutMode;
import com.mongodb.lang.Nullable;

import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

/**
 * A factory for creating {@link ClientSideOperationTimeout} instances
 */
public final class ClientSideOperationTimeouts {

    public static final ClientSideOperationTimeout NO_TIMEOUT = create(null, 0, 0, 0);

    public static ClientSideOperationTimeout create(@Nullable final Long timeoutMS) {
        return create(timeoutMS, TimeoutMode.DEFAULT);
    }

    public static ClientSideOperationTimeout create(@Nullable final Long timeoutMS, final long maxTimeMS) {
        return create(timeoutMS, TimeoutMode.DEFAULT, maxTimeMS);
    }

    public static ClientSideOperationTimeout create(@Nullable final Long timeoutMS, final long maxTimeMS, final long maxAwaitTimeMS) {
        return create(timeoutMS, TimeoutMode.DEFAULT, maxTimeMS, maxAwaitTimeMS);
    }

    public static ClientSideOperationTimeout create(@Nullable final Long timeoutMS, final long maxTimeMS, final long maxAwaitTimeMS,
                                                    final long maxCommitMS) {
        return create(timeoutMS, TimeoutMode.DEFAULT, maxTimeMS, maxAwaitTimeMS, maxCommitMS);
    }

    public static ClientSideOperationTimeout create(@Nullable final Long timeoutMS, final TimeoutMode timeoutMode) {
        return create(timeoutMS, timeoutMode, 0);
    }

    public static ClientSideOperationTimeout create(@Nullable final Long timeoutMS, final TimeoutMode timeoutMode, final long maxTimeMS) {
        return create(timeoutMS, timeoutMode, maxTimeMS, 0);
    }

    public static ClientSideOperationTimeout create(@Nullable final Long timeoutMS, final TimeoutMode timeoutMode,
                                                    final long maxTimeMS, final long maxAwaitTimeMS) {
        return create(timeoutMS, timeoutMode, maxTimeMS, maxAwaitTimeMS, 0);
    }

    public static ClientSideOperationTimeout create(@Nullable final Long timeoutMS, final TimeoutMode timeoutMode,
                                                    final CursorType cursorType, final long maxTimeMS, final long maxAwaitTimeMS) {
        TimeoutMode timeoutModeToUse = timeoutMode;
        switch (cursorType) {
            case Tailable:
            case TailableAwait:
                if (timeoutMode.equals(TimeoutMode.CURSOR_LIFETIME)) {
                    throw new IllegalStateException("Tailable cursors only support the TimeoutMode.ITERATION and it was set to: "
                            + "TimeoutMode.CURSOR_LIFETIME.");
                }
                if (timeoutMS != null && (maxAwaitTimeMS != 0 && maxAwaitTimeMS >= timeoutMS)
                        && cursorType.equals(CursorType.TailableAwait)) {
                    throw new IllegalStateException(format("maxAwaitTimeMS cannot be greater or equal to timeoutMS:%n %d >= %d",
                            maxAwaitTimeMS, timeoutMS));
                }
                timeoutModeToUse = TimeoutMode.ITERATION;
                break;
            default:
                break;
        }
        return create(timeoutMS, timeoutModeToUse, maxTimeMS, maxAwaitTimeMS, 0);
    }

    public static ClientSideOperationTimeout create(@Nullable final Long timeoutMS, final TimeoutMode timeoutMode,
                                                    final long maxTimeMS, final long maxAwaitTimeMS, final long maxCommitMS) {
        TimeoutMode timeoutModeToUse = notNull("timeoutMode", timeoutMode);
        if (timeoutModeToUse.equals(TimeoutMode.DEFAULT)) {
            timeoutModeToUse = TimeoutMode.CURSOR_LIFETIME;
        }
        return new ClientSideOperationTimeout(timeoutMS, timeoutModeToUse, maxAwaitTimeMS, maxTimeMS, maxCommitMS);
    }

    public static ClientSideOperationTimeout withMaxCommitMS(@Nullable final Long timeoutMS,
                                                             @Nullable final Long maxCommitMS) {
        return create(timeoutMS, 0, 0, maxCommitMS != null ? maxCommitMS : 0);
    }

    private ClientSideOperationTimeouts() {
    }
}
