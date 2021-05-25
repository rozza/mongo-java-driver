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

import com.mongodb.client.model.TimeoutMode;
import com.mongodb.lang.Nullable;

import java.util.Objects;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Client Side Operation Timeout.
 *
 * <p>Includes support for the deprecated {@code maxTimeMS} and {@code maxCommitTimeMS} operation configurations</p>
 */
public class ClientSideOperationTimeout {

    private final Long timeoutMS;
    private final TimeoutMode timeoutMode;
    private final long maxAwaitTimeMS;

    // Deprecated operation based operation timeouts
    private final long maxTimeMS;
    private final long maxCommitTimeMS;

    @Nullable
    private Timeout timeout;

    public ClientSideOperationTimeout(@Nullable final Long timeoutMS,
                                      final TimeoutMode timeoutMode,
                                      final long maxAwaitTimeMS,
                                      final long maxTimeMS,
                                      final long maxCommitTimeMS) {
        this.timeoutMS = timeoutMS;
        this.timeoutMode = timeoutMode;
        this.maxAwaitTimeMS = maxAwaitTimeMS;
        this.maxTimeMS = timeoutMS == null ? maxTimeMS : 0;
        this.maxCommitTimeMS = timeoutMS == null ? maxCommitTimeMS : 0;
        setTimeout();
    }

    /**
     * Allows for the differentiation between users explicitly setting a global operation timeout via {@code timeoutMS}.
     *
     * @return true if a timeout has been set.
     */
    public boolean hasTimeoutMS() {
        return timeoutMS != null;
    }

    /**
     * Checks the expiry of the timeout.
     *
     * @return true if the timeout has been set and it has expired
     */
    public boolean expired() {
        return timeout != null && timeout.expired();
    }

    /**
     * Checks if a cursor can getMore.
     *
     * <p>Resets the timeoutMS if the timeout mode is TimeoutMode.ITERATION</p>
     *
     * @return true if the cursor can call get more.
     */
    public boolean canCallGetMore() {
        resetTimeout();
        return !expired();
    }

    /**
     * Returns the remaining {@code timeoutMS} if set or the {@code alternativeTimeoutMS}.
     *
     * @param alternativeTimeoutMS the alternative timeout.
     * @return timeout to use.
     */
    public long timeoutOrAlternative(final long alternativeTimeoutMS) {
        if (timeoutMS == null) {
            return alternativeTimeoutMS;
        } else if (timeoutMS == 0) {
            return timeoutMS;
        } else {
            return timeoutRemainingMS();
        }
    }

    /**
     * Calculates the minimum timeout value between two possible timeouts.
     *
     * @param alternativeTimeoutMS the alternative timeout
     * @return the minimum value to use.
     */
    public long calculateMin(final long alternativeTimeoutMS) {
        if (timeoutMS == null) {
            return alternativeTimeoutMS;
        } else if (timeoutMS == 0) {
            return alternativeTimeoutMS;
        } else if (alternativeTimeoutMS == 0) {
            return timeoutRemainingMS();
        } else {
            return Math.min(timeoutRemainingMS(), alternativeTimeoutMS);
        }
    }

    /**
     * Gets the maxAwaitTimeMS
     *
     * <p>
     *  0 is the default unset value. If greater than 0 getMore calls will use this value as the {@code maxTimeMS} value in the command.
     * </p>
     * @return the maxAwaitTimeMS
     */
    public long getMaxAwaitTimeMS() {
        return maxAwaitTimeMS;
    }

    public TimeoutMode getTimeoutMode() {
        return timeoutMode;
    }

    @Nullable
    public Long getTimeoutMS() {
        return timeoutMS;
    }

    public long getMaxTimeMS() {
        return timeoutOrAlternative(maxTimeMS);
    }

    public long getMaxCommitTimeMS() {
        return timeoutOrAlternative(maxCommitTimeMS);
    }

    private void setTimeout() {
        if (timeoutMS != null) {
            if (timeoutMS == 0) {
                this.timeout = Timeout.infinite();
            } else {
                this.timeout = Timeout.startNow(timeoutMS, MILLISECONDS);
            }
        }
    }

    private void resetTimeout() {
        if (timeoutMode.equals(TimeoutMode.ITERATION)) {
            setTimeout();
        }
    }

    private long timeoutRemainingMS() {
        assertNotNull(timeout);
        return timeout.isInfinite() ? 0 : timeout.remaining(MILLISECONDS);
    }

    @Override
    public String toString() {
        return "ClientSideOperationTimeout{"
                + "timeoutMS=" + timeoutMS
                + ", timeoutMode=" + timeoutMode
                + ", maxAwaitTimeMS=" + maxAwaitTimeMS
                + ", maxTimeMS=" + maxTimeMS
                + ", maxCommitTimeMS=" + maxCommitTimeMS
                + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ClientSideOperationTimeout that = (ClientSideOperationTimeout) o;
        return maxAwaitTimeMS == that.maxAwaitTimeMS && maxTimeMS == that.maxTimeMS && maxCommitTimeMS == that.maxCommitTimeMS
                && Objects.equals(timeoutMS, that.timeoutMS);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeoutMS, maxAwaitTimeMS, maxTimeMS, maxCommitTimeMS);
    }
}
