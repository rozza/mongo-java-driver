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

import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.client.model.FindOptions;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@SuppressWarnings("deprecation")
final class TimeoutContextHelper {

    private TimeoutContextHelper() {
    }

    static TimeoutContext getTimeoutContext(final TimeoutSettings timeoutSettings) {
        return new TimeoutContext(timeoutSettings);
    }

    static TimeoutContext getTimeoutContext(final TimeoutSettings timeoutSettings, final long maxTimeMS) {
        return getTimeoutContext(timeoutSettings.withMaxTimeMS(maxTimeMS));
    }

    static TimeoutContext getTimeoutContext(final TimeoutSettings timeoutSettings, final long maxTimeMS, final long maxAwaitTimeMS) {
        return getTimeoutContext(timeoutSettings.withMaxTimeAndMaxAwaitTimeMS(maxTimeMS, maxAwaitTimeMS));
    }

    static TimeoutContext getTimeoutContext(final TimeoutSettings timeoutSettings, final CountOptions options) {
        return getTimeoutContext(timeoutSettings, options.getMaxTime(MILLISECONDS));
    }

    static TimeoutContext getTimeoutContext(final TimeoutSettings timeoutSettings, final EstimatedDocumentCountOptions options) {
        return getTimeoutContext(timeoutSettings, options.getMaxTime(MILLISECONDS));
    }

    static TimeoutContext getTimeoutContext(final TimeoutSettings timeoutSettings, final FindOptions options) {
        return getTimeoutContext(timeoutSettings, options.getMaxTime(MILLISECONDS), options.getMaxAwaitTime(MILLISECONDS));
    }

    static TimeoutContext getTimeoutContext(final TimeoutSettings timeoutSettings, final FindOneAndDeleteOptions options) {
        return getTimeoutContext(timeoutSettings, options.getMaxTime(MILLISECONDS));
    }


    static TimeoutContext getTimeoutContext(final TimeoutSettings timeoutSettings, final FindOneAndReplaceOptions options) {
        return getTimeoutContext(timeoutSettings, options.getMaxTime(MILLISECONDS));
    }


    static TimeoutContext getTimeoutContext(final TimeoutSettings timeoutSettings, final FindOneAndUpdateOptions options) {
        return getTimeoutContext(timeoutSettings, options.getMaxTime(MILLISECONDS));
    }

    static TimeoutContext getTimeoutContext(final TimeoutSettings timeoutSettings, final CreateIndexOptions options) {
        return getTimeoutContext(timeoutSettings, options.getMaxTime(MILLISECONDS));
    }

    static TimeoutContext getTimeoutContext(final TimeoutSettings timeoutSettings, final DropIndexOptions options) {
        return getTimeoutContext(timeoutSettings, options.getMaxTime(MILLISECONDS));
    }

}
