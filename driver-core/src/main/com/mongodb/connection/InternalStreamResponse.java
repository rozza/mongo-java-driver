/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.connection;

final class InternalStreamResponse {
    private final ResponseBuffers result;
    private final Throwable t;

    public InternalStreamResponse(final ResponseBuffers result, final Throwable t) {
        this.result = result;
        this.t = t;
    }

    public ResponseBuffers getResult() {
        return result;
    }

    public Throwable getError() {
        return t;
    }

    public boolean hasError() {
        return t != null;
    }
}
