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

package com.mongodb;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.mongodb.ErrorCategory.DUPLICATE_KEY;
import static com.mongodb.ErrorCategory.EXECUTION_TIMEOUT;
import static com.mongodb.ErrorCategory.UNCATEGORIZED;
import static com.mongodb.ErrorCategory.fromErrorCode;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ErrorCategoryTest {

    @Test
    @DisplayName("should categorize duplicate key errors")
    void shouldCategorizeDuplicateKeyErrors() {
        assertEquals(DUPLICATE_KEY, fromErrorCode(11000));
        assertEquals(DUPLICATE_KEY, fromErrorCode(11001));
        assertEquals(DUPLICATE_KEY, fromErrorCode(12582));
    }

    @Test
    @DisplayName("should categorize execution timeout errors")
    void shouldCategorizeExecutionTimeoutErrors() {
        assertEquals(EXECUTION_TIMEOUT, fromErrorCode(50));
    }

    @Test
    @DisplayName("should categorize uncategorized errors")
    void shouldCategorizeUncategorizedErrors() {
        assertEquals(UNCATEGORIZED, fromErrorCode(0));
    }
}
