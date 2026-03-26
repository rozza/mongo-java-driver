/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TransactionOptionsTest {

    @Test
    @DisplayName("should have correct defaults")
    void shouldHaveCorrectDefaults() {
        TransactionOptions options = TransactionOptions.builder().build();

        assertNull(options.getReadConcern());
        assertNull(options.getWriteConcern());
        assertNull(options.getReadPreference());
        assertNull(options.getMaxCommitTime(TimeUnit.MILLISECONDS));
    }

    @Test
    @DisplayName("should throw an exception if the timeout is invalid")
    void shouldThrowForInvalidTimeout() {
        TransactionOptions.Builder builder = TransactionOptions.builder();

        assertThrows(IllegalArgumentException.class, () -> builder.timeout(500L, TimeUnit.NANOSECONDS));
        assertThrows(IllegalArgumentException.class, () -> builder.timeout(-1L, TimeUnit.SECONDS).build());
    }

    @Test
    @DisplayName("should apply options set in builder")
    void shouldApplyOptionsSetInBuilder() {
        TransactionOptions options = TransactionOptions.builder()
                .readConcern(ReadConcern.LOCAL)
                .writeConcern(WriteConcern.JOURNALED)
                .readPreference(ReadPreference.secondary())
                .maxCommitTime(5L, TimeUnit.SECONDS)
                .timeout(null, TimeUnit.MILLISECONDS)
                .build();

        assertEquals(ReadConcern.LOCAL, options.getReadConcern());
        assertEquals(WriteConcern.JOURNALED, options.getWriteConcern());
        assertEquals(ReadPreference.secondary(), options.getReadPreference());
        assertEquals(Long.valueOf(5000), options.getMaxCommitTime(TimeUnit.MILLISECONDS));
        assertEquals(Long.valueOf(5), options.getMaxCommitTime(TimeUnit.SECONDS));
        assertNull(options.getTimeout(TimeUnit.MILLISECONDS));
    }

    @Test
    @DisplayName("should merge")
    void shouldMerge() {
        TransactionOptions first = TransactionOptions.builder().build();
        TransactionOptions second = TransactionOptions.builder()
                .readConcern(ReadConcern.MAJORITY)
                .writeConcern(WriteConcern.MAJORITY)
                .readPreference(ReadPreference.secondary())
                .maxCommitTime(5L, TimeUnit.SECONDS)
                .timeout(123L, TimeUnit.MILLISECONDS)
                .build();
        TransactionOptions third = TransactionOptions.builder()
                .readConcern(ReadConcern.LOCAL)
                .writeConcern(WriteConcern.W2)
                .readPreference(ReadPreference.nearest())
                .maxCommitTime(10L, TimeUnit.SECONDS)
                .timeout(123L, TimeUnit.MILLISECONDS)
                .build();

        assertEquals(second, TransactionOptions.merge(first, second));
        assertEquals(second, TransactionOptions.merge(second, first));
        assertEquals(second, TransactionOptions.merge(second, third));
        assertEquals(third, TransactionOptions.merge(third, second));
    }
}
