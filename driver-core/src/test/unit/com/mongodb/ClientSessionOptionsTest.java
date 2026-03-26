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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClientSessionOptionsTest {

    @Test
    @DisplayName("should have correct defaults")
    void shouldHaveCorrectDefaults() {
        ClientSessionOptions options = ClientSessionOptions.builder().build();

        assertNull(options.isCausallyConsistent());
        assertEquals(TransactionOptions.builder().build(), options.getDefaultTransactionOptions());
    }

    @ParameterizedTest
    @MethodSource("applyOptionsData")
    @DisplayName("should apply options set in builder")
    void shouldApplyOptionsSetInBuilder(final boolean causallyConsistent, final TransactionOptions transactionOptions) {
        ClientSessionOptions options = ClientSessionOptions.builder()
                .causallyConsistent(causallyConsistent)
                .defaultTransactionOptions(transactionOptions)
                .build();

        assertEquals(causallyConsistent, options.isCausallyConsistent());
        assertEquals(transactionOptions, options.getDefaultTransactionOptions());
    }

    static Stream<Object[]> applyOptionsData() {
        return Stream.of(
                new Object[]{true, TransactionOptions.builder().build()},
                new Object[]{false, TransactionOptions.builder().readConcern(ReadConcern.LOCAL).build()}
        );
    }

    @Test
    @DisplayName("should throw an exception if the defaultTimeout is set and negative")
    void shouldThrowExceptionForInvalidTimeout() {
        ClientSessionOptions.Builder builder = ClientSessionOptions.builder();

        assertThrows(IllegalArgumentException.class, () -> builder.defaultTimeout(500, TimeUnit.NANOSECONDS));
        assertThrows(IllegalArgumentException.class, () -> builder.defaultTimeout(-1, TimeUnit.SECONDS));
    }

    @ParameterizedTest
    @MethodSource("applyOptionsToBuilderData")
    @DisplayName("should apply options to builder")
    void shouldApplyOptionsToBuilder(final ClientSessionOptions baseOptions) {
        assertEquals(baseOptions, ClientSessionOptions.builder(baseOptions).build());
    }

    static Stream<ClientSessionOptions> applyOptionsToBuilderData() {
        return Stream.of(
                ClientSessionOptions.builder().build(),
                ClientSessionOptions.builder()
                        .causallyConsistent(true)
                        .defaultTransactionOptions(TransactionOptions.builder()
                                .writeConcern(WriteConcern.MAJORITY)
                                .readConcern(ReadConcern.MAJORITY).build())
                        .build()
        );
    }
}
