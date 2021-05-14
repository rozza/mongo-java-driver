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
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.Collection;

import static com.mongodb.ClusterFixture.sleep;
import static com.mongodb.internal.ClientSideOperationTimeouts.NO_TIMEOUT;
import static com.mongodb.internal.ClientSideOperationTimeouts.create;
import static com.mongodb.internal.ClientSideOperationTimeouts.withMaxCommitMS;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

final class ClientSideOperationTimeoutsTest {

    @TestFactory
    Collection<DynamicTest> clientSideOperationTimeoutsTest() {
        return asList(
                dynamicTest("Validates the CursorType and the TimeoutMode", () -> {
                    assertAll(
                            () ->  assertThrows(IllegalStateException.class, () -> create(null, TimeoutMode.CURSOR_LIFETIME,
                                    CursorType.TailableAwait, 0, 0)),
                            () ->  assertThrows(IllegalStateException.class, () -> create(1L, TimeoutMode.CURSOR_LIFETIME,
                                    CursorType.TailableAwait, 0, 0)),
                            () ->  assertThrows(IllegalStateException.class, () -> create(1L, TimeoutMode.ITERATION,
                                    CursorType.TailableAwait, 0, 1L))
                    );
                }),
                dynamicTest("test defaults", () -> {
                    ClientSideOperationTimeout clientSideOperationTimeout = NO_TIMEOUT;
                    assertAll(
                            () -> assertFalse(clientSideOperationTimeout.hasTimeoutMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxTimeMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxAwaitTimeMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxCommitTimeMS()),
                            () -> assertEquals(TimeoutMode.CURSOR_LIFETIME, clientSideOperationTimeout.getTimeoutMode())
                    );
                }),
                dynamicTest("Uses timeoutMS if set", () -> {
                    long altTimeout = 9;
                    ClientSideOperationTimeout clientSideOperationTimeout = create(99999999L, altTimeout, altTimeout, altTimeout);
                    assertAll(
                            () -> assertTrue(clientSideOperationTimeout.hasTimeoutMS()),
                            () -> assertTrue(clientSideOperationTimeout.getMaxTimeMS() > 0),
                            () -> assertTrue(clientSideOperationTimeout.getMaxAwaitTimeMS() > 0),
                            () -> assertTrue(clientSideOperationTimeout.getMaxCommitTimeMS() > 0),
                            () -> assertEquals(TimeoutMode.CURSOR_LIFETIME, clientSideOperationTimeout.getTimeoutMode())
                    );
                }),
                dynamicTest("MaxTimeMS set", () -> {
                    ClientSideOperationTimeout clientSideOperationTimeout = create(null, 9);
                    assertAll(
                            () -> assertFalse(clientSideOperationTimeout.hasTimeoutMS()),
                            () -> assertEquals(9, clientSideOperationTimeout.getMaxTimeMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxAwaitTimeMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxCommitTimeMS()),
                            () -> assertEquals(TimeoutMode.CURSOR_LIFETIME, clientSideOperationTimeout.getTimeoutMode())
                    );
                }),
                dynamicTest("MaxTimeMS and MaxAwaitTimeMS set", () -> {
                    ClientSideOperationTimeout clientSideOperationTimeout = create(null, 9, 99);
                    assertAll(
                            () -> assertFalse(clientSideOperationTimeout.hasTimeoutMS()),
                            () -> assertEquals(9, clientSideOperationTimeout.getMaxTimeMS()),
                            () -> assertEquals(99, clientSideOperationTimeout.getMaxAwaitTimeMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxCommitTimeMS()),
                            () -> assertEquals(TimeoutMode.CURSOR_LIFETIME, clientSideOperationTimeout.getTimeoutMode())
                    );
                }),
                dynamicTest("MaxCommitTimeMS set", () -> {
                    ClientSideOperationTimeout clientSideOperationTimeout = withMaxCommitMS(null, 9L);
                    assertAll(
                            () -> assertFalse(clientSideOperationTimeout.hasTimeoutMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxTimeMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxAwaitTimeMS()),
                            () -> assertEquals(9L, clientSideOperationTimeout.getMaxCommitTimeMS()),
                            () -> assertEquals(TimeoutMode.CURSOR_LIFETIME, clientSideOperationTimeout.getTimeoutMode())
                    );
                }),
                dynamicTest("All deprecated options set", () -> {
                    ClientSideOperationTimeout clientSideOperationTimeout = create(null, 99, 9L, 999);
                    assertAll(
                            () -> assertFalse(clientSideOperationTimeout.hasTimeoutMS()),
                            () -> assertEquals(9, clientSideOperationTimeout.getMaxAwaitTimeMS()),
                            () -> assertEquals(99, clientSideOperationTimeout.getMaxTimeMS()),
                            () -> assertEquals(999, clientSideOperationTimeout.getMaxCommitTimeMS()),
                            () -> assertEquals(TimeoutMode.CURSOR_LIFETIME, clientSideOperationTimeout.getTimeoutMode())
                    );
                }),
                dynamicTest("Use timeout if available or the alternative", () -> assertAll(
                        () -> assertEquals(99L, NO_TIMEOUT.timeoutOrAlternative(99)),
                        () -> assertEquals(0L, ClientSideOperationTimeouts.create(0L).timeoutOrAlternative(99)),
                        () -> assertTrue(ClientSideOperationTimeouts.create(999L).timeoutOrAlternative(0) <= 999),
                        () -> assertTrue(ClientSideOperationTimeouts.create(999L).timeoutOrAlternative(999999) <= 999)
                )),
                dynamicTest("Calculate min works as expected", () -> assertAll(
                        () -> assertEquals(99L, NO_TIMEOUT.calculateMin(99)),
                        () -> assertEquals(99L, ClientSideOperationTimeouts.create(0L).calculateMin(99)),
                        () -> assertTrue(ClientSideOperationTimeouts.create(999L).calculateMin(0) <= 999),
                        () -> assertTrue(ClientSideOperationTimeouts.create(999L).calculateMin(999999) <= 999)
                )),
                dynamicTest("Expired works as expected", () -> {
                    ClientSideOperationTimeout smallTimeout = ClientSideOperationTimeouts.create(1L);
                    ClientSideOperationTimeout longTimeout = ClientSideOperationTimeouts.create(999999999L);
                    sleep(100);
                    assertAll(
                            () -> assertTrue(smallTimeout.expired()),
                            () -> assertFalse(longTimeout.expired()),
                            () -> assertFalse(NO_TIMEOUT.expired())
                    );
                })
        );
    }

    private ClientSideOperationTimeoutsTest() {
    }
}
