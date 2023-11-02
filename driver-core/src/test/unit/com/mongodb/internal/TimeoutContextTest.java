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

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.Collection;

import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_MAX_AWAIT_TIME;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_MAX_COMMIT;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_MAX_TIME;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_MAX_TIME_AND_AWAIT_TIME;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_TIMEOUT;
import static com.mongodb.ClusterFixture.sleep;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

final class TimeoutContextTest {

    @TestFactory
    Collection<DynamicTest> timeoutContextTest() {
        return asList(
                dynamicTest("test defaults", () -> {
                    TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS);
                    assertAll(
                            () -> assertFalse(timeoutContext.hasTimeoutMS()),
                            () -> assertEquals(0, timeoutContext.getMaxTimeMS()),
                            () -> assertEquals(0, timeoutContext.getMaxAwaitTimeMS()),
                            () -> assertEquals(0, timeoutContext.getMaxCommitTimeMS())
                    );
                }),
                dynamicTest("Uses timeoutMS if set", () -> {
                    TimeoutContext timeoutContext =
                            new TimeoutContext(TIMEOUT_SETTINGS_WITH_TIMEOUT.withMaxAwaitTimeMS(9));
                    assertAll(
                            () -> assertTrue(timeoutContext.hasTimeoutMS()),
                            () -> assertTrue(timeoutContext.getMaxTimeMS() > 0),
                            () -> assertTrue(timeoutContext.getMaxAwaitTimeMS() > 0),
                            () -> assertTrue(timeoutContext.getMaxCommitTimeMS() > 0)
                    );
                }),
                dynamicTest("MaxTimeMS set", () -> {
                    TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_MAX_TIME);
                    assertAll(
                            () -> assertEquals(100, timeoutContext.getMaxTimeMS()),
                            () -> assertEquals(0, timeoutContext.getMaxAwaitTimeMS()),
                            () -> assertEquals(0, timeoutContext.getMaxCommitTimeMS())
                    );
                }),
                dynamicTest("MaxAwaitTimeMS set", () -> {
                    TimeoutContext timeoutContext =
                            new TimeoutContext(TIMEOUT_SETTINGS_WITH_MAX_AWAIT_TIME);
                    assertAll(
                            () -> assertEquals(0, timeoutContext.getMaxTimeMS()),
                            () -> assertEquals(101, timeoutContext.getMaxAwaitTimeMS()),
                            () -> assertEquals(0, timeoutContext.getMaxCommitTimeMS())
                    );
                }),
                dynamicTest("MaxTimeMS and MaxAwaitTimeMS set", () -> {
                    TimeoutContext timeoutContext =
                            new TimeoutContext(TIMEOUT_SETTINGS_WITH_MAX_TIME_AND_AWAIT_TIME);
                    assertAll(
                            () -> assertEquals(101, timeoutContext.getMaxTimeMS()),
                            () -> assertEquals(1001, timeoutContext.getMaxAwaitTimeMS()),
                            () -> assertEquals(0, timeoutContext.getMaxCommitTimeMS())
                    );
                }),
                dynamicTest("MaxCommitTimeMS set", () -> {
                    TimeoutContext timeoutContext =
                            new TimeoutContext(TIMEOUT_SETTINGS_WITH_MAX_COMMIT);
                    assertAll(
                            () -> assertEquals(0, timeoutContext.getMaxTimeMS()),
                            () -> assertEquals(0, timeoutContext.getMaxAwaitTimeMS()),
                            () -> assertEquals(999L, timeoutContext.getMaxCommitTimeMS())
                    );
                }),
                dynamicTest("All deprecated options set", () -> {
                    TimeoutContext timeoutContext =
                            new TimeoutContext(TIMEOUT_SETTINGS_WITH_MAX_TIME_AND_AWAIT_TIME
                                    .withMaxCommitMS(999L));
                    assertAll(
                            () -> assertEquals(101, timeoutContext.getMaxTimeMS()),
                            () -> assertEquals(1001, timeoutContext.getMaxAwaitTimeMS()),
                            () -> assertEquals(999, timeoutContext.getMaxCommitTimeMS())
                    );
                }),
                dynamicTest("Use timeout if available or the alternative", () -> assertAll(
                        () -> {
                            TimeoutContext timeoutContext =
                                    new TimeoutContext(TIMEOUT_SETTINGS);
                            assertEquals(99L, timeoutContext.timeoutOrAlternative(99));
                        },
                        () -> {
                            TimeoutContext timeoutContext =
                                    new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(0));
                            assertEquals(0L, timeoutContext.timeoutOrAlternative(99));
                        },
                        () -> {
                            TimeoutContext timeoutContext =
                                    new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(999));
                            assertTrue(timeoutContext.timeoutOrAlternative(0) <= 999);
                        },
                        () -> {
                            TimeoutContext timeoutContext =
                                    new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(999));
                            assertTrue(timeoutContext.timeoutOrAlternative(999999) <= 999);
                        }
                )),
                dynamicTest("Calculate min works as expected", () -> assertAll(
                        () -> {
                            TimeoutContext timeoutContext =
                                    new TimeoutContext(TIMEOUT_SETTINGS);
                            assertEquals(99L, timeoutContext.calculateMin(99));
                        },
                        () -> {
                            TimeoutContext timeoutContext =
                                    new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(0));
                            assertEquals(99L, timeoutContext.calculateMin(99));
                        },
                        () -> {
                            TimeoutContext timeoutContext =
                                    new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(999));
                            assertTrue(timeoutContext.calculateMin(0) <= 999);
                        },
                        () -> {
                            TimeoutContext timeoutContext =
                                    new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(999));
                            assertTrue(timeoutContext.calculateMin(999999) <= 999);
                        }
                )),
                dynamicTest("Expired works as expected", () -> {
                    TimeoutContext smallTimeout = new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(1));
                    TimeoutContext longTimeout =
                            new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(9999999));
                    TimeoutContext noTimeout = new TimeoutContext(TIMEOUT_SETTINGS);
                    sleep(100);
                    assertAll(
                            () -> assertFalse(noTimeout.hasExpired()),
                            () -> assertFalse(longTimeout.hasExpired()),
                            () -> assertTrue(smallTimeout.hasExpired())
                    );
                })
        );
    }

    private TimeoutContextTest() {
    }
}
