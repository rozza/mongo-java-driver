/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client;


import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class PublisherApiTest {

    @TestFactory
    @DisplayName("test that publisher apis matches sync")
    List<DynamicTest> testPublisherApiMatchesSyncApi() {
        return asList(
                dynamicTest("Aggregate Api", () -> assertApis(AggregateIterable.class, AggregatePublisher.class)),
                dynamicTest("Find Api", () -> assertApis(FindIterable.class, FindPublisher.class))
        );
    }

    void assertApis(final Class<?> syncApi, final Class<?> publisherApi) {
        assertIterableEquals(getMethodNames(syncApi),getMethodNames(publisherApi), format("%s != %s", syncApi, publisherApi));
    }

    private static final List<String> SYNC_ONLY_APIS = asList("iterator", "cursor", "map", "into", "spliterator", "forEach");
    private static final List<String> PUBLISHER_ONLY_APIS =  asList("batchCursor", "getBatchSize", "subscribe");

    private List<String> getMethodNames(final Class<?> clazz) {
        return Arrays.stream(clazz.getMethods())
                .map(Method::getName)
                .filter(n -> !SYNC_ONLY_APIS.contains(n) && !PUBLISHER_ONLY_APIS.contains(n))
                .sorted()
                .collect(Collectors.toList());
    }

}
