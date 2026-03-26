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

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MongoDriverInformationTest {

    @Test
    @DisplayName("should set the correct default values")
    void shouldSetCorrectDefaults() {
        MongoDriverInformation options = MongoDriverInformation.builder().build();

        assertEquals(Collections.emptyList(), options.getDriverNames());
        assertEquals(Collections.emptyList(), options.getDriverVersions());
        assertEquals(Collections.emptyList(), options.getDriverPlatforms());
    }

    @Test
    @DisplayName("should not append data if none has been added")
    void shouldNotAppendDataIfNoneAdded() {
        MongoDriverInformation options = MongoDriverInformation.builder(MongoDriverInformation.builder().build()).build();

        assertEquals(Collections.emptyList(), options.getDriverNames());
        assertEquals(Collections.emptyList(), options.getDriverVersions());
        assertEquals(Collections.emptyList(), options.getDriverPlatforms());
    }

    @Test
    @DisplayName("should append data to the list")
    void shouldAppendDataToList() {
        MongoDriverInformation javaDriverInfo = MongoDriverInformation.builder()
                .driverName("mongo-java-driver")
                .driverVersion("3.4.0")
                .driverPlatform("Java oracle64-1.8.0.31")
                .build();

        MongoDriverInformation options = MongoDriverInformation.builder(javaDriverInfo)
                .driverName("mongo-scala-driver")
                .driverVersion("1.2.0")
                .driverPlatform("Scala 2.11")
                .build();

        assertEquals(Arrays.asList("mongo-java-driver", "mongo-scala-driver"), options.getDriverNames());
        assertEquals(Arrays.asList("3.4.0", "1.2.0"), options.getDriverVersions());
        assertEquals(Arrays.asList("Java oracle64-1.8.0.31", "Scala 2.11"), options.getDriverPlatforms());
    }

    @Test
    @DisplayName("should only append data that has been set")
    void shouldOnlyAppendDataThatHasBeenSet() {
        MongoDriverInformation javaDriverInfo = MongoDriverInformation.builder()
                .driverName("mongo-java-driver")
                .driverVersion("3.4.0")
                .driverPlatform("Java oracle64-1.8.0.31")
                .build();

        MongoDriverInformation options = MongoDriverInformation.builder(javaDriverInfo)
                .driverName("mongo-scala-driver")
                .build();

        assertEquals(Arrays.asList("mongo-java-driver", "mongo-scala-driver"), options.getDriverNames());
        assertEquals(Collections.singletonList("3.4.0"), options.getDriverVersions());
        assertEquals(Collections.singletonList("Java oracle64-1.8.0.31"), options.getDriverPlatforms());
    }

    @Test
    @DisplayName("should null check the passed MongoDriverInformation")
    void shouldNullCheck() {
        assertThrows(IllegalArgumentException.class, () -> MongoDriverInformation.builder(null).build());
    }
}
