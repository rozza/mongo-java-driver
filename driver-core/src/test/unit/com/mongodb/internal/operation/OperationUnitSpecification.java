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

import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class providing test infrastructure for operation unit tests.
 * Preserves the static utility methods from the original Groovy OperationUnitSpecification.
 */
class OperationUnitSpecification {

    // Have to add to this map for every server release
    private static final Map<List<Integer>, Integer> SERVER_TO_WIRE_VERSION_MAP = new HashMap<>();
    static {
        SERVER_TO_WIRE_VERSION_MAP.put(Arrays.asList(2, 6), 2);
        SERVER_TO_WIRE_VERSION_MAP.put(Arrays.asList(3, 0), 3);
        SERVER_TO_WIRE_VERSION_MAP.put(Arrays.asList(3, 2), 4);
        SERVER_TO_WIRE_VERSION_MAP.put(Arrays.asList(3, 4), 5);
        SERVER_TO_WIRE_VERSION_MAP.put(Arrays.asList(3, 6), 6);
        SERVER_TO_WIRE_VERSION_MAP.put(Arrays.asList(4, 0), 7);
        SERVER_TO_WIRE_VERSION_MAP.put(Arrays.asList(4, 1), 8);
        SERVER_TO_WIRE_VERSION_MAP.put(Arrays.asList(4, 2), 8);
        SERVER_TO_WIRE_VERSION_MAP.put(Arrays.asList(4, 4), 9);
        SERVER_TO_WIRE_VERSION_MAP.put(Arrays.asList(5, 0), 13);
        SERVER_TO_WIRE_VERSION_MAP.put(Arrays.asList(5, 1), 14);
        SERVER_TO_WIRE_VERSION_MAP.put(Arrays.asList(5, 2), 15);
        SERVER_TO_WIRE_VERSION_MAP.put(Arrays.asList(5, 3), 16);
        SERVER_TO_WIRE_VERSION_MAP.put(Arrays.asList(6, 0), 17);
        SERVER_TO_WIRE_VERSION_MAP.put(Arrays.asList(6, 1), 18);
        SERVER_TO_WIRE_VERSION_MAP.put(Arrays.asList(6, 2), 19);
        SERVER_TO_WIRE_VERSION_MAP.put(Arrays.asList(6, 3), 20);
        SERVER_TO_WIRE_VERSION_MAP.put(Arrays.asList(7, 0), 21);
        SERVER_TO_WIRE_VERSION_MAP.put(Arrays.asList(9, 0), 25);
    }

    static int getMaxWireVersionForServerVersion(final List<Integer> serverVersion) {
        List<Integer> key = serverVersion.subList(0, 2);
        Integer maxWireVersion = SERVER_TO_WIRE_VERSION_MAP.get(key);
        if (maxWireVersion == null) {
            throw new IllegalArgumentException("Unknown server version " + key
                    + ".  Check if it has been added to SERVER_TO_WIRE_VERSION_MAP");
        }
        return maxWireVersion;
    }

    static final Collation DEFAULT_COLLATION = Collation.builder()
            .locale("en")
            .caseLevel(true)
            .collationCaseFirst(CollationCaseFirst.OFF)
            .collationStrength(CollationStrength.IDENTICAL)
            .numericOrdering(true)
            .collationAlternate(CollationAlternate.SHIFTED)
            .collationMaxVariable(CollationMaxVariable.SPACE)
            .normalization(true)
            .backwards(true)
            .build();

    static final Collation CASE_INSENSITIVE_COLLATION = Collation.builder()
            .locale("en")
            .collationStrength(CollationStrength.SECONDARY)
            .build();
}
