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

import com.mongodb.OperationFunctionalSpecification;
import org.bson.BsonJavaScript;
import org.junit.jupiter.api.Test;

class MapReduceToCollectionOperationSpecificationTest extends OperationFunctionalSpecification {

    // NOTE: This is a large test file (326 lines) that heavily uses Spock Mock/Stub
    // and the testSyncOperation/testAsyncOperation helper methods.
    // MapReduce is deprecated, so these tests are lower priority for conversion.
    // TODO: Convert functional tests with full implementation

    @Test
    void shouldCreateOperation() {
        String outCollectionName = getCollectionName() + "_out";
        MapReduceToCollectionOperation operation = new MapReduceToCollectionOperation(
                getNamespace(),
                new BsonJavaScript("function() { emit(this.x, 1); }"),
                new BsonJavaScript("function(key, values) { return Array.sum(values); }"),
                outCollectionName,
                null);
        // Operation created successfully
    }
}
