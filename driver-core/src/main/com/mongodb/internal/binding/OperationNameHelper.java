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

package com.mongodb.internal.binding;


import com.mongodb.internal.operation.ClientBulkWriteOperation;
import com.mongodb.internal.operation.MixedBulkWriteOperation;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.WriteOperation;

import java.util.Set;
import java.util.stream.Collectors;

// TODO Location
public class OperationNameHelper  {

    public static String getReadOperationName(final ReadOperation<?> readOperation) {
        return readOperation.getClass().getSimpleName();
    }

    public static String getWriteOperationName(final WriteOperation<?> writeOperation) {
        if (writeOperation instanceof ClientBulkWriteOperation) {
            return "bulkWrite";
        } else if(writeOperation instanceof MixedBulkWriteOperation) {
            MixedBulkWriteOperation mixedBulkWriteOperation = (MixedBulkWriteOperation) writeOperation;

            Set<String> writeRequests = mixedBulkWriteOperation.getWriteRequests().stream().map(w -> w.getClass().getSimpleName())
                    .collect(Collectors.toSet());


            return "bulkWrite";
        }
        return writeOperation.getClass().getSimpleName();
    }

    private OperationNameHelper() {
    }
}
