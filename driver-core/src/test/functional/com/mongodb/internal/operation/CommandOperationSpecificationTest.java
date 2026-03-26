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
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommandOperationSpecificationTest extends OperationFunctionalSpecification {

    @Test
    void shouldExecuteReadCommandSync() {
        CommandReadOperation<BsonDocument> operation = new CommandReadOperation<>(
                getNamespace().getDatabaseName(),
                new BsonDocument("count", new BsonString(getCollectionName())),
                new BsonDocumentCodec());

        BsonDocument result = execute(operation, false);
        assertEquals(0, result.getNumber("n").intValue());
    }

    @Test
    void shouldExecuteReadCommandAsync() {
        CommandReadOperation<BsonDocument> operation = new CommandReadOperation<>(
                getNamespace().getDatabaseName(),
                new BsonDocument("count", new BsonString(getCollectionName())),
                new BsonDocumentCodec());

        BsonDocument result = execute(operation, true);
        assertEquals(0, result.getNumber("n").intValue());
    }

    @Tag("Slow")
    @Test
    void shouldExecuteCommandLargerThan16MBSync() {
        CommandReadOperation<BsonDocument> operation = new CommandReadOperation<>(
                getNamespace().getDatabaseName(),
                new BsonDocument("findAndModify", new BsonString(getNamespace().getFullName()))
                        .append("query", new BsonDocument("_id", new BsonInt32(42)))
                        .append("update",
                                new BsonDocument("_id", new BsonInt32(42))
                                        .append("b", new BsonBinary(new byte[16 * 1024 * 1024 - 30]))),
                new BsonDocumentCodec());

        BsonDocument result = execute(operation, false);
        assertTrue(result.containsKey("value"));
    }

    @Tag("Slow")
    @Test
    void shouldExecuteCommandLargerThan16MBAsync() {
        CommandReadOperation<BsonDocument> operation = new CommandReadOperation<>(
                getNamespace().getDatabaseName(),
                new BsonDocument("findAndModify", new BsonString(getNamespace().getFullName()))
                        .append("query", new BsonDocument("_id", new BsonInt32(42)))
                        .append("update",
                                new BsonDocument("_id", new BsonInt32(42))
                                        .append("b", new BsonBinary(new byte[16 * 1024 * 1024 - 30]))),
                new BsonDocumentCodec());

        BsonDocument result = execute(operation, true);
        assertTrue(result.containsKey("value"));
    }
}
