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

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import static com.mongodb.internal.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.internal.operation.CommandOperationHelper.rethrowIfNotNamespaceError;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommandOperationHelperTest {

    @Test
    void shouldBeNamespaceErrorIfErrorCodeIs26() {
        assertTrue(isNamespaceError(new MongoCommandException(new BsonDocument("ok", BsonBoolean.FALSE)
                .append("code", new BsonInt32(26)),
                new ServerAddress())));
    }

    @Test
    void shouldBeNamespaceErrorIfMessageContainsNsNotFound() {
        assertTrue(isNamespaceError(new MongoCommandException(new BsonDocument("ok", BsonBoolean.FALSE)
                .append("errmsg", new BsonString("the ns not found here")),
                new ServerAddress())));
    }

    @Test
    void shouldNotBeNamespaceErrorIfMessageDoesNotContainNsNotFound() {
        assertFalse(isNamespaceError(new MongoCommandException(new BsonDocument("ok", BsonBoolean.FALSE)
                .append("errmsg", new BsonString("some other error")),
                new ServerAddress())));
    }

    @Test
    void shouldNotBeNamespaceErrorIfNotMongoCommandException() {
        assertFalse(isNamespaceError(new NullPointerException()));
    }

    @Test
    void shouldRethrowIfNotNamespaceError() {
        assertThrows(MongoCommandException.class, () ->
                rethrowIfNotNamespaceError(new MongoCommandException(new BsonDocument("ok", BsonBoolean.FALSE)
                        .append("errmsg", new BsonString("some other error")),
                        new ServerAddress())));

        assertThrows(MongoCommandException.class, () ->
                rethrowIfNotNamespaceError(new MongoCommandException(new BsonDocument("ok", BsonBoolean.FALSE)
                        .append("errmsg", new BsonString("some other error")),
                        new ServerAddress()), "some value"));
    }

    @Test
    void shouldNotRethrowIfNamespaceError() {
        rethrowIfNotNamespaceError(new MongoCommandException(new BsonDocument("ok", BsonBoolean.FALSE)
                .append("code", new BsonInt32(26)),
                new ServerAddress()));
    }

    @Test
    void shouldReturnDefaultValueIfNamespaceError() {
        assertEquals("some value",
                rethrowIfNotNamespaceError(new MongoCommandException(new BsonDocument("ok", BsonBoolean.FALSE)
                        .append("code", new BsonInt32(26)),
                        new ServerAddress()), "some value"));
    }
}
