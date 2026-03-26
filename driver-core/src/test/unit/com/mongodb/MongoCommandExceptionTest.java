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

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MongoCommandExceptionTest {

    @Test
    @DisplayName("should extract error message")
    void shouldExtractErrorMessage() {
        assertEquals("the error message",
                new MongoCommandException(
                        new BsonDocument("ok", BsonBoolean.FALSE).append("errmsg", new BsonString("the error message")),
                        new ServerAddress()).getErrorMessage());
        assertEquals("",
                new MongoCommandException(new BsonDocument("ok", BsonBoolean.FALSE),
                        new ServerAddress()).getErrorMessage());
    }

    @Test
    @DisplayName("should extract error code")
    void shouldExtractErrorCode() {
        assertEquals(26,
                new MongoCommandException(
                        new BsonDocument("ok", BsonBoolean.FALSE).append("code", new BsonInt32(26)),
                        new ServerAddress()).getErrorCode());
        assertEquals(-1,
                new MongoCommandException(new BsonDocument("ok", BsonBoolean.FALSE),
                        new ServerAddress()).getErrorCode());
    }

    @Test
    @DisplayName("should extract error code name")
    void shouldExtractErrorCodeName() {
        assertEquals("TimeoutError",
                new MongoCommandException(new BsonDocument("ok", BsonBoolean.FALSE).append("code", new BsonInt32(26))
                        .append("codeName", new BsonString("TimeoutError")), new ServerAddress()).getErrorCodeName());
        assertEquals("",
                new MongoCommandException(new BsonDocument("ok", BsonBoolean.FALSE),
                        new ServerAddress()).getErrorCodeName());
    }

    @Test
    @DisplayName("should create message")
    void shouldCreateMessage() {
        assertEquals("Command execution failed on MongoDB server with error 26 (TimeoutError): 'the error message' "
                        + "on server 127.0.0.1:27017. The full response is {\"ok\": false, \"code\": 26, "
                        + "\"codeName\": \"TimeoutError\", \"errmsg\": \"the error message\"}",
                new MongoCommandException(new BsonDocument("ok", BsonBoolean.FALSE).append("code", new BsonInt32(26))
                        .append("codeName", new BsonString("TimeoutError"))
                        .append("errmsg", new BsonString("the error message")),
                        new ServerAddress()).getMessage());

        assertEquals("Command execution failed on MongoDB server with error 26: 'the error message' "
                        + "on server 127.0.0.1:27017. The full response is {\"ok\": false, \"code\": 26, "
                        + "\"errmsg\": \"the error message\"}",
                new MongoCommandException(new BsonDocument("ok", BsonBoolean.FALSE).append("code", new BsonInt32(26))
                        .append("errmsg", new BsonString("the error message")),
                        new ServerAddress()).getMessage());
    }
}
