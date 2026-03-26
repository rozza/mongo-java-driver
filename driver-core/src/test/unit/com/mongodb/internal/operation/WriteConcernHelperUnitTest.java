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

import com.mongodb.bulk.WriteConcernError;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import static com.mongodb.internal.operation.WriteConcernHelper.createWriteConcernError;
import static org.junit.jupiter.api.Assertions.assertEquals;

class WriteConcernHelperUnitTest {

    @Test
    void shouldCreateWriteConcernError() {
        WriteConcernError writeConcernError = createWriteConcernError(new BsonDocument("code", new BsonInt32(42))
                .append("errmsg", new BsonString("a timeout"))
                .append("errInfo", new BsonDocument("wtimeout", new BsonInt32(1))));

        assertEquals(42, writeConcernError.getCode());
        assertEquals("", writeConcernError.getCodeName());
        assertEquals("a timeout", writeConcernError.getMessage());
        assertEquals(new BsonDocument("wtimeout", new BsonInt32(1)), writeConcernError.getDetails());

        writeConcernError = createWriteConcernError(new BsonDocument("code", new BsonInt32(42))
                .append("codeName", new BsonString("TimeoutError"))
                .append("errmsg", new BsonString("a timeout"))
                .append("errInfo", new BsonDocument("wtimeout", new BsonInt32(1))));

        assertEquals(42, writeConcernError.getCode());
        assertEquals("TimeoutError", writeConcernError.getCodeName());
        assertEquals("a timeout", writeConcernError.getMessage());
        assertEquals(new BsonDocument("wtimeout", new BsonInt32(1)), writeConcernError.getDetails());
    }
}
