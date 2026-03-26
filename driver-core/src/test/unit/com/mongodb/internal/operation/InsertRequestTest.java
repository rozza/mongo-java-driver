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

import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.WriteRequest;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InsertRequestTest {

    @Test
    void shouldHaveCorrectType() {
        assertEquals(WriteRequest.Type.INSERT, new InsertRequest(new BsonDocument()).getType());
    }

    @Test
    void shouldNotAllowNullDocument() {
        assertThrows(IllegalArgumentException.class, () -> new InsertRequest(null));
    }

    @Test
    void shouldSetFieldsFromConstructor() {
        BsonDocument document = new BsonDocument("_id", new BsonInt32(1));
        InsertRequest insertRequest = new InsertRequest(document);
        assertEquals(document, insertRequest.getDocument());
    }
}
