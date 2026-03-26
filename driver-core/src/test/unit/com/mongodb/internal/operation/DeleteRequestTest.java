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
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.WriteRequest;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeleteRequestTest {

    @Test
    void shouldHaveCorrectType() {
        assertEquals(WriteRequest.Type.DELETE, new DeleteRequest(new BsonDocument()).getType());
    }

    @Test
    void shouldNotAllowNullFilter() {
        assertThrows(IllegalArgumentException.class, () -> new DeleteRequest(null));
    }

    @Test
    void shouldSetFieldsFromConstructor() {
        BsonDocument filter = new BsonDocument("_id", new BsonInt32(1));
        DeleteRequest removeRequest = new DeleteRequest(filter);
        assertEquals(filter, removeRequest.getFilter());
    }

    @Test
    void multiPropertyShouldDefaultToTrue() {
        assertTrue(new DeleteRequest(new BsonDocument()).isMulti());
    }

    @Test
    void shouldSetMultiProperty() {
        assertFalse(new DeleteRequest(new BsonDocument()).multi(false).isMulti());
    }

    @Test
    void shouldSetCollationProperty() {
        Collation collation = Collation.builder().locale("en").build();
        assertNull(new DeleteRequest(new BsonDocument()).collation(null).getCollation());
        assertEquals(collation, new DeleteRequest(new BsonDocument()).collation(collation).getCollation());
    }
}
