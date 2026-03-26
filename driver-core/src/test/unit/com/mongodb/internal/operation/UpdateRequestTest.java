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
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.bulk.WriteRequest;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UpdateRequestTest {

    @Test
    void shouldHaveCorrectType() {
        assertEquals(WriteRequest.Type.UPDATE,
                new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.UPDATE).getType());
        assertEquals(WriteRequest.Type.REPLACE,
                new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.REPLACE).getType());
    }

    @Test
    void shouldThrowIfTypeIsNotUpdateOrReplace() {
        assertThrows(IllegalArgumentException.class, () ->
                new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.INSERT));
        assertThrows(IllegalArgumentException.class, () ->
                new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.DELETE));
    }

    @Test
    void shouldNotAllowNullFilter() {
        assertThrows(IllegalArgumentException.class, () ->
                new UpdateRequest(null, new BsonDocument(), WriteRequest.Type.UPDATE));
    }

    @Test
    void shouldNotAllowNullUpdate() {
        assertThrows(IllegalArgumentException.class, () ->
                new UpdateRequest(new BsonDocument(), null, WriteRequest.Type.UPDATE));
    }

    @Test
    void shouldSetFieldsFromConstructor() {
        BsonDocument filter = new BsonDocument("_id", new BsonInt32(1));
        BsonDocument update = new BsonDocument("$set", new BsonDocument("x", BsonBoolean.TRUE));
        UpdateRequest updateRequest = new UpdateRequest(filter, update, WriteRequest.Type.UPDATE);
        assertEquals(filter, updateRequest.getFilter());
        assertEquals(update, updateRequest.getUpdateValue());
    }

    @Test
    void multiPropertyShouldDefaultCorrectly() {
        assertTrue(new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.UPDATE).isMulti());
        assertFalse(new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.REPLACE).isMulti());
    }

    @Test
    void shouldSetMultiProperty() {
        assertFalse(new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.UPDATE).multi(false).isMulti());
        assertFalse(new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.REPLACE).multi(false).isMulti());
    }

    @Test
    void shouldThrowIfMultiSetToTrueOnReplace() {
        assertThrows(IllegalArgumentException.class, () ->
                new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.REPLACE).multi(true));
    }

    @Test
    void upsertPropertyShouldDefaultToFalse() {
        assertFalse(new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.UPDATE).isUpsert());
    }

    @Test
    void shouldSetUpsertProperty() {
        assertTrue(new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.UPDATE).upsert(true).isUpsert());
    }

    @Test
    void shouldSetCollationPropertyForUpdate() {
        Collation collation = Collation.builder().locale("en").build();
        assertNull(new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.UPDATE)
                .collation(null).getCollation());
        assertEquals(collation, new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.UPDATE)
                .collation(collation).getCollation());
    }

    @Test
    void shouldSetCollationPropertyForReplace() {
        Collation collation = Collation.builder().locale("en").build();
        assertNull(new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.REPLACE)
                .collation(null).getCollation());
        assertEquals(collation, new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.REPLACE)
                .collation(collation).getCollation());
    }

    @Test
    void shouldSetArrayFiltersProperty() {
        assertNull(new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.UPDATE)
                .arrayFilters(null).getArrayFilters());
        assertEquals(Collections.emptyList(), new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.UPDATE)
                .arrayFilters(Collections.emptyList()).getArrayFilters());
        List<BsonDocument> arrayFilters = Arrays.asList(new BsonDocument("a.b", new BsonInt32(42)));
        assertEquals(arrayFilters, new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.UPDATE)
                .arrayFilters(arrayFilters).getArrayFilters());
    }

    @Test
    void shouldSetSortProperty() {
        assertNull(new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.UPDATE)
                .sort(null).getSort());
        BsonDocument sort = new BsonDocument("_id", new BsonInt32(1));
        assertEquals(sort, new UpdateRequest(new BsonDocument(), new BsonDocument(), WriteRequest.Type.REPLACE)
                .sort(sort).getSort());
    }
}
