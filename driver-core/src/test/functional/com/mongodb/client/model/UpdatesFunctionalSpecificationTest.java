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

package com.mongodb.client.model;

import com.mongodb.OperationFunctionalSpecification;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Updates.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UpdatesFunctionalSpecificationTest extends OperationFunctionalSpecification {
    private final Document a = new Document("_id", 1).append("x", 1);

    @BeforeEach
    public void setUp() {
        super.setUp();
        getCollectionHelper().insertDocuments(a);
    }

    private List<Document> find() {
        return find(new Document("_id", 1));
    }

    private List<Document> find(final Bson filter) {
        return getCollectionHelper().find(filter);
    }

    private void updateOne(final Bson update) {
        getCollectionHelper().updateOne(new Document("_id", 1), update);
    }

    private void updateOne(final Bson filter, final Bson update, final boolean isUpsert) {
        getCollectionHelper().updateOne(filter, update, isUpsert);
    }

    @Test
    void setTest() {
        updateOne(set("x", 5));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", 5)), find());
    }

    @Test
    void setOnInsertTest() {
        updateOne(setOnInsert("y", 5));
        assertEquals(Collections.singletonList(a), find());

        updateOne(new Document("_id", 2), setOnInsert("y", 5), true);
        assertEquals(Collections.singletonList(new Document("_id", 2).append("y", 5)),
                find(new Document("_id", 2)));

        updateOne(new Document("_id", 3), setOnInsert(Document.parse("{a: 1, b: \"two\"}")), true);
        assertEquals(Collections.singletonList(Document.parse("{_id: 3, a: 1, b: \"two\"}")),
                find(new Document("_id", 3)));

        assertThrows(IllegalArgumentException.class, () -> updateOne(new Document("_id", 3), setOnInsert(null), true));
    }

    @Test
    void unsetTest() {
        updateOne(unset("x"));
        assertEquals(Collections.singletonList(new Document("_id", 1)), find());
    }

    @Test
    void renameTest() {
        updateOne(rename("x", "y"));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("y", 1)), find());
    }

    @Test
    void incTest() {
        updateOne(inc("x", 5));
        assertEquals(new Document("_id", 1).append("x", 6), find().get(0));

        updateOne(inc("x", 5L));
        assertEquals(new Document("_id", 1).append("x", 11L), find().get(0));

        updateOne(inc("x", 3.4d));
        assertEquals(new Document("_id", 1).append("x", 14.4), find().get(0));
    }

    @Test
    void mulTest() {
        updateOne(mul("x", 5));
        assertEquals(new Document("_id", 1).append("x", 5), find().get(0));

        updateOne(mul("x", 5L));
        assertEquals(new Document("_id", 1).append("x", 25L), find().get(0));

        updateOne(mul("x", 3.5d));
        assertEquals(new Document("_id", 1).append("x", 87.5), find().get(0));
    }

    @Test
    void minTest() {
        updateOne(min("x", -1));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", -1)), find());
    }

    @Test
    void maxTest() {
        updateOne(max("x", 5));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", 5)), find());
    }

    @Test
    void currentDateTest() {
        updateOne(currentDate("y"));
        assertTrue(find().get(0).get("y") instanceof Date);

        updateOne(currentTimestamp("z"));
        assertTrue(find().get(0).get("z") instanceof BsonTimestamp);
    }

    @Test
    void combineSingleOperatorTest() {
        updateOne(combine(set("x", 5), set("y", 6)));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", 5).append("y", 6)), find());
    }

    @Test
    void combineMultipleOperatorsTest() {
        updateOne(combine(set("a", 5), set("b", 6), inc("x", 3), inc("y", 5)));
        assertEquals(Collections.singletonList(
                new Document("_id", 1).append("a", 5).append("b", 6).append("x", 4).append("y", 5)), find());
    }
}
