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
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static com.mongodb.client.model.Updates.bitwiseAnd;
import static com.mongodb.client.model.Updates.bitwiseOr;
import static com.mongodb.client.model.Updates.bitwiseXor;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BitwiseUpdatesFunctionalSpecificationTest extends OperationFunctionalSpecification {
    private static final long LONG_MASK = 0x0fffffffffffffffL;
    private static final int INT_MASK = 0x0fffffff;
    private static final int NUM = 13;

    private final Document a = new Document("_id", 1).append("x", NUM);

    @BeforeEach
    public void setUp() {
        super.setUp();
        getCollectionHelper().insertDocuments(a);
    }

    private List<Document> find() {
        return getCollectionHelper().find(new Document("_id", 1));
    }

    private void updateOne(final Bson update) {
        getCollectionHelper().updateOne(new Document("_id", 1), update);
    }

    @Test
    void integerBitwiseAnd() {
        updateOne(bitwiseAnd("x", INT_MASK));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", NUM & INT_MASK)), find());
    }

    @Test
    void integerBitwiseOr() {
        updateOne(bitwiseOr("x", INT_MASK));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", NUM | INT_MASK)), find());
    }

    @Test
    void integerBitwiseXor() {
        updateOne(bitwiseXor("x", INT_MASK));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", NUM ^ INT_MASK)), find());
    }

    @Test
    void longBitwiseAnd() {
        updateOne(bitwiseAnd("x", LONG_MASK));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", NUM & LONG_MASK)), find());
    }

    @Test
    void longBitwiseOr() {
        updateOne(bitwiseOr("x", LONG_MASK));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", NUM | LONG_MASK)), find());
    }

    @Test
    void longBitwiseXor() {
        updateOne(bitwiseXor("x", LONG_MASK));
        assertEquals(Collections.singletonList(new Document("_id", 1).append("x", NUM ^ LONG_MASK)), find());
    }
}
