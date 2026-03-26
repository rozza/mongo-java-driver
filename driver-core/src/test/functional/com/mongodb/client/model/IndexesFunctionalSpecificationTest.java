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
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Indexes.*;
import static org.bson.BsonDocument.parse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexesFunctionalSpecificationTest extends OperationFunctionalSpecification {

    private List<BsonDocument> getIndexKeys() {
        return getCollectionHelper().listIndexes().stream()
                .map(doc -> (BsonDocument) doc.get("key"))
                .collect(Collectors.toList());
    }

    @Test
    void ascendingTest() {
        getCollectionHelper().createIndex(ascending("x"));
        assertTrue(getIndexKeys().contains(parse("{x : 1}")));

        getCollectionHelper().createIndex(ascending("x", "y"));
        assertTrue(getIndexKeys().contains(parse("{x : 1, y: 1}")));

        getCollectionHelper().createIndex(ascending(Arrays.asList("a", "b")));
        assertTrue(getIndexKeys().contains(parse("{a : 1, b: 1}")));
    }

    @Test
    void descendingTest() {
        getCollectionHelper().createIndex(descending("x"));
        assertTrue(getIndexKeys().contains(parse("{x : -1}")));

        getCollectionHelper().createIndex(descending("x", "y"));
        assertTrue(getIndexKeys().contains(parse("{x : -1, y: -1}")));

        getCollectionHelper().createIndex(descending(Arrays.asList("a", "b")));
        assertTrue(getIndexKeys().contains(parse("{a : -1, b: -1}")));
    }

    @Test
    void geo2dsphereTest() {
        getCollectionHelper().createIndex(geo2dsphere("x"));
        assertTrue(getIndexKeys().contains(parse("{x : \"2dsphere\"}")));

        getCollectionHelper().createIndex(geo2dsphere("x", "y"));
        assertTrue(getIndexKeys().contains(parse("{x : \"2dsphere\", y: \"2dsphere\"}")));

        getCollectionHelper().createIndex(geo2dsphere(Arrays.asList("a", "b")));
        assertTrue(getIndexKeys().contains(parse("{a : \"2dsphere\", b: \"2dsphere\"}")));
    }

    @Test
    void geo2dTest() {
        getCollectionHelper().createIndex(geo2d("x"));
        assertTrue(getIndexKeys().contains(parse("{x : \"2d\"}")));
    }

    @Test
    void textHelperTest() {
        getCollectionHelper().createIndex(text("x"));
        assertTrue(getIndexKeys().contains(parse("{_fts: \"text\", _ftsx: 1}")));
    }

    @Test
    void textWildcardTest() {
        getCollectionHelper().createIndex(text());
        assertTrue(getIndexKeys().contains(parse("{_fts: \"text\", _ftsx: 1}")));
    }

    @Test
    void hashedTest() {
        getCollectionHelper().createIndex(hashed("x"));
        assertTrue(getIndexKeys().contains(parse("{x : \"hashed\"}")));
    }

    @Test
    void compoundIndexTest() {
        getCollectionHelper().createIndex(compoundIndex(ascending("a"), descending("b")));
        assertTrue(getIndexKeys().contains(parse("{a : 1, b : -1}")));

        getCollectionHelper().createIndex(compoundIndex(Arrays.asList(ascending("x"), descending("y"))));
        assertTrue(getIndexKeys().contains(parse("{x : 1, y : -1}")));
    }
}
