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

import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.internal.bulk.IndexRequest;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexRequestTest {

    @Test
    @DisplayName("should set its options correctly")
    void shouldSetOptionsCorrectly() {
        IndexRequest request = new IndexRequest(new BsonDocument("a", new BsonInt32(1)));

        assertEquals(new BsonDocument("a", new BsonInt32(1)), request.getKeys());
        assertFalse(request.isBackground());
        assertFalse(request.isUnique());
        assertFalse(request.isSparse());
        assertNull(request.getName());
        assertNull(request.getExpireAfter(TimeUnit.SECONDS));
        assertNull(request.getVersion());
        assertNull(request.getWeights());
        assertNull(request.getDefaultLanguage());
        assertNull(request.getLanguageOverride());
        assertNull(request.getTextVersion());
        assertNull(request.getSphereVersion());
        assertNull(request.getBits());
        assertNull(request.getMin());
        assertNull(request.getMax());
        assertFalse(request.getDropDups());
        assertNull(request.getStorageEngine());
        assertNull(request.getPartialFilterExpression());
        assertNull(request.getCollation());
        assertNull(request.getWildcardProjection());
        assertFalse(request.isHidden());

        BsonDocument keys = BsonDocument.parse("{ a: 1 }");
        BsonDocument weights = BsonDocument.parse("{ a: 1000 }");
        BsonDocument storageEngine = BsonDocument.parse("{ wiredTiger : { configString : \"block_compressor=zlib\" }}");
        BsonDocument partialFilterExpression = BsonDocument.parse("{ a: { $gte: 10 } }");
        Collation collation = Collation.builder()
                .locale("en")
                .caseLevel(true)
                .collationCaseFirst(CollationCaseFirst.OFF)
                .collationStrength(CollationStrength.IDENTICAL)
                .numericOrdering(true)
                .collationAlternate(CollationAlternate.SHIFTED)
                .collationMaxVariable(CollationMaxVariable.SPACE)
                .backwards(true)
                .build();
        BsonDocument wildcardProjection = BsonDocument.parse("{a  : 1}");
        IndexRequest request2 = new IndexRequest(keys)
                .background(true)
                .unique(true)
                .sparse(true)
                .name("aIndex")
                .expireAfter(100L, TimeUnit.SECONDS)
                .version(1)
                .weights(weights)
                .defaultLanguage("es")
                .languageOverride("language")
                .textVersion(1)
                .sphereVersion(2)
                .bits(1)
                .min(-180.0)
                .max(180.0)
                .dropDups(true)
                .storageEngine(storageEngine)
                .partialFilterExpression(partialFilterExpression)
                .collation(collation)
                .wildcardProjection(wildcardProjection)
                .hidden(true);

        assertEquals(keys, request2.getKeys());
        assertTrue(request2.isBackground());
        assertTrue(request2.isUnique());
        assertTrue(request2.isSparse());
        assertEquals("aIndex", request2.getName());
        assertEquals(Long.valueOf(100), request2.getExpireAfter(TimeUnit.SECONDS));
        assertEquals(Integer.valueOf(1), request2.getVersion());
        assertEquals(weights, request2.getWeights());
        assertEquals("es", request2.getDefaultLanguage());
        assertEquals("language", request2.getLanguageOverride());
        assertEquals(Integer.valueOf(1), request2.getTextVersion());
        assertEquals(Integer.valueOf(2), request2.getSphereVersion());
        assertEquals(Integer.valueOf(1), request2.getBits());
        assertEquals(-180.0, request2.getMin());
        assertEquals(180.0, request2.getMax());
        assertTrue(request2.getDropDups());
        assertEquals(storageEngine, request2.getStorageEngine());
        assertEquals(partialFilterExpression, request2.getPartialFilterExpression());
        assertEquals(collation, request2.getCollation());
        assertEquals(wildcardProjection, request2.getWildcardProjection());
        assertTrue(request2.isHidden());
    }

    @Test
    @DisplayName("should validate textIndexVersion")
    void shouldValidateTextIndexVersion() {
        IndexRequest options = new IndexRequest(new BsonDocument("a", new BsonInt32(1)));

        assertDoesNotThrow(() -> options.textVersion(1));
        assertDoesNotThrow(() -> options.textVersion(2));
        assertDoesNotThrow(() -> options.textVersion(3));
        assertThrows(IllegalArgumentException.class, () -> options.textVersion(4));
    }

    @Test
    @DisplayName("should validate 2dsphereIndexVersion")
    void shouldValidate2dsphereIndexVersion() {
        IndexRequest options = new IndexRequest(new BsonDocument("a", new BsonInt32(1)));

        assertDoesNotThrow(() -> options.sphereVersion(1));
        assertDoesNotThrow(() -> options.sphereVersion(2));
        assertDoesNotThrow(() -> options.sphereVersion(3));
        assertThrows(IllegalArgumentException.class, () -> options.sphereVersion(4));
    }
}
