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

import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexOptionsTest {

    @Test
    void shouldSetOptionsCorrectly() {
        IndexOptions options = new IndexOptions();

        assertFalse(options.isBackground());
        assertFalse(options.isUnique());
        assertFalse(options.isSparse());
        assertNull(options.getName());
        assertNull(options.getExpireAfter(TimeUnit.SECONDS));
        assertNull(options.getVersion());
        assertNull(options.getWeights());
        assertNull(options.getDefaultLanguage());
        assertNull(options.getLanguageOverride());
        assertNull(options.getTextVersion());
        assertNull(options.getSphereVersion());
        assertNull(options.getBits());
        assertNull(options.getMin());
        assertNull(options.getMax());
        assertNull(options.getStorageEngine());
        assertNull(options.getPartialFilterExpression());
        assertNull(options.getCollation());
        assertNull(options.getWildcardProjection());
        assertFalse(options.isHidden());

        BsonDocument wildcardProjection = BsonDocument.parse("{a  : 1}");
        BsonDocument weights = BsonDocument.parse("{ a: 1000 }");
        BsonDocument storageEngine = BsonDocument.parse("{ wiredTiger : { configString : \"block_compressor=zlib\" }}");
        BsonDocument partialFilterExpression = BsonDocument.parse("{ a: { $gte: 10 } }");
        Collation collation = Collation.builder().locale("en").build();

        options.background(true)
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
                .storageEngine(storageEngine)
                .partialFilterExpression(partialFilterExpression)
                .collation(collation)
                .wildcardProjection(wildcardProjection)
                .hidden(true);

        assertTrue(options.isBackground());
        assertTrue(options.isUnique());
        assertTrue(options.isSparse());
        assertEquals("aIndex", options.getName());
        assertEquals(Long.valueOf(100), options.getExpireAfter(TimeUnit.SECONDS));
        assertEquals(Integer.valueOf(1), options.getVersion());
        assertEquals(weights, options.getWeights());
        assertEquals("es", options.getDefaultLanguage());
        assertEquals("language", options.getLanguageOverride());
        assertEquals(Integer.valueOf(1), options.getTextVersion());
        assertEquals(Integer.valueOf(2), options.getSphereVersion());
        assertEquals(Integer.valueOf(1), options.getBits());
        assertEquals(-180.0, options.getMin());
        assertEquals(180.0, options.getMax());
        assertEquals(storageEngine, options.getStorageEngine());
        assertEquals(partialFilterExpression, options.getPartialFilterExpression());
        assertEquals(collation, options.getCollation());
        assertEquals(wildcardProjection, options.getWildcardProjection());
        assertTrue(options.isHidden());
    }

    @Test
    void shouldConvertExpireAfter() {
        IndexOptions options = new IndexOptions();
        assertNull(options.getExpireAfter(TimeUnit.SECONDS));

        options = new IndexOptions().expireAfter(null, null);
        assertNull(options.getExpireAfter(TimeUnit.SECONDS));

        options = new IndexOptions().expireAfter(4L, TimeUnit.MILLISECONDS);
        assertEquals(Long.valueOf(0), options.getExpireAfter(TimeUnit.SECONDS));

        options = new IndexOptions().expireAfter(1004L, TimeUnit.MILLISECONDS);
        assertEquals(Long.valueOf(1), options.getExpireAfter(TimeUnit.SECONDS));
    }
}
