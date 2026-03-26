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

package org.bson.codecs;

import org.bson.BsonDbPointer;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonUndefined;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWithScope;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BsonTypeClassMapTest {

    @Test
    void shouldHaveDefaultsForAllBsonTypes() {
        BsonTypeClassMap map = new BsonTypeClassMap();

        assertEquals(Binary.class, map.get(BsonType.BINARY));
        assertEquals(Boolean.class, map.get(BsonType.BOOLEAN));
        assertEquals(Date.class, map.get(BsonType.DATE_TIME));
        assertEquals(BsonDbPointer.class, map.get(BsonType.DB_POINTER));
        assertEquals(Document.class, map.get(BsonType.DOCUMENT));
        assertEquals(Double.class, map.get(BsonType.DOUBLE));
        assertEquals(Integer.class, map.get(BsonType.INT32));
        assertEquals(Long.class, map.get(BsonType.INT64));
        assertEquals(Decimal128.class, map.get(BsonType.DECIMAL128));
        assertEquals(MaxKey.class, map.get(BsonType.MAX_KEY));
        assertEquals(MinKey.class, map.get(BsonType.MIN_KEY));
        assertEquals(Code.class, map.get(BsonType.JAVASCRIPT));
        assertEquals(CodeWithScope.class, map.get(BsonType.JAVASCRIPT_WITH_SCOPE));
        assertEquals(ObjectId.class, map.get(BsonType.OBJECT_ID));
        assertEquals(BsonRegularExpression.class, map.get(BsonType.REGULAR_EXPRESSION));
        assertEquals(String.class, map.get(BsonType.STRING));
        assertEquals(Symbol.class, map.get(BsonType.SYMBOL));
        assertEquals(BsonTimestamp.class, map.get(BsonType.TIMESTAMP));
        assertEquals(BsonUndefined.class, map.get(BsonType.UNDEFINED));
        assertEquals(List.class, map.get(BsonType.ARRAY));
    }

    @Test
    void shouldObeyReplacements() {
        Map<BsonType, Class<?>> replacements = new HashMap<>();
        replacements.put(BsonType.DATE_TIME, java.sql.Date.class);
        BsonTypeClassMap map = new BsonTypeClassMap(replacements);

        assertEquals(java.sql.Date.class, map.get(BsonType.DATE_TIME));
    }
}
