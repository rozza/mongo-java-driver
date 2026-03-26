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

import org.bson.BsonType;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.Test;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BsonTypeCodecMapTest {

    private final BsonTypeClassMap bsonTypeClassMap = new BsonTypeClassMap();
    private final CodecRegistry registry = fromRegistries(
            fromProviders(new DocumentCodecProvider(), new ValueCodecProvider(), new BsonValueCodecProvider()));
    private final BsonTypeCodecMap bsonTypeCodecMap = new BsonTypeCodecMap(bsonTypeClassMap, registry);

    @Test
    void shouldMapTypesToCodecs() {
        assertEquals(BinaryCodec.class, bsonTypeCodecMap.get(BsonType.BINARY).getClass());
        assertEquals(BooleanCodec.class, bsonTypeCodecMap.get(BsonType.BOOLEAN).getClass());
        assertEquals(DateCodec.class, bsonTypeCodecMap.get(BsonType.DATE_TIME).getClass());
        assertEquals(BsonDBPointerCodec.class, bsonTypeCodecMap.get(BsonType.DB_POINTER).getClass());
        assertEquals(DocumentCodec.class, bsonTypeCodecMap.get(BsonType.DOCUMENT).getClass());
        assertEquals(DoubleCodec.class, bsonTypeCodecMap.get(BsonType.DOUBLE).getClass());
        assertEquals(IntegerCodec.class, bsonTypeCodecMap.get(BsonType.INT32).getClass());
        assertEquals(LongCodec.class, bsonTypeCodecMap.get(BsonType.INT64).getClass());
        assertEquals(Decimal128Codec.class, bsonTypeCodecMap.get(BsonType.DECIMAL128).getClass());
        assertEquals(MaxKeyCodec.class, bsonTypeCodecMap.get(BsonType.MAX_KEY).getClass());
        assertEquals(MinKeyCodec.class, bsonTypeCodecMap.get(BsonType.MIN_KEY).getClass());
        assertEquals(CodeCodec.class, bsonTypeCodecMap.get(BsonType.JAVASCRIPT).getClass());
        assertEquals(CodeWithScopeCodec.class, bsonTypeCodecMap.get(BsonType.JAVASCRIPT_WITH_SCOPE).getClass());
        assertEquals(ObjectIdCodec.class, bsonTypeCodecMap.get(BsonType.OBJECT_ID).getClass());
        assertEquals(BsonRegularExpressionCodec.class, bsonTypeCodecMap.get(BsonType.REGULAR_EXPRESSION).getClass());
        assertEquals(StringCodec.class, bsonTypeCodecMap.get(BsonType.STRING).getClass());
        assertEquals(SymbolCodec.class, bsonTypeCodecMap.get(BsonType.SYMBOL).getClass());
        assertEquals(BsonTimestampCodec.class, bsonTypeCodecMap.get(BsonType.TIMESTAMP).getClass());
        assertEquals(BsonUndefinedCodec.class, bsonTypeCodecMap.get(BsonType.UNDEFINED).getClass());
    }

    @Test
    void shouldThrowExceptionForUnmappedType() {
        assertThrows(CodecConfigurationException.class, () -> bsonTypeCodecMap.get(BsonType.NULL));
    }

    @Test
    void shouldThrowExceptionForUnregisteredCodec() {
        assertThrows(CodecConfigurationException.class, () -> bsonTypeCodecMap.get(BsonType.ARRAY));
    }
}
