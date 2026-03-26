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

import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDbPointer;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.RawBsonArray;
import org.bson.RawBsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.Test;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class BsonValueCodecProviderTest {

    private final BsonValueCodecProvider provider = new BsonValueCodecProvider();
    private final CodecRegistry codecRegistry = fromProviders(provider);

    @Test
    void shouldGetCorrectCodec() {
        assertNull(provider.get(String.class, codecRegistry));

        assertEquals(BsonInt32Codec.class, provider.get(BsonInt32.class, codecRegistry).getClass());
        assertEquals(BsonInt64Codec.class, provider.get(BsonInt64.class, codecRegistry).getClass());
        assertEquals(BsonDoubleCodec.class, provider.get(BsonDouble.class, codecRegistry).getClass());
        assertEquals(BsonStringCodec.class, provider.get(BsonString.class, codecRegistry).getClass());
        assertEquals(BsonBooleanCodec.class, provider.get(BsonBoolean.class, codecRegistry).getClass());
        assertEquals(BsonDecimal128Codec.class, provider.get(BsonDecimal128.class, codecRegistry).getClass());

        assertEquals(BsonNullCodec.class, provider.get(BsonNull.class, codecRegistry).getClass());
        assertEquals(BsonDateTimeCodec.class, provider.get(BsonDateTime.class, codecRegistry).getClass());
        assertEquals(BsonMinKeyCodec.class, provider.get(BsonMinKey.class, codecRegistry).getClass());
        assertEquals(BsonMaxKeyCodec.class, provider.get(BsonMaxKey.class, codecRegistry).getClass());
        assertEquals(BsonJavaScriptCodec.class, provider.get(BsonJavaScript.class, codecRegistry).getClass());
        assertEquals(BsonObjectIdCodec.class, provider.get(BsonObjectId.class, codecRegistry).getClass());
        assertEquals(BsonRegularExpressionCodec.class, provider.get(BsonRegularExpression.class, codecRegistry).getClass());
        assertEquals(BsonSymbolCodec.class, provider.get(BsonSymbol.class, codecRegistry).getClass());
        assertEquals(BsonTimestampCodec.class, provider.get(BsonTimestamp.class, codecRegistry).getClass());
        assertEquals(BsonUndefinedCodec.class, provider.get(BsonUndefined.class, codecRegistry).getClass());
        assertEquals(BsonDBPointerCodec.class, provider.get(BsonDbPointer.class, codecRegistry).getClass());

        assertEquals(BsonJavaScriptWithScopeCodec.class, provider.get(BsonJavaScriptWithScope.class, codecRegistry).getClass());

        assertEquals(BsonArrayCodec.class, provider.get(BsonArray.class, codecRegistry).getClass());
        assertEquals(BsonArrayCodec.class, provider.get(RawBsonArray.class, codecRegistry).getClass());

        assertEquals(BsonDocumentCodec.class, provider.get(BsonDocument.class, codecRegistry).getClass());
        assertEquals(BsonDocumentWrapperCodec.class, provider.get(BsonDocumentWrapper.class, codecRegistry).getClass());
        assertEquals(RawBsonDocumentCodec.class, provider.get(RawBsonDocument.class, codecRegistry).getClass());
        assertEquals(BsonDocumentCodec.class, provider.get(BsonDocumentSubclass.class, codecRegistry).getClass());
    }
}
