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

import org.bson.BinaryVector;
import org.bson.Document;
import org.bson.Float32BinaryVector;
import org.bson.Int8BinaryVector;
import org.bson.PackedBitBinaryVector;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

class ValueCodecProviderTest {

    private final ValueCodecProvider provider = new ValueCodecProvider();
    private final CodecRegistry registry = fromProviders(provider);

    @Test
    void shouldProvideSupportedCodecs() {
        assertInstanceOf(AtomicBooleanCodec.class, provider.get(AtomicBoolean.class, registry));
        assertInstanceOf(AtomicIntegerCodec.class, provider.get(AtomicInteger.class, registry));
        assertInstanceOf(AtomicLongCodec.class, provider.get(AtomicLong.class, registry));

        assertInstanceOf(BooleanCodec.class, provider.get(Boolean.class, registry));
        assertInstanceOf(IntegerCodec.class, provider.get(Integer.class, registry));
        assertInstanceOf(LongCodec.class, provider.get(Long.class, registry));
        assertInstanceOf(Decimal128Codec.class, provider.get(Decimal128.class, registry));
        assertInstanceOf(BigDecimalCodec.class, provider.get(java.math.BigDecimal.class, registry));
        assertInstanceOf(DoubleCodec.class, provider.get(Double.class, registry));
        assertInstanceOf(CharacterCodec.class, provider.get(Character.class, registry));
        assertInstanceOf(StringCodec.class, provider.get(String.class, registry));
        assertInstanceOf(DateCodec.class, provider.get(Date.class, registry));
        assertInstanceOf(ByteCodec.class, provider.get(Byte.class, registry));
        assertInstanceOf(PatternCodec.class, provider.get(Pattern.class, registry));
        assertInstanceOf(ShortCodec.class, provider.get(Short.class, registry));
        assertInstanceOf(ByteArrayCodec.class, provider.get(byte[].class, registry));
        assertInstanceOf(FloatCodec.class, provider.get(Float.class, registry));
        assertInstanceOf(BinaryVectorCodec.class, provider.get(BinaryVector.class, registry));
        assertInstanceOf(Float32BinaryVectorCodec.class, provider.get(Float32BinaryVector.class, registry));
        assertInstanceOf(Int8VectorCodec.class, provider.get(Int8BinaryVector.class, registry));
        assertInstanceOf(PackedBitBinaryVectorCodec.class, provider.get(PackedBitBinaryVector.class, registry));

        assertInstanceOf(BinaryCodec.class, provider.get(Binary.class, registry));
        assertInstanceOf(MinKeyCodec.class, provider.get(MinKey.class, registry));
        assertInstanceOf(MaxKeyCodec.class, provider.get(MaxKey.class, registry));
        assertInstanceOf(CodeCodec.class, provider.get(Code.class, registry));
        assertInstanceOf(ObjectIdCodec.class, provider.get(ObjectId.class, registry));
        assertInstanceOf(SymbolCodec.class, provider.get(Symbol.class, registry));
        assertInstanceOf(OverridableUuidRepresentationCodec.class, provider.get(UUID.class, registry));

        assertNull(provider.get(Document.class, registry));
    }
}
