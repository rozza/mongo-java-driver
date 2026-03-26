/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.bson.codecs;

import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.RawBsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.Test;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class BsonCodecProviderTest {

    private final BsonCodecProvider provider = new BsonCodecProvider();
    private final CodecRegistry codecRegistry = fromProviders(provider);

    @Test
    void shouldGetCorrectCodec() {
        assertNull(provider.get(String.class, codecRegistry));

        assertEquals(BsonCodec.class, provider.get(BsonDocument.class, codecRegistry).getClass());
        assertEquals(BsonCodec.class, provider.get(BsonDocumentWrapper.class, codecRegistry).getClass());
        assertEquals(BsonCodec.class, provider.get(RawBsonDocument.class, codecRegistry).getClass());
        assertEquals(BsonCodec.class, provider.get(BsonDocumentSubclass.class, codecRegistry).getClass());
    }
}
