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

import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.json.JsonMode;
import org.bson.json.JsonObject;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.Test;

import static org.bson.BsonDocument.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonObjectCodecTest {

    @Test
    void shouldHaveJsonObjectEncodingClass() {
        JsonObjectCodec codec = new JsonObjectCodec();
        assertEquals(JsonObject.class, codec.getEncoderClass());
    }

    @Test
    void shouldEncodeJsonObjectCorrectly() {
        JsonObjectCodec codec = new JsonObjectCodec();
        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

        codec.encode(writer, new JsonObject("{hello: {world: 1}}"), EncoderContext.builder().build());

        assertEquals(parse("{hello: {world: 1}}"), writer.getDocument());
    }

    @Test
    void shouldDecodeJsonObjectCorrectly() {
        JsonObjectCodec codec = new JsonObjectCodec();
        BsonDocumentReader reader = new BsonDocumentReader(parse("{hello: {world: 1}}"));

        JsonObject jsonObject = codec.decode(reader, DecoderContext.builder().build());

        assertEquals("{\"hello\": {\"world\": 1}}", jsonObject.getJson());
    }

    @Test
    void shouldUseJsonWriterSettings() {
        JsonObjectCodec codec = new JsonObjectCodec(
                JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build());
        BsonDocumentReader reader = new BsonDocumentReader(parse("{hello: 1}"));

        JsonObject jsonObject = codec.decode(reader, DecoderContext.builder().build());

        assertEquals("{\"hello\": {\"$numberInt\": \"1\"}}", jsonObject.getJson());
    }
}
