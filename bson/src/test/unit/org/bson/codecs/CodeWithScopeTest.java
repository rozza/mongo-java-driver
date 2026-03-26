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

import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.Document;
import org.bson.types.CodeWithScope;
import org.junit.jupiter.api.Test;

import static org.bson.codecs.CodecTestUtil.prepareReaderWithObjectToBeDecoded;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CodeWithScopeTest {

    private final CodeWithScopeCodec codeWithScopeCodec = new CodeWithScopeCodec(new DocumentCodec());

    @Test
    void shouldEncodeCodeWithScope() {
        String javascriptCode = "<javascript code>";
        CodeWithScope codeWithScope = new CodeWithScope(javascriptCode, new Document("the", "scope"));

        BsonDocument bsonDocument = new BsonDocument();
        BsonDocumentWriter writer = new BsonDocumentWriter(bsonDocument);
        writer.writeStartDocument();
        writer.writeName("code");
        codeWithScopeCodec.encode(writer, codeWithScope, EncoderContext.builder().build());
        writer.writeEndDocument();

        assertEquals(javascriptCode, bsonDocument.get("code").asJavaScriptWithScope().getCode());
        assertEquals("scope", bsonDocument.get("code").asJavaScriptWithScope().getScope().getString("the").getValue());
    }

    @Test
    void shouldDecodeCodeWithScope() {
        CodeWithScope codeWithScope = new CodeWithScope("{javascript code}", new Document("the", "scope"));
        BsonBinaryReader reader = prepareReaderWithObjectToBeDecoded(codeWithScope);

        CodeWithScope actualCodeWithScope = codeWithScopeCodec.decode(reader, DecoderContext.builder().build());

        assertEquals(codeWithScope, actualCodeWithScope);
    }
}
