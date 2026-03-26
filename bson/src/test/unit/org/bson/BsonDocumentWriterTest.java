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

package org.bson;

import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.bson.BsonHelper.documentWithValuesOfEveryType;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BsonDocumentWriterTest {

    @Test
    @DisplayName("should write all types")
    void shouldWriteAllTypes() {
        BsonDocument encodedDoc = new BsonDocument();
        new BsonDocumentCodec().encode(new BsonDocumentWriter(encodedDoc), documentWithValuesOfEveryType(),
                EncoderContext.builder().build());

        assertEquals(documentWithValuesOfEveryType(), encodedDoc);
    }

    @Test
    @DisplayName("should pipe all types")
    void shouldPipeAllTypes() {
        BsonDocument document = new BsonDocument();
        BsonDocumentReader reader = new BsonDocumentReader(documentWithValuesOfEveryType());
        BsonDocumentWriter writer = new BsonDocumentWriter(document);

        writer.pipe(reader);

        assertEquals(documentWithValuesOfEveryType(), document);
    }

    @Test
    @DisplayName("should pipe all types with extra elements")
    void shouldPipeAllTypesWithExtraElements() {
        BsonDocument document = new BsonDocument();
        BsonDocumentReader reader = new BsonDocumentReader(new BsonDocument());
        BsonDocumentWriter writer = new BsonDocumentWriter(document);

        List<BsonElement> extraElements = new ArrayList<>();
        for (Map.Entry<String, BsonValue> entry : documentWithValuesOfEveryType().entrySet()) {
            extraElements.add(new BsonElement(entry.getKey(), entry.getValue()));
        }

        writer.pipe(reader, extraElements);

        assertEquals(documentWithValuesOfEveryType(), document);
    }
}
