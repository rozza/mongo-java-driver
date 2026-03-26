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

package com.mongodb.internal.connection;

import org.bson.BsonArray;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.BsonOutput;
import org.bson.io.OutputBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.bson.BsonHelper.documentWithValuesOfEveryType;
import static org.bson.BsonHelper.getBsonValues;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IdHoldingBsonWriterTest {

    private static final BsonObjectId OBJECT_ID = new BsonObjectId();

    static Stream<BsonObjectId> fallbackIdProvider() {
        return Stream.of(null, OBJECT_ID);
    }

    @ParameterizedTest
    @MethodSource("fallbackIdProvider")
    void shouldWriteAllTypes(BsonObjectId fallbackId) {
        BsonBinaryWriter bsonBinaryWriter = new BsonBinaryWriter(new BasicOutputBuffer());
        IdHoldingBsonWriter idTrackingBsonWriter = new IdHoldingBsonWriter(bsonBinaryWriter, fallbackId);
        BsonDocument document = documentWithValuesOfEveryType();

        new BsonDocumentCodec().encode(idTrackingBsonWriter, document, EncoderContext.builder().build());
        BsonDocument encodedDocument = getEncodedDocument(bsonBinaryWriter.getBsonOutput());

        assertTrue(!document.containsKey("_id"));
        assertTrue(encodedDocument.containsKey("_id"));
        assertEquals(encodedDocument.get("_id"), idTrackingBsonWriter.getId());
        if (fallbackId != null) {
            assertEquals(fallbackId, idTrackingBsonWriter.getId());
        }

        encodedDocument.remove("_id");
        assertEquals(document, encodedDocument);
    }

    static Stream<Object[]> idAndFallbackProvider() {
        List<BsonValue> bsonValues = getBsonValues();
        List<BsonObjectId> fallbackIds = Arrays.asList(null, new BsonObjectId());
        List<Object[]> combinations = new ArrayList<>();
        for (BsonValue id : bsonValues) {
            for (BsonObjectId fallbackId : fallbackIds) {
                combinations.add(new Object[]{id, fallbackId});
            }
        }
        return combinations.stream();
    }

    @ParameterizedTest
    @MethodSource("idAndFallbackProvider")
    void shouldSupportAllTypesForIdValue(BsonValue id, BsonObjectId fallbackId) {
        BsonBinaryWriter bsonBinaryWriter = new BsonBinaryWriter(new BasicOutputBuffer());
        IdHoldingBsonWriter idTrackingBsonWriter = new IdHoldingBsonWriter(bsonBinaryWriter, fallbackId);
        BsonDocument document = new BsonDocument();
        document.put("_id", id);

        new BsonDocumentCodec().encode(idTrackingBsonWriter, document, EncoderContext.builder().build());
        BsonDocument encodedDocument = getEncodedDocument(bsonBinaryWriter.getBsonOutput());

        assertEquals(document, encodedDocument);
        assertEquals(id, idTrackingBsonWriter.getId());
    }

    @ParameterizedTest
    @MethodSource("fallbackIdProvider")
    void serializeDocumentWithListOfDocumentsThatContainAnIdField(BsonObjectId fallbackId) {
        BsonBinaryWriter bsonBinaryWriter = new BsonBinaryWriter(new BasicOutputBuffer());
        IdHoldingBsonWriter idTrackingBsonWriter = new IdHoldingBsonWriter(bsonBinaryWriter, fallbackId);
        BsonDocument document = new BsonDocument("_id", new BsonObjectId())
                .append("items", new BsonArray(Collections.singletonList(new BsonDocument("_id", new BsonObjectId()))));

        new BsonDocumentCodec().encode(idTrackingBsonWriter, document, EncoderContext.builder().build());
        BsonDocument encodedDocument = getEncodedDocument(bsonBinaryWriter.getBsonOutput());

        assertEquals(document, encodedDocument);
    }

    static Stream<Object[]> jsonAndFallbackProvider() {
        List<String> jsonValues = Arrays.asList(
                "{\"_id\": {\"a\": []}, \"b\": 123}",
                "{\"_id\": {\"a\": [1, 2]}, \"b\": 123}",
                "{\"_id\": {\"a\": [[[[1]]]]}, \"b\": 123}",
                "{\"_id\": {\"a\": [{\"a\": [1, 2]}]}, \"b\": 123}",
                "{\"_id\": {\"a\": {\"a\": [1, 2]}}, \"b\": 123}",
                "{\"_id\": {\"a\": [1, 2], \"b\": [123]}}",
                "{\"_id\": [], \"b\": 123}",
                "{\"_id\": [1, 2], \"b\": 123}",
                "{\"_id\": [[1], [[2]]], \"b\": 123}",
                "{\"_id\": [{\"a\": 1}], \"b\": 123}",
                "{\"_id\": [{\"a\": [{\"b\": 123}]}]}"
        );
        List<BsonObjectId> fallbackIds = Arrays.asList(null, new BsonObjectId());
        List<Object[]> combinations = new ArrayList<>();
        for (String json : jsonValues) {
            for (BsonObjectId fallbackId : fallbackIds) {
                combinations.add(new Object[]{json, fallbackId});
            }
        }
        return combinations.stream();
    }

    @ParameterizedTest
    @MethodSource("jsonAndFallbackProvider")
    void serializeIdDocumentsContainingArrays(String json, BsonObjectId fallbackId) {
        BsonBinaryWriter bsonBinaryWriter = new BsonBinaryWriter(new BasicOutputBuffer());
        IdHoldingBsonWriter idTrackingBsonWriter = new IdHoldingBsonWriter(bsonBinaryWriter, fallbackId);
        BsonDocument document = BsonDocument.parse(json);

        new BsonDocumentCodec().encode(idTrackingBsonWriter, document, EncoderContext.builder()
                .isEncodingCollectibleDocument(true).build());
        BsonDocument encodedDocument = getEncodedDocument(bsonBinaryWriter.getBsonOutput());

        assertEquals(document, encodedDocument);
    }

    private static BsonDocument getEncodedDocument(BsonOutput buffer) {
        return new BsonDocumentCodec().decode(new BsonBinaryReader(((OutputBuffer) buffer).getByteBuffers().get(0).asNIO()),
                DecoderContext.builder().build());
    }
}
