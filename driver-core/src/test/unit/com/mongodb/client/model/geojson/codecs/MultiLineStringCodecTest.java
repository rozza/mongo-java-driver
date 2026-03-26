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

package com.mongodb.client.model.geojson.codecs;

import com.mongodb.client.model.geojson.MultiLineString;
import com.mongodb.client.model.geojson.Position;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem.EPSG_4326_STRICT_WINDING;
import static org.bson.BsonDocument.parse;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MultiLineStringCodecTest {

    private final CodecRegistry registry = fromProviders(new GeoJsonCodecProvider());
    private final Codec<MultiLineString> codec = registry.get(MultiLineString.class);
    private final EncoderContext context = EncoderContext.builder().build();

    @Test
    void shouldRoundTrip() {
        MultiLineString multiLineString = new MultiLineString(Arrays.asList(
                Arrays.asList(new Position(Arrays.asList(1.0d, 1.0d)),
                        new Position(Arrays.asList(2.0d, 2.0d)),
                        new Position(Arrays.asList(3.0d, 4.0d))),
                Arrays.asList(new Position(Arrays.asList(2.0d, 3.0d)),
                        new Position(Arrays.asList(3.0d, 2.0d)),
                        new Position(Arrays.asList(4.0d, 4.0d)))));

        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        codec.encode(writer, multiLineString, context);
        assertEquals(parse("{type: \"MultiLineString\", "
                + "coordinates: [[[1.0, 1.0], [2.0, 2.0], [3.0, 4.0]], [[2.0, 3.0], [3.0, 2.0], [4.0, 4.0]]]}"),
                writer.getDocument());

        MultiLineString decoded = codec.decode(new BsonDocumentReader(writer.getDocument()),
                DecoderContext.builder().build());
        assertEquals(multiLineString, decoded);
    }

    @Test
    void shouldRoundTripWithCoordinateReferenceSystem() {
        MultiLineString multiLineString = new MultiLineString(EPSG_4326_STRICT_WINDING, Arrays.asList(
                Arrays.asList(new Position(Arrays.asList(1.0d, 1.0d)),
                        new Position(Arrays.asList(2.0d, 2.0d)),
                        new Position(Arrays.asList(3.0d, 4.0d))),
                Arrays.asList(new Position(Arrays.asList(2.0d, 3.0d)),
                        new Position(Arrays.asList(3.0d, 2.0d)),
                        new Position(Arrays.asList(4.0d, 4.0d)))));

        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        codec.encode(writer, multiLineString, context);
        assertEquals(parse("{type: \"MultiLineString\", "
                + "coordinates: [[[1.0, 1.0], [2.0, 2.0], [3.0, 4.0]], [[2.0, 3.0], [3.0, 2.0], [4.0, 4.0]]], "
                + "crs : {type: 'name', properties : {name : '" + EPSG_4326_STRICT_WINDING.getName() + "'}}}"),
                writer.getDocument());

        MultiLineString decoded = codec.decode(new BsonDocumentReader(writer.getDocument()),
                DecoderContext.builder().build());
        assertEquals(multiLineString, decoded);
    }

    @Test
    void shouldThrowWhenDecodingInvalidDocuments() {
        for (String invalidJson : Arrays.asList(
                "{type: \"MultiLineString\"}",
                "{coordinates: [[[1.0, 1.0], [2.0, 2.0], [3.0, 4.0]], [[2.0, 3.0], [3.0, 2.0], [4.0, 4.0]]]}",
                "{type: \"MultiLineStr\", coordinates: [[[1.0, 1.0], [2.0, 2.0], [3.0, 4.0]], [[2.0, 3.0], [3.0, 2.0], [4.0, 4.0]]]}",
                "{type: \"MultiLineString\", coordinates: [40.0, 18.0]}",
                "{type: \"MultiLineString\", coordinates: [[[[40.0, 18.0], [40.0, 19.0], [41.0, 19.0], [40.0, 18.0]]]]}",
                "{type: 'MultiLineString', crs : {type: 'name', properties : {name : '" + EPSG_4326_STRICT_WINDING.getName() + "'}}}",
                "{type: \"MultiLineString\", coordinates: [[[1.0, 1.0], [2.0, 2.0], [3.0, 4.0]]], crs : {type: \"something\"}}",
                "{type: \"MultiLineString\", coordinates: [[[1.0, 1.0], [2.0, 2.0], [3.0, 4.0]], "
                        + "[[2.0, 3.0], [3.0, 2.0], [4.0, 4.0]]], a: 1}")) {
            assertThrows(CodecConfigurationException.class, () ->
                    codec.decode(new BsonDocumentReader(parse(invalidJson)), DecoderContext.builder().build()));
        }
    }
}
