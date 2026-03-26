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

import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.PolygonCoordinates;
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

class PolygonCodecTest {

    private final CodecRegistry registry = fromProviders(new GeoJsonCodecProvider());
    private final Codec<Polygon> codec = registry.get(Polygon.class);
    private final EncoderContext context = EncoderContext.builder().build();

    @Test
    void shouldRoundTrip() {
        Polygon polygon = new Polygon(Arrays.asList(
                new Position(Arrays.asList(40.0d, 18.0d)),
                new Position(Arrays.asList(40.0d, 19.0d)),
                new Position(Arrays.asList(41.0d, 19.0d)),
                new Position(Arrays.asList(40.0d, 18.0d))));

        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        codec.encode(writer, polygon, context);
        assertEquals(parse("{type: \"Polygon\", coordinates: [[[40.0, 18.0], [40.0, 19.0], [41.0, 19.0], [40.0, 18.0]]]}"),
                writer.getDocument());

        Polygon decodedPolygon = codec.decode(new BsonDocumentReader(writer.getDocument()),
                DecoderContext.builder().build());
        assertEquals(polygon, decodedPolygon);
    }

    @Test
    void shouldRoundTripWithCoordinateReferenceSystem() {
        Polygon polygon = new Polygon(EPSG_4326_STRICT_WINDING,
                new PolygonCoordinates(Arrays.asList(
                        new Position(Arrays.asList(40.0d, 20.0d)),
                        new Position(Arrays.asList(40.0d, 40.0d)),
                        new Position(Arrays.asList(20.0d, 40.0d)),
                        new Position(Arrays.asList(40.0d, 20.0d)))));

        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        codec.encode(writer, polygon, context);
        assertEquals(parse("{type: 'Polygon', coordinates: [[[40.0, 20.0], [40.0, 40.0], [20.0, 40.0], [40.0, 20.0]]], "
                + "crs : {type: 'name', properties : {name : '" + EPSG_4326_STRICT_WINDING.getName() + "'}}}"),
                writer.getDocument());

        Polygon decodedPolygon = codec.decode(new BsonDocumentReader(writer.getDocument()),
                DecoderContext.builder().build());
        assertEquals(polygon, decodedPolygon);
    }

    @Test
    void shouldRoundTripWithHoles() {
        Polygon polygon = new Polygon(
                Arrays.asList(
                        new Position(Arrays.asList(40.0d, 20.0d)),
                        new Position(Arrays.asList(40.0d, 40.0d)),
                        new Position(Arrays.asList(20.0d, 40.0d)),
                        new Position(Arrays.asList(40.0d, 20.0d))),
                Arrays.asList(
                        new Position(Arrays.asList(30.0d, 25.0d)),
                        new Position(Arrays.asList(30.0d, 35.0d)),
                        new Position(Arrays.asList(25.0d, 25.0d)),
                        new Position(Arrays.asList(30.0d, 25.0d))),
                Arrays.asList(
                        new Position(Arrays.asList(36.0d, 37.0d)),
                        new Position(Arrays.asList(36.0d, 37.0d)),
                        new Position(Arrays.asList(37.0d, 37.0d)),
                        new Position(Arrays.asList(36.0d, 37.0d))));

        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        codec.encode(writer, polygon, context);
        assertEquals(parse("{type: 'Polygon', coordinates: "
                + "[[[40.0, 20.0], [40.0, 40.0], [20.0, 40.0], [40.0, 20.0]], "
                + "[[30.0, 25.0], [30.0, 35.0], [25.0, 25.0], [30.0, 25.0]], "
                + "[[36.0, 37.0], [36.0, 37.0], [37.0, 37.0], [36.0, 37.0]]]}"),
                writer.getDocument());

        Polygon decodedPolygon = codec.decode(new BsonDocumentReader(writer.getDocument()),
                DecoderContext.builder().build());
        assertEquals(polygon, decodedPolygon);
    }

    @Test
    void shouldThrowWhenDecodingInvalidDocuments() {
        for (String invalidJson : Arrays.asList(
                "{type: \"Polygon\"}",
                "{coordinates: [[[40.0, 18.0], [40.0, 19.0], [41.0, 19.0], [40.0, 18.0]]]}",
                "{type: \"Polygot\", coordinates: [[[40.0, 18.0], [40.0, 19.0], [41.0, 19.0], [40.0, 18.0]]]}",
                "{type: \"Polygon\", coordinates: [[[40.0, 18.0], [40.0, 19.0]]]}",
                "{type: \"Polygon\", coordinates: []}",
                "{type: \"Polygon\", coordinates: [[]]}",
                "{type: \"Polygon\", coordinates: [[[]]]}",
                "{type: 'Polygon', crs : {type: 'name', properties : {name : '" + EPSG_4326_STRICT_WINDING.getName() + "'}}}",
                "{type: \"Polygon\", coordinates: [[[40.0, 18.0], [40.0, 19.0], [41.0, 19.0], [40.0, 18.0]]], crs : {type: \"something\"}}",
                "{type: \"Polygon\", coordinates: [[[40.0, 18.0], [40.0, 19.0], [41.0, 19.0], [40.0, 18.0]]], "
                        + "crs : {type: \"link\", properties: {href: \"http://example.com/crs/42\"}}}",
                "{type: \"Polygon\", coordinates: [[[40.0, 18.0], [40.0, 19.0], [41.0, 19.0], [40.0, 18.0]]], abc: 123}")) {
            assertThrows(CodecConfigurationException.class, () ->
                    codec.decode(new BsonDocumentReader(parse(invalidJson)), DecoderContext.builder().build()));
        }
    }
}
