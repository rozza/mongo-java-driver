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

import com.mongodb.client.model.geojson.GeoJsonObjectType;
import com.mongodb.client.model.geojson.Geometry;
import com.mongodb.client.model.geojson.GeometryCollection;
import com.mongodb.client.model.geojson.LineString;
import com.mongodb.client.model.geojson.MultiLineString;
import com.mongodb.client.model.geojson.MultiPoint;
import com.mongodb.client.model.geojson.MultiPolygon;
import com.mongodb.client.model.geojson.Point;
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

import static org.bson.BsonDocument.parse;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GeometryCodecTest {

    private final CodecRegistry registry = fromProviders(new GeoJsonCodecProvider());
    private final Codec<Geometry> codec = registry.get(Geometry.class);
    private final EncoderContext context = EncoderContext.builder().build();

    @Test
    void shouldRoundTripKnownGeometries() {
        Geometry[] geometries = {
                new LineString(Arrays.asList(new Position(101d, 0d), new Position(102d, 1d))),
                new MultiLineString(Arrays.asList(
                        Arrays.asList(new Position(Arrays.asList(1.0d, 1.0d)),
                                new Position(Arrays.asList(2.0d, 2.0d)),
                                new Position(Arrays.asList(3.0d, 4.0d))),
                        Arrays.asList(new Position(Arrays.asList(2.0d, 3.0d)),
                                new Position(Arrays.asList(3.0d, 2.0d)),
                                new Position(Arrays.asList(4.0d, 4.0d))))),
                new Point(new Position(100d, 0d)),
                new MultiPoint(Arrays.asList(
                        new Position(Arrays.asList(40.0d, 18.0d)),
                        new Position(Arrays.asList(40.0d, 19.0d)),
                        new Position(Arrays.asList(41.0d, 19.0d)))),
                new Polygon(Arrays.asList(
                        new Position(Arrays.asList(40.0d, 18.0d)),
                        new Position(Arrays.asList(40.0d, 19.0d)),
                        new Position(Arrays.asList(41.0d, 19.0d)),
                        new Position(Arrays.asList(40.0d, 18.0d)))),
                new MultiPolygon(Arrays.asList(
                        new PolygonCoordinates(Arrays.asList(
                                new Position(102.0, 2.0), new Position(103.0, 2.0),
                                new Position(103.0, 3.0), new Position(102.0, 3.0),
                                new Position(102.0, 2.0))),
                        new PolygonCoordinates(Arrays.asList(
                                new Position(100.0, 0.0), new Position(101.0, 0.0),
                                new Position(101.0, 1.0), new Position(100.0, 1.0),
                                new Position(100.0, 0.0)),
                                Arrays.asList(
                                        new Position(100.2, 0.2), new Position(100.8, 0.2),
                                        new Position(100.8, 0.8), new Position(100.2, 0.8),
                                        new Position(100.2, 0.2))))),
                new GeometryCollection(Arrays.asList(
                        new Point(new Position(100d, 0d)),
                        new LineString(Arrays.asList(new Position(101d, 0d), new Position(102d, 1d)))))
        };

        String[] geoJsons = {
                "{type: \"LineString\", coordinates: [ [101.0, 0.0], [102.0, 1.0] ]}",
                "{type: \"MultiLineString\", coordinates: [[[1.0, 1.0], [2.0, 2.0], [3.0, 4.0]], [[2.0, 3.0], [3.0, 2.0], [4.0, 4.0]]]}",
                "{type: \"Point\", coordinates: [100.0, 0.0]}",
                "{type: \"MultiPoint\", coordinates: [[40.0, 18.0], [40.0, 19.0], [41.0, 19.0]]}",
                "{type: \"Polygon\", coordinates: [[[40.0, 18.0], [40.0, 19.0], [41.0, 19.0], [40.0, 18.0]]]}",
                "{type: \"MultiPolygon\", coordinates: [[[[102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0]]], "
                        + "[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]], "
                        + "[[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2]]]]}",
                "{ type: \"GeometryCollection\", geometries: [{ type: \"Point\", coordinates: [100.0, 0.0]}, "
                        + "{ type: \"LineString\", coordinates: [ [101.0, 0.0], [102.0, 1.0] ]}]}"
        };

        for (int i = 0; i < geometries.length; i++) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
            codec.encode(writer, geometries[i], context);
            assertEquals(parse(geoJsons[i]), writer.getDocument());

            Geometry decoded = codec.decode(new BsonDocumentReader(writer.getDocument()),
                    DecoderContext.builder().build());
            assertEquals(geometries[i], decoded);
        }
    }

    @Test
    void shouldThrowWhenDecodingInvalidDocuments() {
        for (String invalidJson : Arrays.asList(
                "{type: \"GeoShard\", coordinates: [100.0, 0.0]}",
                "{coordinates: [100.0, 0.0]}",
                "{type: \"Point\", coordinates: [40.0, 18.0], crs : {type: \"link\", properties: {href: \"http://example.com/crs/42\"}}}")) {
            assertThrows(CodecConfigurationException.class, () ->
                    codec.decode(new BsonDocumentReader(parse(invalidJson)), DecoderContext.builder().build()));
        }
    }

    @Test
    void shouldNotSupportUnknownGeometries() {
        Geometry geometry = new Geometry() {
            @Override
            public GeoJsonObjectType getType() {
                return GeoJsonObjectType.POINT;
            }
        };

        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        assertThrows(CodecConfigurationException.class, () -> codec.encode(writer, geometry, context));
    }
}
