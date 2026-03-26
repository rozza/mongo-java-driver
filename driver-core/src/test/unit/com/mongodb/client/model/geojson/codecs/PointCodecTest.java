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

import com.mongodb.client.model.geojson.Point;
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

class PointCodecTest {

    private final CodecRegistry registry = fromProviders(new GeoJsonCodecProvider());
    private final Codec<Point> codec = registry.get(Point.class);
    private final EncoderContext context = EncoderContext.builder().build();

    @Test
    void shouldRoundTrip() {
        Point point = new Point(new Position(Arrays.asList(40.0d, 18.0d)));

        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        codec.encode(writer, point, context);
        assertEquals(parse("{type: \"Point\", coordinates: [40.0, 18.0]}"), writer.getDocument());

        Point decodedPoint = codec.decode(new BsonDocumentReader(writer.getDocument()), DecoderContext.builder().build());
        assertEquals(point, decodedPoint);
    }

    @Test
    void shouldRoundTripWithCoordinateReferenceSystem() {
        Point point = new Point(EPSG_4326_STRICT_WINDING, new Position(Arrays.asList(40.0d, 18.0d)));

        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        codec.encode(writer, point, context);
        assertEquals(parse("{type: \"Point\", coordinates: [40.0, 18.0], "
                + "crs : {type: 'name', properties : {name : '" + EPSG_4326_STRICT_WINDING.getName() + "'}}}"),
                writer.getDocument());

        Point decodedPoint = codec.decode(new BsonDocumentReader(writer.getDocument()), DecoderContext.builder().build());
        assertEquals(point, decodedPoint);
    }

    @Test
    void shouldThrowWhenDecodingInvalidDocuments() {
        for (String invalidJson : Arrays.asList(
                "{type: \"Point\"}",
                "{coordinates: [40.0, 18.0]}",
                "{type: \"Pointer\", coordinates: [40.0, 18.0]}",
                "{type: 'Point', crs : {type: 'name', properties : {name : '" + EPSG_4326_STRICT_WINDING.getName() + "'}}}",
                "{type: \"Point\", coordinates: [40.0, 18.0], crs : {type: \"link\", properties: {href: \"http://example.com/crs/42\"}}}",
                "{type: \"Point\", coordinates: [40.0, 18.0], crs : {type: \"name\", properties: {}}}",
                "{type: \"Point\", coordinates: [40.0, 18.0], abc: 123}")) {
            assertThrows(CodecConfigurationException.class, () ->
                    codec.decode(new BsonDocumentReader(parse(invalidJson)), DecoderContext.builder().build()));
        }
    }
}
