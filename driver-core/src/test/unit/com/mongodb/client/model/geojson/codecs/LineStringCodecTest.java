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

import com.mongodb.client.model.geojson.LineString;
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

class LineStringCodecTest {

    private final CodecRegistry registry = fromProviders(new GeoJsonCodecProvider());
    private final Codec<LineString> codec = registry.get(LineString.class);
    private final EncoderContext context = EncoderContext.builder().build();

    @Test
    void shouldRoundTrip() {
        LineString lineString = new LineString(Arrays.asList(
                new Position(Arrays.asList(40.0d, 18.0d)),
                new Position(Arrays.asList(40.0d, 19.0d)),
                new Position(Arrays.asList(41.0d, 19.0d))));

        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        codec.encode(writer, lineString, context);
        assertEquals(parse("{type: \"LineString\", coordinates: [[40.0, 18.0], [40.0, 19.0], [41.0, 19.0]]}"),
                writer.getDocument());

        LineString decodedLineString = codec.decode(new BsonDocumentReader(writer.getDocument()),
                DecoderContext.builder().build());
        assertEquals(lineString, decodedLineString);
    }

    @Test
    void shouldRoundTripWithCoordinateReferenceSystem() {
        LineString lineString = new LineString(EPSG_4326_STRICT_WINDING, Arrays.asList(
                new Position(Arrays.asList(40.0d, 20.0d)),
                new Position(Arrays.asList(40.0d, 40.0d)),
                new Position(Arrays.asList(20.0d, 40.0d))));

        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        codec.encode(writer, lineString, context);
        assertEquals(parse("{type: \"LineString\", coordinates: [[40.0, 20.0], [40.0, 40.0], [20.0, 40.0]], "
                + "crs : {type: 'name', properties : {name : '" + EPSG_4326_STRICT_WINDING.getName() + "'}}}"),
                writer.getDocument());

        LineString decodedLineString = codec.decode(new BsonDocumentReader(writer.getDocument()),
                DecoderContext.builder().build());
        assertEquals(lineString, decodedLineString);
    }

    @Test
    void shouldThrowWhenDecodingInvalidDocuments() {
        for (String invalidJson : Arrays.asList(
                "{type: \"lineString\"}",
                "{coordinates: [[40.0, 18.0], [40.0, 19.0]]}",
                "{type: \"lineStr\", coordinates: [[40.0, 18.0], [40.0, 19.0]]}",
                "{type: \"lineString\", coordinates: [40.0, 18.0]}",
                "{type: \"lineString\", coordinates: []}",
                "{type: \"lineString\", coordinates: [[]]}",
                "{type: \"lineString\", coordinates: [[[40.0, 18.0], [40.0, 19.0], [41.0, 19.0], [40.0, 18.0]]]}",
                "{type: 'lineString', crs : {type: 'name', properties : {name : '" + EPSG_4326_STRICT_WINDING.getName() + "'}}}",
                "{type: \"lineString\", coordinates: [[40.0, 18.0], [40.0, 19.0], [41.0, 19.0], [40.0, 18.0]], crs : {type: \"something\"}}",
                "{type: \"lineString\", coordinates: [[40.0, 18.0], [40.0, 19.0], [41.0, 19.0], [40.0, 18.0]], "
                        + "crs : {type: \"link\", properties: {href: \"http://example.com/crs/42\"}}}",
                "{type: \"lineString\", coordinates: [[40.0, 18.0], [40.0, 19.0]], abc: 123}")) {
            assertThrows(CodecConfigurationException.class, () ->
                    codec.decode(new BsonDocumentReader(parse(invalidJson)), DecoderContext.builder().build()));
        }
    }
}
