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

import com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem;
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

import static com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem.CRS_84;
import static com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem.EPSG_4326;
import static com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem.EPSG_4326_STRICT_WINDING;
import static org.bson.BsonDocument.parse;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NamedCoordinateReferenceSystemTest {

    private final CodecRegistry registry = fromProviders(new GeoJsonCodecProvider());
    private final Codec<NamedCoordinateReferenceSystem> codec = registry.get(NamedCoordinateReferenceSystem.class);
    private final EncoderContext context = EncoderContext.builder().build();

    @Test
    void shouldRoundTrip() {
        for (NamedCoordinateReferenceSystem crs : Arrays.asList(CRS_84, EPSG_4326, EPSG_4326_STRICT_WINDING)) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
            codec.encode(writer, crs, context);
            assertEquals(parse("{type: 'name', properties : {name : '" + crs.getName() + "'}}"), writer.getDocument());

            NamedCoordinateReferenceSystem decodedCrs = codec.decode(new BsonDocumentReader(writer.getDocument()),
                    DecoderContext.builder().build());
            assertEquals(crs, decodedCrs);
        }
    }

    @Test
    void shouldThrowWhenDecodingInvalidDocuments() {
        for (String invalidJson : Arrays.asList(
                "{type: \"name\"}",
                "{type: \"name\",  properties : {}}",
                "{type: \"name\",  properties : {type: \"link\", properties: {href: \"http://example.com/crs/42\"}}}")) {
            assertThrows(CodecConfigurationException.class, () ->
                    codec.decode(new BsonDocumentReader(parse(invalidJson)), DecoderContext.builder().build()));
        }
    }
}
