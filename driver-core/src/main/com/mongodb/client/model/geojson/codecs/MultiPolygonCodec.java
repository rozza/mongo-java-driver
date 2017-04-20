/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model.geojson.codecs;

import com.mongodb.client.model.geojson.MultiPolygon;
import org.bson.codecs.configuration.CodecRegistry;

/**
 * A Codec for a GeoJSON MultiPolygon.
 *
 * @since 3.1
 */
public class MultiPolygonCodec extends AbstractGeometryCodec<MultiPolygon> {

    /**
     * Constructs an instance.
     *
     * @param registry the registry
     */
    public MultiPolygonCodec(final CodecRegistry registry) {
        super(registry);
    }

    @Override
    public Class<MultiPolygon> getEncoderClass() {
        return MultiPolygon.class;
    }

}
