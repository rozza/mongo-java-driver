/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Transformer;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.HashMap;
import java.util.Map;

/**
 * A Codec for Map instances.
 *
 * @since 3.5
 */
public class MapCodec extends AbstractMapCodec<Map<String, Object>> {

    /**
     * Construct a new instance with a default {@code CodecRegistry}
     */
    public MapCodec() {
        super();
    }

    /**
     Construct a new instance with the given registry
     *
     * @param registry the registry
     */
    public MapCodec(final CodecRegistry registry) {
        super(registry);
    }

    /**
     * Construct a new instance with the given registry and BSON type class map.
     *
     * @param registry         the registry
     * @param bsonTypeClassMap the BSON type class map
     */
    public MapCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap) {
        super(registry, bsonTypeClassMap);
    }

    /**
     * Construct a new instance with the given registry and BSON type class map. The transformer is applied as a last step when decoding
     * values, which allows users of this codec to control the decoding process.  For example, a user of this class could substitute a
     * value decoded as a Document with an instance of a special purpose class (e.g., one representing a DBRef in MongoDB).
     *
     * @param registry         the registry
     * @param bsonTypeClassMap the BSON type class map
     * @param valueTransformer the value transformer to use as a final step when decoding the value of any field in the map
     */
    public MapCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap, final Transformer valueTransformer) {
        super(registry, bsonTypeClassMap, valueTransformer);
    }

    @Override
    Map<String, Object> newInstance() {
        return new HashMap<String, Object>();
    }

    @Override
    void beforeFields(final BsonWriter bsonWriter, final EncoderContext encoderContext, final Map<String, Object> map) {
    }

    @Override
    boolean skipField(final EncoderContext encoderContext, final String key) {
        return false;
    }

    @Override
    public void encode(final BsonWriter writer, final Map<String, Object> map, final EncoderContext encoderContext) {
        super.encodeMap(writer, map, encoderContext);
    }

    @Override
    public Map<String, Object> decode(final BsonReader reader, final DecoderContext decoderContext) {
        return super.decodeMap(reader, decoderContext);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<Map<String, Object>> getEncoderClass() {
        return (Class<Map<String, Object>>) ((Class) Map.class);
    }

}
