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
package org.bson.codecs.pojo;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static java.lang.String.format;

final class MapPropertyCodecProvider implements PropertyCodecProvider {

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public <T> Codec<T> get(final TypeWithTypeParameters<T> type, final PropertyCodecRegistry registry) {
        if (Map.class.isAssignableFrom(type.getType()) && type.getTypeParameters().size() == 2) {
            Class<?> keyType = type.getTypeParameters().get(0).getType();
            if (!keyType.equals(String.class) && !keyType.isEnum()) {
                throw new CodecConfigurationException(format("Invalid Map type. Maps MUST have String or Enum keys,"
                        + " found %s instead.", keyType));
            }

            try {
                return new MapCodec(type.getType(), type.getTypeParameters().get(0).getType(),
                        registry.get(type.getTypeParameters().get(1)));
            } catch (CodecConfigurationException e) {
                if (type.getTypeParameters().get(1).getType() == Object.class) {
                    try {
                        return (Codec<T>) registry.get(TypeData.builder(Map.class).build());
                    } catch (CodecConfigurationException e1) {
                        // Ignore and return original exception
                    }
                }
                throw e;
            }
        } else {
            return null;
        }
    }

    private static class MapCodec<K, V> implements Codec<Map<K, V>> {
        private final Class<Map<K, V>> encoderClass;
        private final Class<K> keyClazz;
        private final Codec<V> valueCodec;
        private final boolean isEnum;

        MapCodec(final Class<Map<K, V>> encoderClass, final Class<K> keyClazz, final Codec<V> valueCodec) {
            this.encoderClass = encoderClass;
            this.keyClazz = keyClazz;
            this.valueCodec = valueCodec;
            this.isEnum = keyClazz.isEnum();
        }

        @Override
        public void encode(final BsonWriter writer, final Map<K, V> map, final EncoderContext encoderContext) {
            writer.writeStartDocument();
            for (final Entry<K, V> entry : map.entrySet()) {
                String keyValue = isEnum ? ((Enum<?>) entry.getKey()).name() : (String) entry.getKey();
                writer.writeName(keyValue);
                if (entry.getValue() == null) {
                    writer.writeNull();
                } else {
                    valueCodec.encode(writer, entry.getValue(), encoderContext);
                }
            }
            writer.writeEndDocument();
        }

        @Override
        public Map<K, V> decode(final BsonReader reader, final DecoderContext context) {
            reader.readStartDocument();
            Map<K, V> map = getInstance();
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                if (reader.getCurrentBsonType() == BsonType.NULL) {
                    map.put(getKey(reader.readName()), null);
                    reader.readNull();
                } else {
                    map.put(getKey(reader.readName()), valueCodec.decode(reader, context));
                }
            }
            reader.readEndDocument();
            return map;
        }

        @Override
        public Class<Map<K, V>> getEncoderClass() {
            return encoderClass;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private K getKey(final String keyName) {
            if (isEnum) {
                try {
                    return (K) Enum.valueOf((Class<Enum>) keyClazz, keyName);
                } catch (Exception e) {
                    throw new CodecConfigurationException(format("Invalid Map key value. The map has an Enum keys and value %s does not"
                            + " match any value for '%s'.", keyClazz, keyName));
                }
            } else {
                return (K) keyName;
            }
        }

        private Map<K, V> getInstance() {
            if (encoderClass.isInterface()) {
                return new HashMap<K, V>();
            }
            try {
                return encoderClass.getDeclaredConstructor().newInstance();
            } catch (final Exception e) {
                throw new CodecConfigurationException(e.getMessage(), e);
            }
        }
    }
}
