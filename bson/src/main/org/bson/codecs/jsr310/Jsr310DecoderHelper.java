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

package org.bson.codecs.jsr310;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.codecs.configuration.CodecConfigurationException;

import static java.lang.String.format;

final class Jsr310DecoderHelper {

    static void validateIsDecimal128(final BsonReader reader, final Class<?> clazz) {
        validateType(BsonType.DECIMAL128, reader, clazz);
    }

    static void validateIsDateTime(final BsonReader reader, final Class<?> clazz) {
        validateType(BsonType.DATE_TIME, reader, clazz);
    }

    static void validateIsString(final BsonReader reader, final Class<?> clazz) {
        validateType(BsonType.STRING, reader, clazz);
    }

    static void validateIsNumeric(final BsonReader reader, final BsonType expectedType, final Class<?> clazz) {
        BsonType currentType = getCurrentBsonType(reader);
        switch (currentType) {
                case INT32:
                case INT64:
                case DOUBLE:
                    break;
                default:
                    throw createException(expectedType, currentType, clazz);
        }
    }

    private static void validateType(final BsonType expectedType, final BsonReader reader, final Class<?> clazz) {
        BsonType currentType = getCurrentBsonType(reader);
        if (!currentType.equals(expectedType)) {
            throw createException(expectedType, currentType, clazz);
        }
    }

    private static BsonType getCurrentBsonType(final BsonReader reader) {
        BsonType currentType = reader.getCurrentBsonType();
        if (currentType == null) {
            currentType = reader.readBsonType();
        }
        return currentType;
    }

    private static CodecConfigurationException createException(final BsonType expectedType, final BsonType actualType,
                                                               final Class<?> clazz) {
        return new CodecConfigurationException(format("Could not decode into %s, expected '%s' BsonType but got '%s'.",
                clazz.getSimpleName(), expectedType, actualType));
    }

    private Jsr310DecoderHelper() {
    }
}
