/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs;

import org.bson.BsonInvalidOperationException;
import org.bson.BsonReader;
import org.bson.BsonType;

import static java.lang.String.format;

final class NumberCodecHelper {

    static final double DEFAULT_DELTA = 0.00000000000001d;

    @SuppressWarnings("unchecked")
    static <T extends Number> T decodeNumber(final BsonReader reader, final Class<T> clazz, final double delta) {
        Number number;
        boolean isADownConversion;
        BsonType bsonType = reader.getCurrentBsonType();
        switch (bsonType) {
            case DOUBLE:
                number = reader.readDouble();
                isADownConversion = clazz != Double.class;
                break;
            case INT64:
                number = reader.readInt64();
                isADownConversion = clazz == Integer.class;
                break;
            case INT32:
                number = reader.readInt32();
                isADownConversion = false;
                break;
            default:
                throw new BsonInvalidOperationException(format("Invalid numeric type, found: %s", bsonType));
        }

        Number converted;
        if (clazz == Double.class) {
            converted = number.doubleValue();
        } else if (clazz == Integer.class) {
            converted = number.intValue();
        } else if (clazz == Long.class) {
            converted = number.longValue();
        } else {
            throw new BsonInvalidOperationException(format("Unsupported numeric type, found: %s", clazz));
        }

        if (isADownConversion && hasLostPrecision(number.doubleValue(), converted.doubleValue(), delta)) {
            throw new BsonInvalidOperationException(format("Could not convert number (%s) to %s without losing precision", number, clazz));
        }

        return (T) converted;
    }

    private static <T extends Number> boolean hasLostPrecision(final double d1, final double d2, final double delta) {
        if (Double.compare(d1, d2) == 0) {
            return false;
        }
        if ((Math.abs(d1 - d2) <= delta)) {
            return false;
        }

        return true;
    }

    private NumberCodecHelper() {
    }
}
