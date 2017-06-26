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
import static org.bson.assertions.Assertions.isTrueArgument;

final class NumberCodecHelper {

    @SuppressWarnings("unchecked")
    static <T extends Number> T decodeNumber(final BsonReader reader, final Class<T> clazz) {
        isTrueArgument("Clazz must be Integer, Long or Double", clazz == Integer.class || clazz == Long.class || clazz == Double.class);
        Number number;
        T decodedNumber;
        BsonType bsonType = reader.getCurrentBsonType();
        switch (bsonType) {
            case DOUBLE:
                number = reader.readDouble();
                Double doubleValue = number.doubleValue();
                Double convertedDouble = number.doubleValue();

                if (clazz == Integer.class) {
                    decodedNumber = (T) (Integer) doubleValue.intValue();
                    convertedDouble = ((Integer) doubleValue.intValue()).doubleValue();
                } else if (clazz == Long.class) {
                    decodedNumber = (T) (Long) doubleValue.longValue();
                    convertedDouble = ((Long) doubleValue.longValue()).doubleValue();
                } else {
                    decodedNumber = (T) doubleValue;
                }

                if (!doubleValue.equals(convertedDouble)) {
                    throw invalidConversion(clazz, number);
                }
                break;
            case INT64:
                number = reader.readInt64();
                Long longValue = number.longValue();
                Long convertedLong = number.longValue();

                if (clazz == Integer.class) {
                    decodedNumber = (T) (Integer) longValue.intValue();
                    convertedLong = ((Integer) longValue.intValue()).longValue();
                } else if (clazz == Double.class) {
                    decodedNumber = (T) (Double) longValue.doubleValue();
                    convertedLong = ((Double) longValue.doubleValue()).longValue();
                } else {
                    decodedNumber = (T) longValue;
                }

                if (!longValue.equals(convertedLong)) {
                    throw invalidConversion(clazz, number);
                }
                break;
            case INT32:
                number = reader.readInt32();
                Integer intValue = number.intValue();

                if (clazz == Double.class) {
                    decodedNumber = (T) (Double) intValue.doubleValue();
                } else if (clazz == Long.class) {
                    decodedNumber = (T) (Long) intValue.longValue();
                } else {
                    decodedNumber = (T) intValue;
                }
                break;
            default:
                throw new BsonInvalidOperationException(format("Invalid numeric type, found: %s", bsonType));
        }
        return decodedNumber;
    }

    private static  <T extends Number> BsonInvalidOperationException invalidConversion(final Class<T> clazz, final Number number) {
        return new BsonInvalidOperationException(format("Could not convert number (%s) to %s without losing precision", number, clazz));
    }

    private NumberCodecHelper() {
    }
}
