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

package org.bson;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static org.bson.AbstractBsonReader.State.DONE;
import static org.bson.AbstractBsonReader.State.TYPE;
import static org.bson.BsonHelper.toBson;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BsonBinaryReaderUnitTest {

    @ParameterizedTest
    @MethodSource("values")
    @DisplayName("should skip value")
    void shouldSkipValue(final BsonValue value) {
        BsonDocument document = new BsonDocument("name", value);
        BsonBinaryReader reader = new BsonBinaryReader(toBson(document));
        reader.readStartDocument();
        reader.readBsonType();

        reader.skipName();
        reader.skipValue();
        assertEquals(TYPE, reader.getState());

        reader.readEndDocument();
        assertEquals(DONE, reader.getState());
    }

    private static List<BsonValue> values() {
        return BsonHelper.valuesOfEveryType();
    }
}
