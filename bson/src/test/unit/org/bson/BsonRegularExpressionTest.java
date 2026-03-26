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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BsonRegularExpressionTest {

    @Test
    @DisplayName("should get type")
    void shouldGetType() {
        assertEquals(BsonType.REGULAR_EXPRESSION, new BsonRegularExpression("abc", "").getBsonType());
    }

    @Test
    @DisplayName("should sort options")
    void shouldSortOptions() {
        assertEquals("imsux", new BsonRegularExpression("abc", "uxsmi").getOptions());
    }

    @Test
    @DisplayName("should accept invalid options")
    void shouldAcceptInvalidOptions() {
        assertEquals("imsuwx", new BsonRegularExpression("abc", "uxsmiw").getOptions());
    }

    @Test
    @DisplayName("should allow null options")
    void shouldAllowNullOptions() {
        assertEquals("", new BsonRegularExpression("abc").getOptions());
        assertEquals("", new BsonRegularExpression("abc", null).getOptions());
    }

    @Test
    @DisplayName("should get regular expression")
    void shouldGetRegularExpression() {
        assertEquals("abc", new BsonRegularExpression("abc", null).getPattern());
    }

    @Test
    @DisplayName("equivalent values should be equal and have same hashcode")
    void equivalentValuesShouldBeEqualAndHaveSameHashcode() {
        BsonRegularExpression first = new BsonRegularExpression("abc", "uxsmi");
        BsonRegularExpression second = new BsonRegularExpression("abc", "imsxu");

        assertTrue(first.equals(second));
        assertEquals(first.hashCode(), second.hashCode());
    }

    @Test
    @DisplayName("should convert to string")
    void shouldConvertToString() {
        assertEquals("BsonRegularExpression{pattern='abc', options='imsux'}",
                new BsonRegularExpression("abc", "uxsmi").toString());
    }
}
