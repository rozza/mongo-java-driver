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

package com.mongodb.client.model.geojson;

import org.junit.jupiter.api.Test;

import static com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem.EPSG_4326_STRICT_WINDING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PointTest {

    @Test
    void constructorShouldSetCoordinates() {
        assertEquals(new Position(1.0d, 2.0d), new Point(new Position(1.0d, 2.0d)).getCoordinates());
        assertEquals(new Position(1.0d, 2.0d), new Point(new Position(1.0d, 2.0d)).getPosition());
    }

    @Test
    void constructorShouldSetCoordinateReferenceSystem() {
        assertNull(new Point(new Position(1.0d, 2.0d)).getCoordinateReferenceSystem());
        assertEquals(EPSG_4326_STRICT_WINDING,
                new Point(EPSG_4326_STRICT_WINDING, new Position(1.0d, 2.0d)).getCoordinateReferenceSystem());
    }

    @Test
    void constructorsShouldThrowIfPreconditionsAreViolated() {
        assertThrows(IllegalArgumentException.class, () -> new Point(null));
        assertThrows(IllegalArgumentException.class, () -> new Point(EPSG_4326_STRICT_WINDING, null));
    }

    @Test
    void shouldGetType() {
        assertEquals(GeoJsonObjectType.POINT, new Point(new Position(1.0d, 2.0d)).getType());
    }

    @Test
    void equalsHashcodeAndToStringShouldBeOverridden() {
        assertEquals(new Point(new Position(1.0d, 2.0d)), new Point(new Position(1.0d, 2.0d)));
        assertEquals(new Point(new Position(1.0d, 2.0d)).hashCode(), new Point(new Position(1.0d, 2.0d)).hashCode());
        assertEquals("Point{coordinate=Position{values=[1.0, 2.0]}}", new Point(new Position(1.0d, 2.0d)).toString());
    }
}
