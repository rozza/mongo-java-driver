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

import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem.EPSG_4326_STRICT_WINDING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LineStringTest {

    private final List<Position> coordinates = Arrays.asList(
            new Position(Arrays.asList(40.0d, 18.0d)),
            new Position(Arrays.asList(40.0d, 19.0d)),
            new Position(Arrays.asList(41.0d, 19.0d)),
            new Position(Arrays.asList(40.0d, 18.0d)));

    @Test
    void constructorShouldSetCoordinates() {
        assertEquals(coordinates, new LineString(coordinates).getCoordinates());
    }

    @Test
    void constructorShouldSetCoordinateReferenceSystem() {
        assertNull(new LineString(coordinates).getCoordinateReferenceSystem());
        assertEquals(EPSG_4326_STRICT_WINDING,
                new LineString(EPSG_4326_STRICT_WINDING, coordinates).getCoordinateReferenceSystem());
    }

    @Test
    void constructorsShouldThrowIfPreconditionsAreViolated() {
        assertThrows(IllegalArgumentException.class, () -> new LineString(null));
        assertThrows(IllegalArgumentException.class, () ->
                new LineString(Arrays.asList(new Position(Arrays.asList(40.0d, 18.0d)))));
        assertThrows(IllegalArgumentException.class, () ->
                new LineString(Arrays.asList(
                        new Position(Arrays.asList(40.0d, 18.0d)),
                        new Position(Arrays.asList(40.0d, 19.0d)),
                        null)));
    }

    @Test
    void shouldGetType() {
        assertEquals(GeoJsonObjectType.LINE_STRING, new LineString(coordinates).getType());
    }

    @Test
    void equalsHashcodeAndToStringShouldBeOverridden() {
        assertEquals(new LineString(coordinates), new LineString(coordinates));
        assertEquals(new LineString(coordinates).hashCode(), new LineString(coordinates).hashCode());
        assertEquals("LineString{coordinates=[Position{values=[40.0, 18.0]}, "
                + "Position{values=[40.0, 19.0]}, "
                + "Position{values=[41.0, 19.0]}, "
                + "Position{values=[40.0, 18.0]}]}", new LineString(coordinates).toString());
    }
}
