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

class PolygonTest {

    private final List<Position> exterior = Arrays.asList(
            new Position(Arrays.asList(40.0d, 18.0d)),
            new Position(Arrays.asList(40.0d, 19.0d)),
            new Position(Arrays.asList(41.0d, 19.0d)),
            new Position(Arrays.asList(40.0d, 18.0d)));
    private final PolygonCoordinates coordinates = new PolygonCoordinates(exterior);

    @Test
    void constructorShouldSetCoordinates() {
        assertEquals(coordinates, new Polygon(exterior).getCoordinates());
    }

    @Test
    void constructorShouldSetCoordinateReferenceSystem() {
        assertNull(new Polygon(exterior).getCoordinateReferenceSystem());
        assertEquals(EPSG_4326_STRICT_WINDING,
                new Polygon(EPSG_4326_STRICT_WINDING, coordinates).getCoordinateReferenceSystem());
    }

    @Test
    void constructorsShouldThrowIfPreconditionsAreViolated() {
        assertThrows(IllegalArgumentException.class, () -> new Polygon(null));

        // Only 3 positions (not closed ring)
        assertThrows(IllegalArgumentException.class, () ->
                new Polygon(Arrays.asList(
                        new Position(Arrays.asList(40.0d, 18.0d)),
                        new Position(Arrays.asList(40.0d, 19.0d)),
                        new Position(Arrays.asList(41.0d, 19.0d)))));

        // Contains null
        assertThrows(IllegalArgumentException.class, () ->
                new Polygon(Arrays.asList(
                        new Position(Arrays.asList(40.0d, 18.0d)),
                        new Position(Arrays.asList(40.0d, 19.0d)),
                        null)));

        // First and last positions don't match
        assertThrows(IllegalArgumentException.class, () ->
                new Polygon(Arrays.asList(
                        new Position(Arrays.asList(40.0d, 18.0d)),
                        new Position(Arrays.asList(40.0d, 19.0d)),
                        new Position(Arrays.asList(41.0d, 19.0d)),
                        new Position(1.0, 2.0))));

        // Null hole
        assertThrows(IllegalArgumentException.class, () ->
                new Polygon(exterior, (List<Position>) null));

        // Hole contains null
        assertThrows(IllegalArgumentException.class, () ->
                new Polygon(exterior, Arrays.asList(
                        new Position(Arrays.asList(40.0d, 18.0d)),
                        new Position(Arrays.asList(40.0d, 19.0d)),
                        null)));

        // Hole not closed ring
        assertThrows(IllegalArgumentException.class, () ->
                new Polygon(exterior, Arrays.asList(
                        new Position(Arrays.asList(40.0d, 18.0d)),
                        new Position(Arrays.asList(40.0d, 19.0d)),
                        new Position(Arrays.asList(41.0d, 19.0d)))));

        // Hole first and last don't match
        assertThrows(IllegalArgumentException.class, () ->
                new Polygon(exterior, Arrays.asList(
                        new Position(Arrays.asList(40.0d, 18.0d)),
                        new Position(Arrays.asList(40.0d, 19.0d)),
                        new Position(Arrays.asList(41.0d, 19.0d)),
                        new Position(1.0, 2.0))));
    }

    @Test
    void shouldGetType() {
        assertEquals(GeoJsonObjectType.POLYGON, new Polygon(exterior).getType());
    }

    @Test
    void equalsHashcodeAndToStringShouldBeOverridden() {
        assertEquals(new Polygon(exterior), new Polygon(exterior));
        assertEquals(new Polygon(exterior).hashCode(), new Polygon(exterior).hashCode());
        assertEquals("Polygon{exterior=[Position{values=[40.0, 18.0]}, "
                + "Position{values=[40.0, 19.0]}, "
                + "Position{values=[41.0, 19.0]}, "
                + "Position{values=[40.0, 18.0]}]}", new Polygon(exterior).toString());
    }
}
