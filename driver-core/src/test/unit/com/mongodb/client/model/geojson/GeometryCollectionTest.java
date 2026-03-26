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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GeometryCollectionTest {

    private final List<Geometry> geometries = Arrays.asList(
            new Point(new Position(1d, 2d)),
            new Point(new Position(2d, 2d)));

    @SuppressWarnings("unused")
    @Test
    void constructorShouldAcceptListsContainingSubtypeOfGeometry() {
        List<Point> points = Arrays.asList(new Point(new Position(1d, 2d)), new Point(new Position(2d, 2d)));
        GeometryCollection gc = new GeometryCollection(points);
        assertNotNull(gc);
    }

    @Test
    void constructorShouldSetGeometries() {
        assertEquals(geometries, new GeometryCollection(geometries).getGeometries());
    }

    @Test
    void constructorsShouldThrowIfPreconditionsAreViolated() {
        assertThrows(IllegalArgumentException.class, () -> new GeometryCollection(null));
        assertThrows(IllegalArgumentException.class, () ->
                new GeometryCollection(Arrays.asList(new Point(new Position(1d, 2d)), null)));
    }

    @Test
    void shouldGetType() {
        assertEquals(GeoJsonObjectType.GEOMETRY_COLLECTION, new GeometryCollection(geometries).getType());
    }

    @Test
    void equalsHashcodeAndToStringShouldBeOverridden() {
        assertEquals(new GeometryCollection(geometries), new GeometryCollection(geometries));
        assertEquals(new GeometryCollection(geometries).hashCode(), new GeometryCollection(geometries).hashCode());
        assertEquals("GeometryCollection{geometries=[Point{coordinate=Position{values=[1.0, 2.0]}}, "
                + "Point{coordinate=Position{values=[2.0, 2.0]}}]}",
                new GeometryCollection(geometries).toString());
    }
}
