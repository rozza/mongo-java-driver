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

class MultiPolygonTest {

    private final List<Position> exteriorOne = Arrays.asList(
            new Position(Arrays.asList(40.0d, 18.0d)),
            new Position(Arrays.asList(40.0d, 19.0d)),
            new Position(Arrays.asList(41.0d, 19.0d)),
            new Position(Arrays.asList(40.0d, 18.0d)));
    private final PolygonCoordinates coordinatesOne = new PolygonCoordinates(exteriorOne);

    private final List<Position> exteriorTwo = Arrays.asList(
            new Position(Arrays.asList(80.0d, 18.0d)),
            new Position(Arrays.asList(80.0d, 19.0d)),
            new Position(Arrays.asList(81.0d, 19.0d)),
            new Position(Arrays.asList(80.0d, 18.0d)));
    private final PolygonCoordinates coordinatesTwo = new PolygonCoordinates(exteriorTwo);

    @Test
    void constructorShouldSetCoordinates() {
        assertEquals(Arrays.asList(coordinatesOne, coordinatesTwo),
                new MultiPolygon(Arrays.asList(coordinatesOne, coordinatesTwo)).getCoordinates());
    }

    @Test
    void constructorShouldSetCoordinateReferenceSystem() {
        assertNull(new MultiPolygon(Arrays.asList(coordinatesOne)).getCoordinateReferenceSystem());
        assertEquals(EPSG_4326_STRICT_WINDING,
                new MultiPolygon(EPSG_4326_STRICT_WINDING, Arrays.asList(coordinatesOne)).getCoordinateReferenceSystem());
    }

    @Test
    void constructorsShouldThrowIfPreconditionsAreViolated() {
        assertThrows(IllegalArgumentException.class, () -> new MultiPolygon(null));
        assertThrows(IllegalArgumentException.class, () -> new MultiPolygon(Arrays.asList(coordinatesOne, null)));
    }

    @Test
    void shouldGetType() {
        assertEquals(GeoJsonObjectType.MULTI_POLYGON, new MultiPolygon(Arrays.asList(coordinatesOne)).getType());
    }

    @Test
    void equalsHashcodeAndToStringShouldBeOverridden() {
        assertEquals(new MultiPolygon(Arrays.asList(coordinatesOne, coordinatesTwo)),
                new MultiPolygon(Arrays.asList(coordinatesOne, coordinatesTwo)));
        assertEquals(new MultiPolygon(Arrays.asList(coordinatesOne, coordinatesTwo)).hashCode(),
                new MultiPolygon(Arrays.asList(coordinatesOne, coordinatesTwo)).hashCode());
        assertEquals("MultiPolygon{coordinates=["
                + "PolygonCoordinates{exterior=[Position{values=[40.0, 18.0]}, Position{values=[40.0, 19.0]}, "
                + "Position{values=[41.0, 19.0]}, Position{values=[40.0, 18.0]}]}, "
                + "PolygonCoordinates{exterior=[Position{values=[80.0, 18.0]}, Position{values=[80.0, 19.0]}, "
                + "Position{values=[81.0, 19.0]}, Position{values=[80.0, 18.0]}]}]}",
                new MultiPolygon(Arrays.asList(coordinatesOne, coordinatesTwo)).toString());
    }
}
