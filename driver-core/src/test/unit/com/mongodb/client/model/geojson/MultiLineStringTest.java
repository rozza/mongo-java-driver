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

class MultiLineStringTest {

    private final List<List<Position>> coordinates = Arrays.asList(
            Arrays.asList(new Position(Arrays.asList(1.0d, 1.0d)),
                    new Position(Arrays.asList(2.0d, 2.0d)),
                    new Position(Arrays.asList(3.0d, 4.0d))),
            Arrays.asList(new Position(Arrays.asList(2.0d, 3.0d)),
                    new Position(Arrays.asList(3.0d, 2.0d)),
                    new Position(Arrays.asList(4.0d, 4.0d))));

    @Test
    void constructorShouldSetCoordinates() {
        assertEquals(coordinates, new MultiLineString(coordinates).getCoordinates());
    }

    @Test
    void constructorShouldSetCoordinateReferenceSystem() {
        assertNull(new MultiLineString(coordinates).getCoordinateReferenceSystem());
        assertEquals(EPSG_4326_STRICT_WINDING,
                new MultiLineString(EPSG_4326_STRICT_WINDING, coordinates).getCoordinateReferenceSystem());
    }

    @Test
    void constructorsShouldThrowIfPreconditionsAreViolated() {
        assertThrows(IllegalArgumentException.class, () -> new MultiLineString(null));
        assertThrows(IllegalArgumentException.class, () ->
                new MultiLineString(Arrays.asList(
                        Arrays.asList(new Position(Arrays.asList(40.0d, 18.0d)), new Position(Arrays.asList(40.0d, 19.0d))),
                        null)));
        assertThrows(IllegalArgumentException.class, () ->
                new MultiLineString(Arrays.asList(
                        Arrays.asList(new Position(Arrays.asList(40.0d, 18.0d)), new Position(Arrays.asList(40.0d, 19.0d)), null))));
    }

    @Test
    void shouldGetType() {
        assertEquals(GeoJsonObjectType.MULTI_LINE_STRING, new MultiLineString(coordinates).getType());
    }

    @Test
    void equalsHashcodeAndToStringShouldBeOverridden() {
        assertEquals(new MultiLineString(coordinates), new MultiLineString(coordinates));
        assertEquals(new MultiLineString(coordinates).hashCode(), new MultiLineString(coordinates).hashCode());
        assertEquals("MultiLineString{coordinates=["
                + "[Position{values=[1.0, 1.0]}, Position{values=[2.0, 2.0]}, Position{values=[3.0, 4.0]}], "
                + "[Position{values=[2.0, 3.0]}, Position{values=[3.0, 2.0]}, Position{values=[4.0, 4.0]}]]}",
                new MultiLineString(coordinates).toString());
    }
}
