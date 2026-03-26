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

package com.mongodb.client.model;

import com.mongodb.OperationFunctionalSpecification;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.Position;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.mongodb.client.model.Filters.geoIntersects;
import static com.mongodb.client.model.Filters.geoWithin;
import static com.mongodb.client.model.Filters.near;
import static com.mongodb.client.model.Filters.nearSphere;
import static com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem.CRS_84;
import static com.mongodb.client.model.geojson.NamedCoordinateReferenceSystem.EPSG_4326;
import static org.junit.jupiter.api.Assertions.assertEquals;

class GeoJsonFiltersFunctionalSpecificationTest extends OperationFunctionalSpecification {
    private final Document firstPoint = new Document("_id", 1)
            .append("geo", Document.parse(new Point(CRS_84, new Position(1d, 1d)).toJson()));
    private final Document secondPoint = new Document("_id", 2)
            .append("geo", Document.parse(new Point(EPSG_4326, new Position(2d, 2d)).toJson()));
    private final Document thirdPoint = new Document("_id", 3)
            .append("geo", Document.parse(new Point(new Position(3d, 3d)).toJson()));
    private final Document firstPolygon = new Document("_id", 4)
            .append("geo", Document.parse(new Polygon(Arrays.asList(
                    new Position(2d, 2d), new Position(6d, 2d),
                    new Position(6d, 6d), new Position(2d, 6d),
                    new Position(2d, 2d))).toJson()));

    @BeforeEach
    public void setUp() {
        super.setUp();
        getCollectionHelper().createIndex(new Document("geo", "2dsphere"));
        getCollectionHelper().insertDocuments(firstPoint, secondPoint, thirdPoint, firstPolygon);
    }

    private List<Document> find(final Bson filter) {
        return getCollectionHelper().find(filter, new Document("_id", 1));
    }

    @Test
    void geoWithinTest() {
        Polygon polygon = new Polygon(Arrays.asList(
                new Position(0d, 0d), new Position(4d, 0d),
                new Position(4d, 4d), new Position(0d, 4d),
                new Position(0d, 0d)));
        assertEquals(Arrays.asList(firstPoint, secondPoint, thirdPoint), find(geoWithin("geo", polygon)));
    }

    @Test
    void geoIntersectsTest() {
        Polygon polygon = new Polygon(Arrays.asList(
                new Position(0d, 0d), new Position(4d, 0d),
                new Position(4d, 4d), new Position(0d, 4d),
                new Position(0d, 0d)));
        assertEquals(Arrays.asList(firstPoint, secondPoint, thirdPoint, firstPolygon),
                find(geoIntersects("geo", polygon)));
    }

    @Test
    void nearTest() {
        assertEquals(Collections.singletonList(firstPoint),
                find(near("geo", new Point(new Position(1.01d, 1.01d)), 10000d, null)));
    }

    @Test
    void nearSphereTest() {
        assertEquals(Collections.singletonList(firstPoint),
                find(nearSphere("geo", new Point(new Position(1.01d, 1.01d)), 10000d, null)));
    }
}
