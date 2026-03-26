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
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.mongodb.client.model.Filters;
import static org.junit.jupiter.api.Assertions.assertEquals;

class GeoFiltersFunctionalSpecificationTest extends OperationFunctionalSpecification {
    private final Document firstPoint = new Document("_id", 1).append("geo", Arrays.asList(1d, 1d));
    private final Document secondPoint = new Document("_id", 2).append("geo", Arrays.asList(45d, 2d));
    private final Document thirdPoint = new Document("_id", 3).append("geo", Arrays.asList(3d, 3d));

    @BeforeEach
    public void setUp() {
        super.setUp();
        getCollectionHelper().createIndex(new Document("geo", "2d"));
        getCollectionHelper().insertDocuments(firstPoint, secondPoint, thirdPoint);
    }

    private List<Document> find(final Bson filter) {
        return getCollectionHelper().find(filter, new Document("_id", 1));
    }

    @Test
    void testNear() {
        assertEquals(Collections.singletonList(firstPoint), find(Filters.near("geo", 1.01d, 1.01d, Double.valueOf(0.1d), Double.valueOf(0.0d))));
    }

    @Test
    void testNearSphere() {
        assertEquals(Arrays.asList(firstPoint, thirdPoint), find(Filters.nearSphere("geo", 1.01d, 1.01d, Double.valueOf(0.1d), Double.valueOf(0.0d))));
    }

    @Test
    void testGeoWithinBox() {
        assertEquals(Arrays.asList(firstPoint, thirdPoint), find(Filters.geoWithinBox("geo", 0.0d, 0.0d, 4.0d, 4.0d)));
    }

    @Test
    void testGeoWithinPolygon() {
        assertEquals(Arrays.asList(firstPoint, thirdPoint),
                find(Filters.geoWithinPolygon("geo", Arrays.asList(
                        Arrays.asList(0.0d, 0.0d), Arrays.asList(0.0d, 4.0d),
                        Arrays.asList(4.0d, 4.0d), Arrays.asList(4.0d, 0.0d)))));
    }

    @Test
    void testGeoWithinCenter() {
        assertEquals(Arrays.asList(firstPoint, thirdPoint), find(Filters.geoWithinCenter("geo", 2.0d, 2.0d, 4.0d)));
    }

    @Test
    void testGeoWithinCenterSphere() {
        assertEquals(Arrays.asList(firstPoint, secondPoint, thirdPoint),
                find(Filters.geoWithinCenterSphere("geo", 2.0d, 2.0d, 4.0d)));
    }
}
