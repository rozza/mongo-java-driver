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

package com.mongodb;

import com.mongodb.internal.operation.MapReduceBatchCursor;
import com.mongodb.internal.operation.MapReduceStatistics;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("deprecation")
class MapReduceOutputTest extends FunctionalSpecification {

    @Test
    @DisplayName("should return the name of the collection the results are contained in if it is not inline")
    void shouldReturnCollectionNameWhenNotInline() {
        String expectedCollectionName = "collectionForResults";
        DBCollection outputCollection = database.getCollection(expectedCollectionName);
        DBCursor results = outputCollection.find();

        MapReduceOutput mapReduceOutput = new MapReduceOutput(new BasicDBObject(), results, null, outputCollection);

        String collectionName = mapReduceOutput.getCollectionName();
        assertNotNull(collectionName);
        assertEquals(expectedCollectionName, collectionName);
    }

    @Test
    @DisplayName("should return null for the name of the collection if it is inline")
    void shouldReturnNullCollectionNameWhenInline() {
        @SuppressWarnings("unchecked")
        MapReduceBatchCursor<DBObject> mongoCursor = mock(MapReduceBatchCursor.class);

        MapReduceOutput mapReduceOutput = new MapReduceOutput(new BasicDBObject(), mongoCursor);

        assertNull(mapReduceOutput.getCollectionName());
    }

    @Test
    @DisplayName("should return the name of the database the results are contained in if it is not inline")
    void shouldReturnDatabaseNameWhenNotInline() {
        String expectedDatabaseName = getDatabaseName();
        String expectedCollectionName = "collectionForResults";
        DBCollection outputCollection = database.getCollection(expectedCollectionName);
        DBCursor results = outputCollection.find();

        MapReduceOutput mapReduceOutput = new MapReduceOutput(new BasicDBObject(), results, null, outputCollection);

        String databaseName = mapReduceOutput.getDatabaseName();
        assertNotNull(databaseName);
        assertEquals(expectedDatabaseName, databaseName);
    }

    @Test
    @DisplayName("should return the duration for a map-reduce into a collection")
    void shouldReturnDurationForCollectionMapReduce() {
        int expectedDuration = 2774;

        MapReduceStatistics mapReduceStats = mock(MapReduceStatistics.class);
        when(mapReduceStats.getDuration()).thenReturn(expectedDuration);

        MapReduceOutput mapReduceOutput = new MapReduceOutput(new BasicDBObject(), null, mapReduceStats, null);

        assertEquals(expectedDuration, mapReduceOutput.getDuration());
    }

    @Test
    @DisplayName("should return the duration for an inline map-reduce")
    void shouldReturnDurationForInlineMapReduce() {
        int expectedDuration = 2774;

        @SuppressWarnings("unchecked")
        MapReduceBatchCursor<DBObject> mongoCursor = mock(MapReduceBatchCursor.class);
        when(mongoCursor.getStatistics()).thenReturn(new MapReduceStatistics(5, 10, 5, expectedDuration));

        MapReduceOutput mapReduceOutput = new MapReduceOutput(new BasicDBObject(), mongoCursor);

        assertEquals(expectedDuration, mapReduceOutput.getDuration());
    }

    @Test
    @DisplayName("should return the count values for a map-reduce into a collection")
    void shouldReturnCountValuesForCollectionMapReduce() {
        int expectedInputCount = 3;
        int expectedOutputCount = 4;
        int expectedEmitCount = 6;

        MapReduceStatistics mapReduceStats = new MapReduceStatistics(expectedInputCount, expectedOutputCount, expectedEmitCount, 5);

        MapReduceOutput mapReduceOutput = new MapReduceOutput(new BasicDBObject(), null, mapReduceStats, null);

        assertEquals(expectedInputCount, mapReduceOutput.getInputCount());
        assertEquals(expectedOutputCount, mapReduceOutput.getOutputCount());
        assertEquals(expectedEmitCount, mapReduceOutput.getEmitCount());
    }

    @Test
    @DisplayName("should return the count values for an inline map-reduce output")
    void shouldReturnCountValuesForInlineMapReduce() {
        int expectedInputCount = 3;
        int expectedOutputCount = 4;
        int expectedEmitCount = 6;
        int expectedDuration = 10;

        @SuppressWarnings("unchecked")
        MapReduceBatchCursor<DBObject> mapReduceCursor = mock(MapReduceBatchCursor.class);
        when(mapReduceCursor.getStatistics()).thenReturn(
                new MapReduceStatistics(expectedInputCount, expectedOutputCount, expectedEmitCount, expectedDuration));

        MapReduceOutput mapReduceOutput = new MapReduceOutput(new BasicDBObject(), mapReduceCursor);

        assertEquals(expectedInputCount, mapReduceOutput.getInputCount());
        assertEquals(expectedOutputCount, mapReduceOutput.getOutputCount());
        assertEquals(expectedEmitCount, mapReduceOutput.getEmitCount());
    }
}
