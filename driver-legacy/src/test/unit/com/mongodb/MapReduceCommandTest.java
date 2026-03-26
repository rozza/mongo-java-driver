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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.mongodb.MapReduceCommand.OutputType;
import static com.mongodb.ReadPreference.primary;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MapReduceCommandTest {

    private static final String COLLECTION_NAME = "collectionName";

    private static BasicDBObject sort() {
        return new BasicDBObject("a", 1);
    }

    private static Map<String, Object> scope() {
        Map<String, Object> map = new HashMap<>();
        map.put("a", "b");
        return map;
    }

    private static MapReduceCommand createCommand() {
        DBCollection collection = mock(DBCollection.class);
        when(collection.getName()).thenReturn(COLLECTION_NAME);
        return new MapReduceCommand(collection, "map", "reduce", "test", OutputType.REDUCE, new BasicDBObject());
    }

    @Test
    @DisplayName("should have the correct defaults")
    void shouldHaveTheCorrectDefaults() {
        MapReduceCommand cmd = createCommand();
        assertNull(cmd.getFinalize());
        assertEquals(COLLECTION_NAME, cmd.getInput());
        assertNull(cmd.getJsMode());
        assertEquals(0, cmd.getLimit());
        assertEquals("map", cmd.getMap());
        assertEquals(0, cmd.getMaxTime(SECONDS));
        assertNull(cmd.getOutputDB());
        assertEquals("test", cmd.getOutputTarget());
        assertEquals(OutputType.REDUCE, cmd.getOutputType());
        assertEquals(new BasicDBObject(), cmd.getQuery());
        assertNull(cmd.getReadPreference());
        assertEquals("reduce", cmd.getReduce());
        assertNull(cmd.getScope());
        assertNull(cmd.getSort());
        assertTrue(cmd.isVerbose());
    }

    @Test
    @DisplayName("should be able to change the defaults")
    void shouldBeAbleToChangeTheDefaults() {
        MapReduceCommand cmd = createCommand();
        cmd.setFinalize("final");
        assertEquals("final", cmd.getFinalize());

        cmd.setJsMode(true);
        assertEquals(true, cmd.getJsMode());

        cmd.setLimit(100);
        assertEquals(100, cmd.getLimit());

        cmd.setMaxTime(1, SECONDS);
        assertEquals(1, cmd.getMaxTime(SECONDS));

        cmd.setOutputDB("outDB");
        assertEquals("outDB", cmd.getOutputDB());

        cmd.setReadPreference(primary());
        assertEquals(primary(), cmd.getReadPreference());

        cmd.setScope(scope());
        assertEquals(scope(), cmd.getScope());

        cmd.setSort(sort());
        assertEquals(sort(), cmd.getSort());

        cmd.setVerbose(false);
        assertEquals(false, cmd.isVerbose());
    }

    @Test
    @DisplayName("should produce the expected DBObject when changed")
    void shouldProduceTheExpectedDBObjectWhenChanged() {
        MapReduceCommand cmd = createCommand();
        cmd.setFinalize("final");
        cmd.setJsMode(true);
        cmd.setLimit(100);
        cmd.setMaxTime(1, SECONDS);
        cmd.setOutputDB("outDB");
        cmd.setReadPreference(primary());
        cmd.setScope(scope());
        cmd.setSort(sort());
        cmd.setVerbose(false);

        BasicDBObject expected = new BasicDBObject("mapreduce", COLLECTION_NAME)
                .append("map", "map")
                .append("reduce", "reduce")
                .append("verbose", false)
                .append("out", new BasicDBObject("reduce", "test").append("db", "outDB"))
                .append("query", new BasicDBObject())
                .append("finalize", "final")
                .append("sort", sort())
                .append("limit", 100)
                .append("scope", scope())
                .append("jsMode", true)
                .append("maxTimeMS", 1000L);

        assertEquals(expected, cmd.toDBObject());
    }
}
