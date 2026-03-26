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

package com.mongodb.client.model.changestream;

import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class UpdateDescriptionTest {

    @Test
    void shouldCreateTheExpectedUpdateDescription() {
        // removedFields with null updatedFields and null truncatedArrays
        UpdateDescription desc1 = new UpdateDescription(Arrays.asList("a", "b"), null, null, null);
        assertEquals(Arrays.asList("a", "b"), desc1.getRemovedFields());
        assertNull(desc1.getUpdatedFields());
        assertEquals(Collections.emptyList(), desc1.getTruncatedArrays());
        assertNull(desc1.getDisambiguatedPaths());

        // null removedFields with updatedFields and empty truncatedArrays
        UpdateDescription desc2 = new UpdateDescription(null, BsonDocument.parse("{c: 1}"),
                Collections.emptyList(), null);
        assertNull(desc2.getRemovedFields());
        assertEquals(BsonDocument.parse("{c: 1}"), desc2.getUpdatedFields());
        assertEquals(Collections.emptyList(), desc2.getTruncatedArrays());
        assertNull(desc2.getDisambiguatedPaths());

        // All fields populated with truncatedArrays
        UpdateDescription desc3 = new UpdateDescription(Arrays.asList("a", "b"), BsonDocument.parse("{c: 1}"),
                Collections.singletonList(new TruncatedArray("d", 1)), null);
        assertEquals(Arrays.asList("a", "b"), desc3.getRemovedFields());
        assertEquals(BsonDocument.parse("{c: 1}"), desc3.getUpdatedFields());
        assertEquals(Collections.singletonList(new TruncatedArray("d", 1)), desc3.getTruncatedArrays());
        assertNull(desc3.getDisambiguatedPaths());

        // All fields populated with disambiguatedPaths
        UpdateDescription desc4 = new UpdateDescription(Arrays.asList("a", "b"), BsonDocument.parse("{c: 1}"),
                Collections.singletonList(new TruncatedArray("d", 1)), BsonDocument.parse("{e: 1}"));
        assertEquals(Arrays.asList("a", "b"), desc4.getRemovedFields());
        assertEquals(BsonDocument.parse("{c: 1}"), desc4.getUpdatedFields());
        assertEquals(Collections.singletonList(new TruncatedArray("d", 1)), desc4.getTruncatedArrays());
        assertEquals(BsonDocument.parse("{e: 1}"), desc4.getDisambiguatedPaths());
    }
}
