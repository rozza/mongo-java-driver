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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TagSetTest {

    @Test
    @DisplayName("should iterate an empty tag set")
    void shouldIterateEmptyTagSet() {
        TagSet tagSet = new TagSet();
        assertFalse(tagSet.iterator().hasNext());
    }

    @Test
    @DisplayName("should iterate a tag set with a single tag")
    void shouldIterateSingleTag() {
        Tag tag = new Tag("dc", "ny");
        TagSet tagSet = new TagSet(tag);
        Iterator<Tag> iterator = tagSet.iterator();

        assertTrue(iterator.hasNext());
        assertEquals(tag, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    @DisplayName("should iterate a tag set with multiple tags")
    void shouldIterateMultipleTags() {
        Tag tagOne = new Tag("dc", "ny");
        Tag tagTwo = new Tag("rack", "1");
        TagSet tagSet = new TagSet(Arrays.asList(tagOne, tagTwo));
        Iterator<Tag> iterator = tagSet.iterator();

        assertTrue(iterator.hasNext());
        assertEquals(tagOne, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(tagTwo, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    @DisplayName("should throw on null argument")
    void shouldThrowOnNullArgument() {
        assertThrows(IllegalArgumentException.class, () -> new TagSet((Tag) null));
        assertThrows(IllegalArgumentException.class, () -> new TagSet((List<Tag>) null));
        assertThrows(IllegalArgumentException.class, () -> new TagSet(Arrays.asList(new Tag("dc", "ny"), null)));
    }

    @Test
    @DisplayName("should throw on duplicate tag name")
    void shouldThrowOnDuplicateTagName() {
        assertThrows(IllegalArgumentException.class,
                () -> new TagSet(Arrays.asList(new Tag("dc", "ny"), new Tag("dc", "ca"))));
    }

    @Test
    @DisplayName("should alphabetically order tags")
    void shouldAlphabeticallyOrderTags() {
        Tag pTag = new Tag("p", "1");
        Tag dcTag = new Tag("dc", "ny");
        TagSet tagSet = new TagSet(Arrays.asList(pTag, dcTag));
        Iterator<Tag> iter = tagSet.iterator();

        assertEquals(dcTag, iter.next());
        assertEquals(pTag, iter.next());
        assertFalse(iter.hasNext());
        assertEquals(new TagSet(Arrays.asList(dcTag, pTag)), tagSet);
    }
}
