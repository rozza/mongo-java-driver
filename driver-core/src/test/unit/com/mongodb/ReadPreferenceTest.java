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

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.bson.BsonDocument.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReadPreferenceTest {

    private static final TagSet TAG_SET = new TagSet(new Tag("rack", "1"));
    private static final List<TagSet> TAG_SET_LIST = Collections.singletonList(TAG_SET);
    private static final ReadPreferenceHedgeOptions HEDGE_OPTIONS =
            ReadPreferenceHedgeOptions.builder().enabled(true).build();

    @ParameterizedTest
    @MethodSource("correctNamesData")
    @DisplayName("should have correct names")
    void shouldHaveCorrectNames(final ReadPreference readPreference, final String name) {
        assertEquals(name, readPreference.getName());
    }

    static Stream<Object[]> correctNamesData() {
        return Stream.of(
                new Object[]{ReadPreference.primary(), "primary"},
                new Object[]{ReadPreference.primaryPreferred(), "primaryPreferred"},
                new Object[]{ReadPreference.secondary(), "secondary"},
                new Object[]{ReadPreference.secondaryPreferred(), "secondaryPreferred"},
                new Object[]{ReadPreference.nearest(), "nearest"}
        );
    }

    @ParameterizedTest
    @MethodSource("maxStalenessAndTagSetData")
    @DisplayName("should have correct max staleness and tag set list")
    void shouldHaveCorrectMaxStalenessAndTagSetList(final ReadPreference readPreference, final Long maxStalenessMS,
                                                     final List<TagSet> tagSetList,
                                                     final ReadPreferenceHedgeOptions hedgeOptions) {
        TaggableReadPreference taggable = (TaggableReadPreference) readPreference;
        assertEquals(maxStalenessMS, taggable.getMaxStaleness(MILLISECONDS));
        assertEquals(tagSetList, taggable.getTagSetList());
        assertEquals(hedgeOptions, taggable.getHedgeOptions());
    }

    static Stream<Object[]> maxStalenessAndTagSetData() {
        return Stream.of(
                new Object[]{ReadPreference.primaryPreferred(), null, emptyList(), null},
                new Object[]{ReadPreference.secondary(), null, emptyList(), null},
                new Object[]{ReadPreference.secondaryPreferred(), null, emptyList(), null},
                new Object[]{ReadPreference.nearest(), null, emptyList(), null},
                new Object[]{ReadPreference.secondary(10, SECONDS), 10000L, emptyList(), null},
                new Object[]{ReadPreference.secondaryPreferred(10, SECONDS), 10000L, emptyList(), null},
                new Object[]{ReadPreference.primaryPreferred(10, SECONDS), 10000L, emptyList(), null},
                new Object[]{ReadPreference.nearest(10, SECONDS), 10000L, emptyList(), null},
                new Object[]{ReadPreference.secondary(TAG_SET, 10, SECONDS), 10000L, TAG_SET_LIST, null},
                new Object[]{ReadPreference.secondaryPreferred(TAG_SET, 10, SECONDS), 10000L, TAG_SET_LIST, null},
                new Object[]{ReadPreference.primaryPreferred(TAG_SET, 10, SECONDS), 10000L, TAG_SET_LIST, null},
                new Object[]{ReadPreference.nearest(TAG_SET, 10, SECONDS), 10000L, TAG_SET_LIST, null},
                new Object[]{ReadPreference.secondary(TAG_SET_LIST, 10, SECONDS), 10000L, TAG_SET_LIST, null},
                new Object[]{ReadPreference.secondaryPreferred(TAG_SET_LIST, 10, SECONDS), 10000L, TAG_SET_LIST, null},
                new Object[]{ReadPreference.primaryPreferred(TAG_SET_LIST, 10, SECONDS), 10000L, TAG_SET_LIST, null},
                new Object[]{ReadPreference.nearest(TAG_SET_LIST, 10, SECONDS), 10000L, TAG_SET_LIST, null},
                new Object[]{ReadPreference.secondary().withMaxStalenessMS(10L, SECONDS), 10000L, emptyList(), null},
                new Object[]{ReadPreference.secondaryPreferred().withMaxStalenessMS(10L, SECONDS), 10000L, emptyList(), null},
                new Object[]{ReadPreference.primaryPreferred().withMaxStalenessMS(10L, SECONDS), 10000L, emptyList(), null},
                new Object[]{ReadPreference.nearest().withMaxStalenessMS(10L, SECONDS), 10000L, emptyList(), null},
                new Object[]{ReadPreference.secondary().withHedgeOptions(HEDGE_OPTIONS), null, emptyList(), HEDGE_OPTIONS},
                new Object[]{ReadPreference.secondaryPreferred().withHedgeOptions(HEDGE_OPTIONS), null, emptyList(),
                        HEDGE_OPTIONS},
                new Object[]{ReadPreference.primaryPreferred().withHedgeOptions(HEDGE_OPTIONS), null, emptyList(),
                        HEDGE_OPTIONS},
                new Object[]{ReadPreference.nearest().withHedgeOptions(HEDGE_OPTIONS), null, emptyList(), HEDGE_OPTIONS},
                new Object[]{ReadPreference.secondary().withTagSet(TAG_SET).withMaxStalenessMS(10L, SECONDS)
                        .withHedgeOptions(HEDGE_OPTIONS), 10000L, TAG_SET_LIST, HEDGE_OPTIONS},
                new Object[]{ReadPreference.secondaryPreferred().withTagSet(TAG_SET).withMaxStalenessMS(10L, SECONDS)
                        .withHedgeOptions(HEDGE_OPTIONS), 10000L, TAG_SET_LIST, HEDGE_OPTIONS},
                new Object[]{ReadPreference.primaryPreferred().withTagSet(TAG_SET).withMaxStalenessMS(10L, SECONDS)
                        .withHedgeOptions(HEDGE_OPTIONS), 10000L, TAG_SET_LIST, HEDGE_OPTIONS},
                new Object[]{ReadPreference.nearest().withTagSet(TAG_SET).withMaxStalenessMS(10L, SECONDS)
                        .withHedgeOptions(HEDGE_OPTIONS), 10000L, TAG_SET_LIST, HEDGE_OPTIONS}
        );
    }

    @Test
    @DisplayName("should throw if max staleness is negative")
    void shouldThrowIfMaxStalenessIsNegative() {
        assertThrows(IllegalArgumentException.class, () -> ReadPreference.secondary(-1, SECONDS));
        assertThrows(IllegalArgumentException.class, () -> ReadPreference.secondary().withMaxStalenessMS(-1L, SECONDS));
    }

    @Test
    @DisplayName("should have correct valueOf")
    void shouldHaveCorrectValueOf() {
        assertEquals(ReadPreference.primary(), ReadPreference.valueOf("primary"));
        assertEquals(ReadPreference.secondary(), ReadPreference.valueOf("secondary"));
        assertEquals(ReadPreference.primaryPreferred(), ReadPreference.valueOf("primaryPreferred"));
        assertEquals(ReadPreference.secondaryPreferred(), ReadPreference.valueOf("secondaryPreferred"));
        assertEquals(ReadPreference.nearest(), ReadPreference.valueOf("nearest"));
    }

    @Test
    @DisplayName("valueOf should throw with null name")
    void valueOfShouldThrowWithNullName() {
        assertThrows(IllegalArgumentException.class, () -> ReadPreference.valueOf(null));
        assertThrows(IllegalArgumentException.class,
                () -> ReadPreference.valueOf(null, asList(new TagSet(new Tag("dc", "ny")))));
    }

    @Test
    @DisplayName("valueOf should throw with unexpected name")
    void valueOfShouldThrowWithUnexpectedName() {
        assertThrows(IllegalArgumentException.class, () -> ReadPreference.valueOf("unknown"));
        assertThrows(IllegalArgumentException.class,
                () -> ReadPreference.valueOf(null, asList(new TagSet(new Tag("dc", "ny")))));
        assertThrows(IllegalArgumentException.class,
                () -> ReadPreference.valueOf("primary", asList(new TagSet(new Tag("dc", "ny")))));
    }

    @Test
    @DisplayName("should have correct valueOf with tag set list")
    void shouldHaveCorrectValueOfWithTagSetList() {
        List<TagSet> tags = Arrays.asList(
                new TagSet(asList(new Tag("dy", "ny"), new Tag("rack", "1"))),
                new TagSet(asList(new Tag("dy", "ca"), new Tag("rack", "2"))));

        assertEquals(ReadPreference.secondary(tags), ReadPreference.valueOf("secondary", tags));
        assertEquals(ReadPreference.primaryPreferred(tags), ReadPreference.valueOf("primaryPreferred", tags));
        assertEquals(ReadPreference.secondaryPreferred(tags), ReadPreference.valueOf("secondaryPreferred", tags));
        assertEquals(ReadPreference.nearest(tags), ReadPreference.valueOf("nearest", tags));
    }

    @Test
    @DisplayName("should have correct valueOf with max staleness")
    void shouldHaveCorrectValueOfWithMaxStaleness() {
        assertEquals(ReadPreference.secondary(10, SECONDS),
                ReadPreference.valueOf("secondary", emptyList(), 10, SECONDS));
    }

    @ParameterizedTest
    @MethodSource("maxStalenessDocumentData")
    @DisplayName("should convert read preference with max staleness to correct documents")
    void shouldConvertMaxStalenessToDocuments(final ReadPreference readPreference, final BsonDocument document) {
        assertEquals(document, readPreference.toDocument());
    }

    static Stream<Object[]> maxStalenessDocumentData() {
        return Stream.of(
                new Object[]{ReadPreference.primaryPreferred(10, SECONDS),
                        parse("{mode : \"primaryPreferred\", maxStalenessSeconds : {$numberLong : \"10\" }}")},
                new Object[]{ReadPreference.secondary(10, SECONDS),
                        parse("{mode : \"secondary\", maxStalenessSeconds : {$numberLong : \"10\" }}")},
                new Object[]{ReadPreference.secondaryPreferred(10, SECONDS),
                        parse("{mode : \"secondaryPreferred\", maxStalenessSeconds : {$numberLong : \"10\" }}")},
                new Object[]{ReadPreference.nearest(10, SECONDS),
                        parse("{mode : \"nearest\", maxStalenessSeconds : {$numberLong : \"10\" }}")},
                new Object[]{ReadPreference.nearest(10005, MILLISECONDS),
                        parse("{mode : \"nearest\", maxStalenessSeconds : {$numberLong : \"10\" }}")},
                new Object[]{ReadPreference.primaryPreferred().withMaxStalenessMS(10L, SECONDS),
                        parse("{mode : \"primaryPreferred\", maxStalenessSeconds : {$numberLong : \"10\" }}")},
                new Object[]{ReadPreference.secondary().withMaxStalenessMS(10L, SECONDS),
                        parse("{mode : \"secondary\", maxStalenessSeconds : {$numberLong : \"10\" }}")},
                new Object[]{ReadPreference.secondaryPreferred().withMaxStalenessMS(10L, SECONDS),
                        parse("{mode : \"secondaryPreferred\", maxStalenessSeconds : {$numberLong : \"10\" }}")},
                new Object[]{ReadPreference.nearest().withMaxStalenessMS(10L, SECONDS),
                        parse("{mode : \"nearest\", maxStalenessSeconds : {$numberLong : \"10\" }}")},
                new Object[]{ReadPreference.nearest().withMaxStalenessMS(10005L, MILLISECONDS),
                        parse("{mode : \"nearest\", maxStalenessSeconds : {$numberLong : \"10\" }}")}
        );
    }

    @ParameterizedTest
    @MethodSource("hedgeOptionsDocumentData")
    @DisplayName("should convert read preference with hedge options to correct documents")
    void shouldConvertHedgeOptionsToDocuments(final ReadPreference readPreference, final BsonDocument document) {
        assertEquals(document, readPreference.toDocument());
    }

    static Stream<Object[]> hedgeOptionsDocumentData() {
        return Stream.of(
                new Object[]{ReadPreference.primaryPreferred().withHedgeOptions(HEDGE_OPTIONS),
                        parse("{mode : \"primaryPreferred\", hedge : { enabled : true }}")},
                new Object[]{ReadPreference.secondary().withHedgeOptions(HEDGE_OPTIONS),
                        parse("{mode : \"secondary\", hedge : { enabled : true }}")},
                new Object[]{ReadPreference.secondaryPreferred().withHedgeOptions(HEDGE_OPTIONS),
                        parse("{mode : \"secondaryPreferred\", hedge : { enabled : true }}")},
                new Object[]{ReadPreference.nearest().withHedgeOptions(HEDGE_OPTIONS),
                        parse("{mode : \"nearest\", hedge : { enabled : true }}")}
        );
    }

    @ParameterizedTest
    @MethodSource("singleTagSetDocumentData")
    @DisplayName("should convert read preferences with a single tag set to correct documents")
    void shouldConvertSingleTagSetToDocuments(final ReadPreference readPreference, final BsonDocument document) {
        assertEquals(document, readPreference.toDocument());
    }

    static Stream<Object[]> singleTagSetDocumentData() {
        BsonArray tagsArray = new BsonArray(Collections.singletonList(
                new BsonDocument("dc", new BsonString("ny")).append("rack", new BsonString("1"))));
        return Stream.of(
                new Object[]{ReadPreference.primary(), new BsonDocument("mode", new BsonString("primary"))},
                new Object[]{ReadPreference.primaryPreferred(),
                        new BsonDocument("mode", new BsonString("primaryPreferred"))},
                new Object[]{ReadPreference.secondary(), new BsonDocument("mode", new BsonString("secondary"))},
                new Object[]{ReadPreference.secondaryPreferred(),
                        new BsonDocument("mode", new BsonString("secondaryPreferred"))},
                new Object[]{ReadPreference.nearest(), new BsonDocument("mode", new BsonString("nearest"))},
                new Object[]{ReadPreference.primaryPreferred(
                        new TagSet(asList(new Tag("dc", "ny"), new Tag("rack", "1")))),
                        new BsonDocument("mode", new BsonString("primaryPreferred")).append("tags", tagsArray)},
                new Object[]{ReadPreference.secondary(
                        new TagSet(asList(new Tag("dc", "ny"), new Tag("rack", "1")))),
                        new BsonDocument("mode", new BsonString("secondary")).append("tags", tagsArray)},
                new Object[]{ReadPreference.secondaryPreferred(
                        new TagSet(asList(new Tag("dc", "ny"), new Tag("rack", "1")))),
                        new BsonDocument("mode", new BsonString("secondaryPreferred")).append("tags", tagsArray)},
                new Object[]{ReadPreference.nearest(
                        new TagSet(asList(new Tag("dc", "ny"), new Tag("rack", "1")))),
                        new BsonDocument("mode", new BsonString("nearest")).append("tags", tagsArray)},
                new Object[]{ReadPreference.primaryPreferred().withTagSet(
                        new TagSet(asList(new Tag("dc", "ny"), new Tag("rack", "1")))),
                        new BsonDocument("mode", new BsonString("primaryPreferred")).append("tags", tagsArray)},
                new Object[]{ReadPreference.secondary().withTagSet(
                        new TagSet(asList(new Tag("dc", "ny"), new Tag("rack", "1")))),
                        new BsonDocument("mode", new BsonString("secondary")).append("tags", tagsArray)},
                new Object[]{ReadPreference.secondaryPreferred().withTagSet(
                        new TagSet(asList(new Tag("dc", "ny"), new Tag("rack", "1")))),
                        new BsonDocument("mode", new BsonString("secondaryPreferred")).append("tags", tagsArray)},
                new Object[]{ReadPreference.nearest().withTagSet(
                        new TagSet(asList(new Tag("dc", "ny"), new Tag("rack", "1")))),
                        new BsonDocument("mode", new BsonString("nearest")).append("tags", tagsArray)}
        );
    }

    @ParameterizedTest
    @MethodSource("differentHashCodesData")
    @DisplayName("different read preferences should have different hash codes")
    void differentReadPreferencesShouldHaveDifferentHashCodes(final ReadPreference first,
                                                               final ReadPreference second) {
        assertNotEquals(first.hashCode(), second.hashCode());
    }

    static Stream<Object[]> differentHashCodesData() {
        return Stream.of(
                new Object[]{ReadPreference.primary(), ReadPreference.secondary()},
                new Object[]{ReadPreference.secondary(), ReadPreference.nearest()},
                new Object[]{ReadPreference.secondary(),
                        ReadPreference.secondary(Collections.singletonList(new TagSet(
                                Collections.singletonList(new Tag("dc", "ny")))))},
                new Object[]{ReadPreference.secondary(Collections.singletonList(new TagSet(
                        Collections.singletonList(new Tag("dc", "ny"))))),
                        ReadPreference.secondary(Collections.singletonList(new TagSet(
                                Collections.singletonList(new Tag("dc", "la")))))},
                new Object[]{ReadPreference.secondary(), ReadPreference.secondary(1000, MILLISECONDS)},
                new Object[]{ReadPreference.secondary().withHedgeOptions(HEDGE_OPTIONS), ReadPreference.secondary()}
        );
    }
}
