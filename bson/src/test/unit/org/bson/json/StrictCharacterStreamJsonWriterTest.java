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

package org.bson.json;

import org.bson.BsonInvalidOperationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.StringWriter;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StrictCharacterStreamJsonWriterTest {

    private StringWriter stringWriter;
    private StrictCharacterStreamJsonWriter writer;

    @BeforeEach
    void setUp() {
        stringWriter = new StringWriter();
        writer = new StrictCharacterStreamJsonWriter(stringWriter,
                StrictCharacterStreamJsonWriterSettings.builder().build());
    }

    @Test
    void shouldWriteEmptyDocument() {
        writer.writeStartObject();
        writer.writeEndObject();
        assertEquals("{}", stringWriter.toString());
    }

    @Test
    void shouldWriteEmptyArray() {
        writer.writeStartArray();
        writer.writeEndArray();
        assertEquals("[]", stringWriter.toString());
    }

    @Test
    void shouldWriteNull() {
        writer.writeStartObject();
        writer.writeNull("n");
        writer.writeEndObject();
        assertEquals("{\"n\": null}", stringWriter.toString());
    }

    @Test
    void shouldWriteBoolean() {
        writer.writeStartObject();
        writer.writeBoolean("b1", true);
        writer.writeEndObject();
        assertEquals("{\"b1\": true}", stringWriter.toString());
    }

    @Test
    void shouldWriteNumber() {
        writer.writeStartObject();
        writer.writeNumber("n", "42");
        writer.writeEndObject();
        assertEquals("{\"n\": 42}", stringWriter.toString());
    }

    @Test
    void shouldWriteString() {
        writer.writeStartObject();
        writer.writeString("n", "42");
        writer.writeEndObject();
        assertEquals("{\"n\": \"42\"}", stringWriter.toString());
    }

    @Test
    void shouldWriteUnquotedString() {
        writer.writeStartObject();
        writer.writeRaw("s", "NumberDecimal(\"42.0\")");
        writer.writeEndObject();
        assertEquals("{\"s\": NumberDecimal(\"42.0\")}", stringWriter.toString());
    }

    @Test
    void shouldWriteDocument() {
        writer.writeStartObject();
        writer.writeStartObject("d");
        writer.writeEndObject();
        writer.writeEndObject();
        assertEquals("{\"d\": {}}", stringWriter.toString());
    }

    @Test
    void shouldWriteArray() {
        writer.writeStartObject();
        writer.writeStartArray("a");
        writer.writeEndArray();
        writer.writeEndObject();
        assertEquals("{\"a\": []}", stringWriter.toString());
    }

    @Test
    void shouldWriteArrayOfValues() {
        writer.writeStartObject();
        writer.writeStartArray("a");
        writer.writeNumber("1");
        writer.writeNull();
        writer.writeString("str");
        writer.writeEndArray();
        writer.writeEndObject();
        assertEquals("{\"a\": [1, null, \"str\"]}", stringWriter.toString());
    }

    private static Stream<Arguments> stringValues() {
        return Stream.of(
                Arguments.of("", "\"\""),
                Arguments.of(" ", "\" \""),
                Arguments.of("a", "\"a\""),
                Arguments.of("ab", "\"ab\""),
                Arguments.of("abc", "\"abc\""),
                Arguments.of("abc\u0000def", "\"abc\\u0000def\""),
                Arguments.of("\\", "\"\\\\\""),
                Arguments.of("'", "\"'\""),
                Arguments.of("\"", "\"\\\"\""),
                Arguments.of("\0", "\"\\u0000\""),
                Arguments.of("\b", "\"\\b\""),
                Arguments.of("\f", "\"\\f\""),
                Arguments.of("\n", "\"\\n\""),
                Arguments.of("\r", "\"\\r\""),
                Arguments.of("\t", "\"\\t\""),
                Arguments.of("\u0080", "\"\\u0080\""),
                Arguments.of("\u0080\u0081", "\"\\u0080\\u0081\""),
                Arguments.of("\u0080\u0081\u0082", "\"\\u0080\\u0081\\u0082\"")
        );
    }

    @ParameterizedTest
    @MethodSource("stringValues")
    void shouldWriteStrings(final String value, final String expected) {
        writer.writeStartObject();
        writer.writeString("str", value);
        writer.writeEndObject();
        assertEquals("{\"str\": " + expected + "}", stringWriter.toString());
    }

    @Test
    void shouldWriteTwoObjectElements() {
        writer.writeStartObject();
        writer.writeBoolean("b1", true);
        writer.writeBoolean("b2", false);
        writer.writeEndObject();
        assertEquals("{\"b1\": true, \"b2\": false}", stringWriter.toString());
    }

    @Test
    void shouldIndentOneElement() {
        writer = new StrictCharacterStreamJsonWriter(stringWriter,
                StrictCharacterStreamJsonWriterSettings.builder().indent(true).build());
        writer.writeStartObject();
        writer.writeString("name", "value");
        writer.writeEndObject();
        assertEquals(format("{%n  \"name\": \"value\"%n}"), stringWriter.toString());
    }

    @Test
    void shouldIndentOneElementWithIndentAndNewlineCharacters() {
        writer = new StrictCharacterStreamJsonWriter(stringWriter,
                StrictCharacterStreamJsonWriterSettings.builder()
                        .indent(true)
                        .indentCharacters("\t")
                        .newLineCharacters("\r")
                        .build());
        writer.writeStartObject();
        writer.writeString("name", "value");
        writer.writeEndObject();
        assertEquals("{\r\t\"name\": \"value\"\r}", stringWriter.toString());
    }

    @Test
    void shouldIndentTwoElements() {
        writer = new StrictCharacterStreamJsonWriter(stringWriter,
                StrictCharacterStreamJsonWriterSettings.builder().indent(true).build());
        writer.writeStartObject();
        writer.writeString("a", "x");
        writer.writeString("b", "y");
        writer.writeEndObject();
        assertEquals(format("{%n  \"a\": \"x\",%n  \"b\": \"y\"%n}"), stringWriter.toString());
    }

    @Test
    void shouldIndentTwoArrayElements() {
        writer = new StrictCharacterStreamJsonWriter(stringWriter,
                StrictCharacterStreamJsonWriterSettings.builder().indent(true).build());
        writer.writeStartObject();
        writer.writeStartArray("a");
        writer.writeNull();
        writer.writeNumber("4");
        writer.writeEndArray();
        writer.writeEndObject();
        assertEquals(format("{%n  \"a\": [%n    null,%n    4%n  ]%n}"), stringWriter.toString());
    }

    @Test
    void shouldIndentTwoDocumentElements() {
        writer = new StrictCharacterStreamJsonWriter(stringWriter,
                StrictCharacterStreamJsonWriterSettings.builder().indent(true).build());
        writer.writeStartObject();
        writer.writeStartArray("a");
        writer.writeStartObject();
        writer.writeNull("a");
        writer.writeEndObject();
        writer.writeStartObject();
        writer.writeNull("a");
        writer.writeEndObject();
        writer.writeEndArray();
        writer.writeEndObject();
        assertEquals(format("{%n  \"a\": [%n    {%n      \"a\": null%n    },%n    {%n      \"a\": null%n    }%n  ]%n}"),
                stringWriter.toString());
    }

    @Test
    void shouldIndentEmbeddedDocument() {
        writer = new StrictCharacterStreamJsonWriter(stringWriter,
                StrictCharacterStreamJsonWriterSettings.builder().indent(true).build());
        writer.writeStartObject();
        writer.writeStartObject("doc");
        writer.writeNumber("a", "1");
        writer.writeNumber("b", "2");
        writer.writeEndObject();
        writer.writeEndObject();
        assertEquals(format("{%n  \"doc\": {%n    \"a\": 1,%n    \"b\": 2%n  }%n}"), stringWriter.toString());
    }

    @Test
    void shouldThrowExceptionForBooleanWhenWritingBeforeStartingDocument() {
        assertThrows(BsonInvalidOperationException.class, () -> writer.writeBoolean("b1", true));
    }

    @Test
    void shouldThrowExceptionForNameWhenWritingBeforeStartingDocument() {
        assertThrows(BsonInvalidOperationException.class, () -> writer.writeName("name"));
    }

    @Test
    void shouldThrowExceptionForStringWhenStateIsValue() {
        writer.writeStartObject();
        assertThrows(BsonInvalidOperationException.class, () -> writer.writeString("SomeString"));
    }

    @Test
    void shouldThrowExceptionWhenEndingAnArrayWhenStateIsValue() {
        writer.writeStartObject();
        assertThrows(BsonInvalidOperationException.class, writer::writeEndArray);
    }

    @Test
    void shouldThrowExceptionWhenWritingASecondName() {
        writer.writeStartObject();
        writer.writeName("f1");
        assertThrows(BsonInvalidOperationException.class, () -> writer.writeName("i2"));
    }

    @Test
    void shouldThrowExceptionWhenEndingADocumentBeforeValueIsWritten() {
        writer.writeStartObject();
        writer.writeName("f1");
        assertThrows(BsonInvalidOperationException.class, writer::writeEndObject);
    }

    @Test
    void shouldThrowAnExceptionWhenTryingToWriteAValue() {
        assertThrows(BsonInvalidOperationException.class, () -> writer.writeString("i2"));
    }

    @Test
    void shouldThrowAnExceptionWhenWritingANameInAnArray() {
        writer.writeStartObject();
        writer.writeStartArray("f2");
        assertThrows(BsonInvalidOperationException.class, () -> writer.writeName("i3"));
    }

    @Test
    void shouldThrowAnExceptionWhenEndingDocumentInMiddleOfWritingArray() {
        writer.writeStartObject();
        writer.writeStartArray("f2");
        assertThrows(BsonInvalidOperationException.class, writer::writeEndObject);
    }

    @Test
    void shouldThrowAnExceptionWhenEndingAnArrayInASubDocument() {
        writer.writeStartObject();
        writer.writeStartArray("f2");
        writer.writeStartObject();
        assertThrows(BsonInvalidOperationException.class, writer::writeEndArray);
    }

    @Test
    void shouldThrowAnExceptionWhenEndingAnArrayWhenValueIsExpected() {
        writer.writeStartObject();
        writer.writeName("a");
        assertThrows(BsonInvalidOperationException.class, writer::writeEndArray);
    }

    @Test
    void shouldThrowAnExceptionWhenWritingNameInArrayEvenWhenSubDocumentExists() {
        writer.writeStartObject();
        writer.writeStartArray("f2");
        writer.writeStartObject();
        writer.writeEndObject();
        assertThrows(BsonInvalidOperationException.class, () -> writer.writeName("i3"));
    }

    @Test
    void shouldThrowAnExceptionWhenStartingAnObjectWhenDone() {
        writer.writeStartObject();
        writer.writeEndObject();
        assertThrows(BsonInvalidOperationException.class, writer::writeStartObject);
    }

    @Test
    void shouldThrowAnExceptionWhenStartingAnObjectWhenNameIsExpected() {
        writer.writeStartObject();
        assertThrows(BsonInvalidOperationException.class, writer::writeStartObject);
    }

    @Test
    void shouldThrowAnExceptionWhenAttemptingToEndAnArrayThatWasNotStarted() {
        writer.writeStartObject();
        writer.writeStartArray("f2");
        writer.writeEndArray();
        assertThrows(BsonInvalidOperationException.class, writer::writeEndArray);
    }

    @Test
    void shouldThrowAnExceptionWhenWritingNullName() {
        writer.writeStartObject();
        assertThrows(IllegalArgumentException.class, () -> writer.writeName(null));
    }

    @Test
    void shouldThrowAnExceptionWhenWritingNullValue() {
        writer.writeStartObject();
        writer.writeName("v");
        assertThrows(IllegalArgumentException.class, () -> writer.writeNumber(null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeString(null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeRaw(null));
    }

    @Test
    void shouldThrowAnExceptionWhenWritingNullMemberValue() {
        writer.writeStartObject();
        assertThrows(IllegalArgumentException.class, () -> writer.writeNumber("v", null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeString("v", null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeRaw("v", null));
    }

    @Test
    void shouldThrowAnExceptionWhenWritingNullMemberName() {
        writer.writeStartObject();
        assertThrows(IllegalArgumentException.class, () -> writer.writeNumber(null, "1"));
        assertThrows(IllegalArgumentException.class, () -> writer.writeString(null, "str"));
        assertThrows(IllegalArgumentException.class, () -> writer.writeRaw(null, "raw"));
        assertThrows(IllegalArgumentException.class, () -> writer.writeBoolean(null, true));
        assertThrows(IllegalArgumentException.class, () -> writer.writeNull(null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeStartObject(null));
        assertThrows(IllegalArgumentException.class, () -> writer.writeStartArray(null));
    }

    private static IntStream maxLengths() {
        return IntStream.rangeClosed(1, 20);
    }

    @ParameterizedTest
    @MethodSource("maxLengths")
    void shouldStopAtMaxLength(final int maxLength) {
        String fullJsonText = "{\"n\": null}";
        StringWriter sw = new StringWriter();
        StrictCharacterStreamJsonWriter w = new StrictCharacterStreamJsonWriter(sw,
                StrictCharacterStreamJsonWriterSettings.builder().maxLength(maxLength).build());
        w.writeStartObject();
        w.writeNull("n");
        w.writeEndObject();

        int expectedLength = Math.min(maxLength, fullJsonText.length());
        assertEquals(fullJsonText.substring(0, expectedLength), sw.toString());
        assertEquals(expectedLength, w.getCurrentLength());
        assertTrue(w.isTruncated() || fullJsonText.length() <= maxLength);
    }
}
