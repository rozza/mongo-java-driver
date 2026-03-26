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

package org.bson;

import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BsonWriterTest {

    private static Stream<BsonWriter> writers() {
        return Stream.of(
                new BsonBinaryWriter(new BasicOutputBuffer()),
                new BsonDocumentWriter(new BsonDocument())
        );
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldThrowExceptionForBooleanWhenWritingBeforeStartingDocument")
    void shouldThrowExceptionForBooleanBeforeStartingDocument(final BsonWriter writer) {
        assertThrows(BsonInvalidOperationException.class, () -> writer.writeBoolean("b1", true));
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldThrowExceptionForArrayWhenWritingBeforeStartingDocument")
    void shouldThrowExceptionForArrayBeforeStartingDocument(final BsonWriter writer) {
        assertThrows(BsonInvalidOperationException.class, writer::writeStartArray);
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldThrowExceptionForNullWhenWritingBeforeStartingDocument")
    void shouldThrowExceptionForNullBeforeStartingDocument(final BsonWriter writer) {
        assertThrows(BsonInvalidOperationException.class, writer::writeNull);
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldThrowExceptionForStringWhenStateIsValue")
    void shouldThrowExceptionForStringWhenStateIsValue(final BsonWriter writer) {
        assertThrows(BsonInvalidOperationException.class, () -> {
            writer.writeStartDocument();
            writer.writeString("SomeString");
        });
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldThrowExceptionWhenEndingAnArrayWhenStateIsValue")
    void shouldThrowExceptionWhenEndingArrayWhenStateIsValue(final BsonWriter writer) {
        assertThrows(BsonInvalidOperationException.class, () -> {
            writer.writeStartDocument();
            writer.writeEndArray();
        });
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldThrowExceptionWhenWritingASecondName")
    void shouldThrowExceptionWhenWritingSecondName(final BsonWriter writer) {
        assertThrows(BsonInvalidOperationException.class, () -> {
            writer.writeStartDocument();
            writer.writeName("f1");
            writer.writeName("i2");
        });
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldThrowExceptionWhenEndingADocumentBeforeValueIsWritten")
    void shouldThrowExceptionWhenEndingDocumentBeforeValueWritten(final BsonWriter writer) {
        assertThrows(BsonInvalidOperationException.class, () -> {
            writer.writeStartDocument();
            writer.writeName("f1");
            writer.writeEndDocument();
        });
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldThrowAnExceptionWhenTryingToWriteASecondValue")
    void shouldThrowExceptionWhenWritingSecondValue(final BsonWriter writer) {
        assertThrows(BsonInvalidOperationException.class, () -> {
            writer.writeStartDocument();
            writer.writeName("f1");
            writer.writeDouble(100);
            writer.writeString("i2");
        });
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldThrowAnExceptionWhenTryingToWriteJavaScript")
    void shouldThrowExceptionWhenWritingJavaScript(final BsonWriter writer) {
        assertThrows(BsonInvalidOperationException.class, () -> {
            writer.writeStartDocument();
            writer.writeName("f1");
            writer.writeDouble(100);
            writer.writeJavaScript("var i");
        });
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldThrowAnExceptionWhenWritingANameInAnArray")
    void shouldThrowExceptionWhenWritingNameInArray(final BsonWriter writer) {
        assertThrows(BsonInvalidOperationException.class, () -> {
            writer.writeStartDocument();
            writer.writeName("f1");
            writer.writeDouble(100);
            writer.writeStartArray("f2");
            writer.writeName("i3");
        });
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldThrowAnExceptionWhenEndingDocumentInTheMiddleOfWritingAnArray")
    void shouldThrowExceptionWhenEndingDocumentInMiddleOfArray(final BsonWriter writer) {
        assertThrows(BsonInvalidOperationException.class, () -> {
            writer.writeStartDocument();
            writer.writeName("f1");
            writer.writeDouble(100);
            writer.writeStartArray("f2");
            writer.writeEndDocument();
        });
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldThrowAnExceptionWhenEndingAnArrayInASubDocument")
    void shouldThrowExceptionWhenEndingArrayInSubDocument(final BsonWriter writer) {
        assertThrows(BsonInvalidOperationException.class, () -> {
            writer.writeStartDocument();
            writer.writeName("f1");
            writer.writeDouble(100);
            writer.writeStartArray("f2");
            writer.writeStartDocument();
            writer.writeEndArray();
        });
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldThrowAnExceptionWhenWritingANameInAnArrayEvenWhenSubDocumentExistsInArray")
    void shouldThrowExceptionWhenWritingNameInArrayWithSubDoc(final BsonWriter writer) {
        assertThrows(BsonInvalidOperationException.class, () -> {
            writer.writeStartDocument();
            writer.writeName("f1");
            writer.writeDouble(100);
            writer.writeStartArray("f2");
            writer.writeStartDocument();
            writer.writeEndDocument();
            writer.writeName("i3");
        });
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldThrowExceptionWhenWritingObjectsIntoNestedArrays")
    void shouldThrowExceptionWhenWritingObjectsIntoNestedArrays(final BsonWriter writer) {
        assertThrows(BsonInvalidOperationException.class, () -> {
            writer.writeStartDocument();
            writer.writeName("f1");
            writer.writeDouble(100);
            writer.writeStartArray("f2");
            writer.writeStartArray();
            writer.writeStartArray();
            writer.writeStartArray();
            writer.writeInt64("i4", 10);
        });
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldThrowAnExceptionWhenAttemptingToEndAnArrayThatWasNotStarted")
    void shouldThrowExceptionWhenEndingArrayNotStarted(final BsonWriter writer) {
        assertThrows(BsonInvalidOperationException.class, () -> {
            writer.writeStartDocument();
            writer.writeStartArray("f2");
            writer.writeEndArray();
            writer.writeEndArray();
        });
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldThrowAnErrorIfTryingToWriteNamesIntoAJavascriptScope1")
    void shouldThrowErrorWritingIntoJavascriptScope1(final BsonWriter writer) {
        assertThrows(BsonInvalidOperationException.class, () -> {
            writer.writeStartDocument();
            writer.writeJavaScriptWithScope("js1", "var i = 1");
            writer.writeBoolean("b4", true);
        });
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldThrowAnErrorIfTryingToWriteNamesIntoAJavascriptScope2")
    void shouldThrowErrorWritingIntoJavascriptScope2(final BsonWriter writer) {
        assertThrows(BsonInvalidOperationException.class, () -> {
            writer.writeStartDocument();
            writer.writeJavaScriptWithScope("js1", "var i = 1");
            writer.writeBinaryData(new BsonBinary(new byte[]{0, 0, 1, 0}));
        });
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldThrowAnErrorIfTryingToWriteNamesIntoAJavascriptScope3")
    void shouldThrowErrorWritingIntoJavascriptScope3(final BsonWriter writer) {
        assertThrows(BsonInvalidOperationException.class, () -> {
            writer.writeStartDocument();
            writer.writeJavaScriptWithScope("js1", "var i = 1");
            writer.writeStartArray();
        });
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldThrowAnErrorIfTryingToWriteNamesIntoAJavascriptScope4")
    void shouldThrowErrorWritingIntoJavascriptScope4(final BsonWriter writer) {
        assertThrows(BsonInvalidOperationException.class, () -> {
            writer.writeStartDocument();
            writer.writeJavaScriptWithScope("js1", "var i = 1");
            writer.writeEndDocument();
        });
    }

    @Test
    @DisplayName("shouldThrowAnErrorIfKeyContainsNullCharacter")
    void shouldThrowErrorIfKeyContainsNullCharacter() {
        BsonWriter writer = new BsonBinaryWriter(new BasicOutputBuffer());
        assertThrows(BSONException.class, () -> {
            writer.writeStartDocument();
            writer.writeBoolean("h\u0000i", true);
        });
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldNotThrowAnErrorIfValueContainsNullCharacter")
    void shouldNotThrowIfValueContainsNullCharacter(final BsonWriter writer) {
        writer.writeStartDocument();
        writer.writeString("x", "h\u0000i");
        // no exception expected
    }

    @ParameterizedTest
    @MethodSource("writers")
    @DisplayName("shouldNotThrowAnExceptionIfCorrectlyStartingAndEndingDocumentsAndSubDocuments")
    void shouldNotThrowIfCorrectlyStartingAndEndingDocuments(final BsonWriter writer) {
        writer.writeStartDocument();
        writer.writeJavaScriptWithScope("js1", "var i = 1");
        writer.writeStartDocument();
        writer.writeEndDocument();
        writer.writeEndDocument();
        // no exception expected
    }

    @Test
    @DisplayName("shouldThrowOnInvalidFieldName")
    void shouldThrowOnInvalidFieldName() {
        BsonWriter writer = new BsonBinaryWriter(new BasicOutputBuffer(), new TestFieldNameValidator("bad"));
        writer.writeStartDocument();
        writer.writeString("good", "string");

        assertThrows(IllegalArgumentException.class, () -> writer.writeString("bad", "string"));
    }

    @Test
    @DisplayName("shouldThrowOnInvalidFieldNameNestedInDocument")
    void shouldThrowOnInvalidFieldNameNestedInDocument() {
        BsonWriter writer = new BsonBinaryWriter(new BasicOutputBuffer(), new TestFieldNameValidator("bad"));
        writer.writeStartDocument();
        writer.writeName("doc");
        writer.writeStartDocument();
        writer.writeString("good", "string");
        writer.writeString("bad", "string");

        assertThrows(IllegalArgumentException.class, () -> writer.writeString("bad-child", "string"));
    }

    @Test
    @DisplayName("shouldThrowOnInvalidFieldNameNestedInDocumentInArray")
    void shouldThrowOnInvalidFieldNameNestedInDocumentInArray() {
        BsonWriter writer = new BsonBinaryWriter(new BasicOutputBuffer(), new TestFieldNameValidator("bad"));
        writer.writeStartDocument();
        writer.writeName("doc");
        writer.writeStartArray();
        writer.writeStartDocument();
        writer.writeString("good", "string");
        writer.writeString("bad", "string");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> writer.writeString("bad-child", "string"));
        assertEquals("testFieldNameValidator error", e.getMessage());
    }

    private static class TestFieldNameValidator implements FieldNameValidator {
        private final String badFieldName;

        TestFieldNameValidator(final String badFieldName) {
            this.badFieldName = badFieldName;
        }

        @Override
        public boolean validate(final String fieldName) {
            return !fieldName.equals(badFieldName);
        }

        @Override
        public String getValidationErrorMessage(final String fieldName) {
            return "testFieldNameValidator error";
        }

        @Override
        public FieldNameValidator getValidatorForField(final String fieldName) {
            return new TestFieldNameValidator(badFieldName + "-child");
        }
    }
}
