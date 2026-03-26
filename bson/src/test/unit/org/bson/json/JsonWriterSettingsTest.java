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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonWriterSettingsTest {

    @Test
    void testDefaults() {
        JsonWriterSettings settings = JsonWriterSettings.builder().build();
        assertFalse(settings.isIndent());
        assertEquals(JsonMode.RELAXED, settings.getOutputMode());
        assertEquals(0, settings.getMaxLength());
    }

    @Test
    void testOutputMode() {
        JsonWriterSettings settings = JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build();
        assertEquals(JsonMode.SHELL, settings.getOutputMode());
    }

    @Test
    void testIndentDefaults() {
        JsonWriterSettings settings = JsonWriterSettings.builder().indent(true).build();
        assertTrue(settings.isIndent());
        assertEquals("  ", settings.getIndentCharacters());
        assertEquals(System.getProperty("line.separator"), settings.getNewLineCharacters());
    }

    @Test
    void testIndentSettings() {
        JsonWriterSettings settings = JsonWriterSettings.builder()
                .indent(true).indentCharacters("\t").newLineCharacters("\r\n").build();
        assertEquals("\t", settings.getIndentCharacters());
        assertEquals("\r\n", settings.getNewLineCharacters());
    }

    @Test
    void testMaxLengthSetting() {
        JsonWriterSettings settings = JsonWriterSettings.builder().maxLength(100).build();
        assertEquals(100, settings.getMaxLength());
    }

    @SuppressWarnings("deprecation")
    @Test
    void shouldUseLegacyExtendedJsonConvertersForStrictMode() {
        JsonWriterSettings settings = JsonWriterSettings.builder().outputMode(JsonMode.STRICT).build();
        assertInstanceOf(LegacyExtendedJsonBinaryConverter.class, settings.getBinaryConverter());
        assertInstanceOf(JsonBooleanConverter.class, settings.getBooleanConverter());
        assertInstanceOf(LegacyExtendedJsonDateTimeConverter.class, settings.getDateTimeConverter());
        assertInstanceOf(ExtendedJsonDecimal128Converter.class, settings.getDecimal128Converter());
        assertInstanceOf(JsonDoubleConverter.class, settings.getDoubleConverter());
        assertInstanceOf(JsonInt32Converter.class, settings.getInt32Converter());
        assertInstanceOf(ExtendedJsonInt64Converter.class, settings.getInt64Converter());
        assertInstanceOf(JsonJavaScriptConverter.class, settings.getJavaScriptConverter());
        assertInstanceOf(ExtendedJsonMaxKeyConverter.class, settings.getMaxKeyConverter());
        assertInstanceOf(ExtendedJsonMinKeyConverter.class, settings.getMinKeyConverter());
        assertInstanceOf(JsonNullConverter.class, settings.getNullConverter());
        assertInstanceOf(ExtendedJsonObjectIdConverter.class, settings.getObjectIdConverter());
        assertInstanceOf(LegacyExtendedJsonRegularExpressionConverter.class, settings.getRegularExpressionConverter());
        assertInstanceOf(JsonStringConverter.class, settings.getStringConverter());
        assertInstanceOf(JsonSymbolConverter.class, settings.getSymbolConverter());
        assertInstanceOf(ExtendedJsonTimestampConverter.class, settings.getTimestampConverter());
        assertInstanceOf(ExtendedJsonUndefinedConverter.class, settings.getUndefinedConverter());
    }

    @Test
    void shouldUseExtendedJsonConvertersForExtendedJsonMode() {
        JsonWriterSettings settings = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();
        assertInstanceOf(ExtendedJsonBinaryConverter.class, settings.getBinaryConverter());
        assertInstanceOf(JsonBooleanConverter.class, settings.getBooleanConverter());
        assertInstanceOf(ExtendedJsonDateTimeConverter.class, settings.getDateTimeConverter());
        assertInstanceOf(ExtendedJsonDecimal128Converter.class, settings.getDecimal128Converter());
        assertInstanceOf(ExtendedJsonDoubleConverter.class, settings.getDoubleConverter());
        assertInstanceOf(ExtendedJsonInt32Converter.class, settings.getInt32Converter());
        assertInstanceOf(ExtendedJsonInt64Converter.class, settings.getInt64Converter());
        assertInstanceOf(JsonJavaScriptConverter.class, settings.getJavaScriptConverter());
        assertInstanceOf(ExtendedJsonMaxKeyConverter.class, settings.getMaxKeyConverter());
        assertInstanceOf(ExtendedJsonMinKeyConverter.class, settings.getMinKeyConverter());
        assertInstanceOf(JsonNullConverter.class, settings.getNullConverter());
        assertInstanceOf(ExtendedJsonObjectIdConverter.class, settings.getObjectIdConverter());
        assertInstanceOf(ExtendedJsonRegularExpressionConverter.class, settings.getRegularExpressionConverter());
        assertInstanceOf(JsonStringConverter.class, settings.getStringConverter());
        assertInstanceOf(JsonSymbolConverter.class, settings.getSymbolConverter());
        assertInstanceOf(ExtendedJsonTimestampConverter.class, settings.getTimestampConverter());
        assertInstanceOf(ExtendedJsonUndefinedConverter.class, settings.getUndefinedConverter());
    }

    @Test
    void shouldUseShellConvertersForShellMode() {
        JsonWriterSettings settings = JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build();
        assertInstanceOf(ShellBinaryConverter.class, settings.getBinaryConverter());
        assertInstanceOf(JsonBooleanConverter.class, settings.getBooleanConverter());
        assertInstanceOf(ShellDateTimeConverter.class, settings.getDateTimeConverter());
        assertInstanceOf(ShellDecimal128Converter.class, settings.getDecimal128Converter());
        assertInstanceOf(JsonDoubleConverter.class, settings.getDoubleConverter());
        assertInstanceOf(JsonInt32Converter.class, settings.getInt32Converter());
        assertInstanceOf(ShellInt64Converter.class, settings.getInt64Converter());
        assertInstanceOf(JsonJavaScriptConverter.class, settings.getJavaScriptConverter());
        assertInstanceOf(ShellMaxKeyConverter.class, settings.getMaxKeyConverter());
        assertInstanceOf(ShellMinKeyConverter.class, settings.getMinKeyConverter());
        assertInstanceOf(JsonNullConverter.class, settings.getNullConverter());
        assertInstanceOf(ShellObjectIdConverter.class, settings.getObjectIdConverter());
        assertInstanceOf(ShellRegularExpressionConverter.class, settings.getRegularExpressionConverter());
        assertInstanceOf(JsonStringConverter.class, settings.getStringConverter());
        assertInstanceOf(JsonSymbolConverter.class, settings.getSymbolConverter());
        assertInstanceOf(ShellTimestampConverter.class, settings.getTimestampConverter());
        assertInstanceOf(ShellUndefinedConverter.class, settings.getUndefinedConverter());
    }

    @Test
    void shouldSetConverters() {
        ShellBinaryConverter binaryConverter = new ShellBinaryConverter();
        JsonBooleanConverter booleanConverter = new JsonBooleanConverter();
        ShellDateTimeConverter dateTimeConverter = new ShellDateTimeConverter();
        ShellDecimal128Converter decimal128Converter = new ShellDecimal128Converter();
        JsonDoubleConverter doubleConverter = new JsonDoubleConverter();
        JsonInt32Converter int32Converter = new JsonInt32Converter();
        ShellInt64Converter int64Converter = new ShellInt64Converter();
        JsonJavaScriptConverter javaScriptConverter = new JsonJavaScriptConverter();
        ShellMaxKeyConverter maxKeyConverter = new ShellMaxKeyConverter();
        ShellMinKeyConverter minKeyConverter = new ShellMinKeyConverter();
        JsonNullConverter nullConverter = new JsonNullConverter();
        ShellObjectIdConverter objectIdConverter = new ShellObjectIdConverter();
        ShellRegularExpressionConverter regularExpressionConverter = new ShellRegularExpressionConverter();
        JsonStringConverter stringConverter = new JsonStringConverter();
        JsonSymbolConverter symbolConverter = new JsonSymbolConverter();
        ShellTimestampConverter timestampConverter = new ShellTimestampConverter();
        ShellUndefinedConverter undefinedConverter = new ShellUndefinedConverter();

        JsonWriterSettings settings = JsonWriterSettings.builder()
                .binaryConverter(binaryConverter)
                .booleanConverter(booleanConverter)
                .dateTimeConverter(dateTimeConverter)
                .decimal128Converter(decimal128Converter)
                .doubleConverter(doubleConverter)
                .int32Converter(int32Converter)
                .int64Converter(int64Converter)
                .javaScriptConverter(javaScriptConverter)
                .maxKeyConverter(maxKeyConverter)
                .minKeyConverter(minKeyConverter)
                .nullConverter(nullConverter)
                .objectIdConverter(objectIdConverter)
                .regularExpressionConverter(regularExpressionConverter)
                .stringConverter(stringConverter)
                .symbolConverter(symbolConverter)
                .timestampConverter(timestampConverter)
                .undefinedConverter(undefinedConverter)
                .build();

        assertSame(binaryConverter, settings.getBinaryConverter());
        assertSame(booleanConverter, settings.getBooleanConverter());
        assertSame(dateTimeConverter, settings.getDateTimeConverter());
        assertSame(decimal128Converter, settings.getDecimal128Converter());
        assertSame(doubleConverter, settings.getDoubleConverter());
        assertSame(int32Converter, settings.getInt32Converter());
        assertSame(int64Converter, settings.getInt64Converter());
        assertSame(javaScriptConverter, settings.getJavaScriptConverter());
        assertSame(maxKeyConverter, settings.getMaxKeyConverter());
        assertSame(minKeyConverter, settings.getMinKeyConverter());
        assertSame(nullConverter, settings.getNullConverter());
        assertSame(objectIdConverter, settings.getObjectIdConverter());
        assertSame(regularExpressionConverter, settings.getRegularExpressionConverter());
        assertSame(stringConverter, settings.getStringConverter());
        assertSame(symbolConverter, settings.getSymbolConverter());
        assertSame(timestampConverter, settings.getTimestampConverter());
        assertSame(undefinedConverter, settings.getUndefinedConverter());
    }
}
