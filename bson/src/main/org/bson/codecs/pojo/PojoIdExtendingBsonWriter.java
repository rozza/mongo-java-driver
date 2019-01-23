/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.bson.codecs.pojo;

import org.bson.BsonBinary;
import org.bson.BsonDbPointer;
import org.bson.BsonReader;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

class PojoIdExtendingBsonWriter implements BsonWriter {
    private static final CodecRegistry REGISTRY = fromProviders(new BsonValueCodecProvider());
    private static final EncoderContext ENCODER_CONTEXT = EncoderContext.builder().build();
    private static final String ID_NAME = "_id";
    private final BsonValue id;
    private final BsonWriter writer;


    PojoIdExtendingBsonWriter(final BsonValue id, final BsonWriter writer) {
        this.id = id;
        this.writer = writer;
    }

    @Override
    public void writeStartDocument() {
        writer.writeStartDocument();
        writer.writeName(ID_NAME);
        REGISTRY.get(BsonValue.class).encode(writer, id, ENCODER_CONTEXT);
    }

    @Override
    public void flush() {
        writer.flush();
    }

    @Override
    public void writeBinaryData(final BsonBinary binary) {
        writer.writeBinaryData(binary);
    }

    @Override
    public void writeBinaryData(final String name, final BsonBinary binary) {
        writer.writeBinaryData(name, binary);
    }

    @Override
    public void writeBoolean(final boolean value) {
        writer.writeBoolean(value);
    }

    @Override
    public void writeBoolean(final String name, final boolean value) {
        writer.writeBoolean(name, value);
    }

    @Override
    public void writeDateTime(final long value) {
        writer.writeDateTime(value);
    }

    @Override
    public void writeDateTime(final String name, final long value) {
        writer.writeDateTime(name, value);
    }

    @Override
    public void writeDBPointer(final BsonDbPointer value) {
        writer.writeDBPointer(value);
    }

    @Override
    public void writeDBPointer(final String name, final BsonDbPointer value) {
        writer.writeDBPointer(name, value);
    }

    @Override
    public void writeDouble(final double value) {
        writer.writeDouble(value);
    }

    @Override
    public void writeDouble(final String name, final double value) {
        writer.writeDouble(name, value);
    }

    @Override
    public void writeEndArray() {
        writer.writeEndArray();
    }

    @Override
    public void writeEndDocument() {
        writer.writeEndDocument();
    }

    @Override
    public void writeInt32(final int value) {
        writer.writeInt32(value);
    }

    @Override
    public void writeInt32(final String name, final int value) {
        writer.writeInt32(name, value);
    }

    @Override
    public void writeInt64(final long value) {
        writer.writeInt64(value);
    }

    @Override
    public void writeInt64(final String name, final long value) {
        writer.writeInt64(name, value);
    }

    @Override
    public void writeDecimal128(final Decimal128 value) {
        writer.writeDecimal128(value);
    }

    @Override
    public void writeDecimal128(final String name, final Decimal128 value) {
        writer.writeDecimal128(name, value);
    }

    @Override
    public void writeJavaScript(final String code) {
        writer.writeJavaScript(code);
    }

    @Override
    public void writeJavaScript(final String name, final String code) {
        writer.writeJavaScript(name, code);
    }

    @Override
    public void writeJavaScriptWithScope(final String code) {
        writer.writeJavaScriptWithScope(code);
    }

    @Override
    public void writeJavaScriptWithScope(final String name, final String code) {
        writer.writeJavaScriptWithScope(name, code);
    }

    @Override
    public void writeMaxKey() {
        writer.writeMaxKey();
    }

    @Override
    public void writeMaxKey(final String name) {
        writer.writeMaxKey(name);
    }

    @Override
    public void writeMinKey() {
        writer.writeMinKey();
    }

    @Override
    public void writeMinKey(final String name) {
        writer.writeMinKey(name);
    }

    @Override
    public void writeName(final String name) {
        writer.writeName(name);
    }

    @Override
    public void writeNull() {
        writer.writeNull();
    }

    @Override
    public void writeNull(final String name) {
        writer.writeNull(name);
    }

    @Override
    public void writeObjectId(final ObjectId objectId) {
        writer.writeObjectId(objectId);
    }

    @Override
    public void writeObjectId(final String name, final ObjectId objectId) {
        writer.writeObjectId(name, objectId);
    }

    @Override
    public void writeRegularExpression(final BsonRegularExpression regularExpression) {
        writer.writeRegularExpression(regularExpression);
    }

    @Override
    public void writeRegularExpression(final String name, final BsonRegularExpression regularExpression) {
        writer.writeRegularExpression(name, regularExpression);
    }

    @Override
    public void writeStartArray() {
        writer.writeStartArray();
    }

    @Override
    public void writeStartArray(final String name) {
        writer.writeStartArray(name);
    }

    @Override
    public void writeStartDocument(final String name) {
        writer.writeStartDocument(name);
    }

    @Override
    public void writeString(final String value) {
        writer.writeString(value);
    }

    @Override
    public void writeString(final String name, final String value) {
        writer.writeString(name, value);
    }

    @Override
    public void writeSymbol(final String value) {
        writer.writeSymbol(value);
    }

    @Override
    public void writeSymbol(final String name, final String value) {
        writer.writeSymbol(name, value);
    }

    @Override
    public void writeTimestamp(final BsonTimestamp value) {
        writer.writeTimestamp(value);
    }

    @Override
    public void writeTimestamp(final String name, final BsonTimestamp value) {
        writer.writeTimestamp(name, value);
    }

    @Override
    public void writeUndefined() {
        writer.writeUndefined();
    }

    @Override
    public void writeUndefined(final String name) {
        writer.writeUndefined(name);
    }

    @Override
    public void pipe(final BsonReader reader) {
        writer.pipe(reader);
    }
}
