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

package com.mongodb.internal.connection;

import org.bson.BsonBinary;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDbPointer;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonReader;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.util.function.Supplier;

public class IdTrackingBsonWriter extends BsonWriterDecorator {

    private final LevelCountingBsonWriter idBsonBinaryWriter;
    private static final String ID_FIELD_NAME = "_id";
    private int level = -1;
    private String currentFieldName;
    private BsonValue id;

    public IdTrackingBsonWriter(final BsonWriter bsonWriter) {
        super(bsonWriter);
        idBsonBinaryWriter = new LevelCountingBsonWriter(new BsonBinaryWriter(new BasicOutputBuffer())){};
    }

    @Override
    public void writeStartDocument(final String name) {
        setCurrentFieldName(name);

        if (isWritingId()) {
            idBsonBinaryWriter.writeStartDocument(name);
        }
        level++;
        super.writeStartDocument(name);
    }

    @Override
    public void writeStartDocument() {
        if (isWritingId()) {
            idBsonBinaryWriter.writeStartDocument();
        }
        level++;
        super.writeStartDocument();
    }

    @Override
    public void writeEndDocument() {
        if (isWritingId()) {
            if (idBsonBinaryWriter.getCurrentLevel() >= 0) {
                idBsonBinaryWriter.writeEndDocument();
            }
            if (idBsonBinaryWriter.getCurrentLevel() == -1) {
                if (id != null && id.isJavaScriptWithScope()) {
                    id = new BsonJavaScriptWithScope(id.asJavaScriptWithScope().getCode(), new RawBsonDocument(getBytes()));
                } else if (id == null) {
                    id = new RawBsonDocument(getBytes());
                }
            }
        }


        if (level == 0 && id == null) {
            id = new BsonObjectId();
            writeObjectId(ID_FIELD_NAME, id.asObjectId().getValue());
        }

        level--;
        super.writeEndDocument();
    }

    @Override
    public void writeStartArray() {
        if (isWritingId()) {
            if (idBsonBinaryWriter.getCurrentLevel() == -1) {
                idBsonBinaryWriter.writeStartDocument();
                idBsonBinaryWriter.writeName(ID_FIELD_NAME);
            }
            idBsonBinaryWriter.writeStartArray();
        }

        level++;
        super.writeStartArray();
    }

    @Override
    public void writeStartArray(final String name) {
        setCurrentFieldName(name);
        if (isWritingId()) {
            if (idBsonBinaryWriter.getCurrentLevel() == -1) {
                idBsonBinaryWriter.writeStartDocument();
            }
            idBsonBinaryWriter.writeStartArray(name);
        }

        level++;
        super.writeStartArray(name);
    }

    @Override
    public void writeEndArray() {
        level--;
        super.writeEndArray();

        if (isWritingId()) {
            idBsonBinaryWriter.writeEndArray();
            if (level == 0) {
                idBsonBinaryWriter.writeEndDocument();
                id = new RawBsonDocument(getBytes()).get(ID_FIELD_NAME);
            }
        }
    }

    @Override
    public void writeBinaryData(final String name, final BsonBinary binary) {
        setCurrentFieldName(name);
        addBsonValue(() -> binary, () -> idBsonBinaryWriter.writeBinaryData(name, binary));
        super.writeBinaryData(name, binary);
    }

    @Override
    public void writeBinaryData(final BsonBinary binary) {
        addBsonValue(() -> binary, () -> idBsonBinaryWriter.writeBinaryData(binary));
        super.writeBinaryData(binary);
    }

    @Override
    public void writeBoolean(final String name, final boolean value) {
        setCurrentFieldName(name);
        addBsonValue(() -> BsonBoolean.valueOf(value), () -> idBsonBinaryWriter.writeBoolean(name, value));
        super.writeBoolean(name, value);
    }

    @Override
    public void writeBoolean(final boolean value) {
        addBsonValue(() -> BsonBoolean.valueOf(value), () -> idBsonBinaryWriter.writeBoolean(value));
        super.writeBoolean(value);
    }

    @Override
    public void writeDateTime(final String name, final long value) {
        setCurrentFieldName(name);
        addBsonValue(() -> new BsonDateTime(value), () -> idBsonBinaryWriter.writeDateTime(name, value));
        super.writeDateTime(name, value);
    }

    @Override
    public void writeDateTime(final long value) {
        addBsonValue(() -> new BsonDateTime(value), () -> idBsonBinaryWriter.writeDateTime(value));
        super.writeDateTime(value);
    }

    @Override
    public void writeDBPointer(final String name, final BsonDbPointer value) {
        setCurrentFieldName(name);
        addBsonValue(() -> value, () -> idBsonBinaryWriter.writeDBPointer(name, value));
        super.writeDBPointer(name, value);
    }

    @Override
    public void writeDBPointer(final BsonDbPointer value) {
        addBsonValue(() -> value, () -> idBsonBinaryWriter.writeDBPointer(value));
        super.writeDBPointer(value);
    }

    @Override
    public void writeDouble(final String name, final double value) {
        setCurrentFieldName(name);
        addBsonValue(() -> new BsonDouble(value), () -> idBsonBinaryWriter.writeDouble(name, value));
        super.writeDouble(name, value);
    }

    @Override
    public void writeDouble(final double value) {
        addBsonValue(() -> new BsonDouble(value), () -> idBsonBinaryWriter.writeDouble(value));
        super.writeDouble(value);
    }

    @Override
    public void writeInt32(final String name, final int value) {
        setCurrentFieldName(name);
        addBsonValue(() -> new BsonInt32(value), () -> idBsonBinaryWriter.writeInt32(name, value));
        super.writeInt32(name, value);
    }

    @Override
    public void writeInt32(final int value) {
        addBsonValue(() -> new BsonInt32(value), () -> idBsonBinaryWriter.writeInt32(value));
        super.writeInt32(value);
    }

    @Override
    public void writeInt64(final String name, final long value) {
        setCurrentFieldName(name);
        addBsonValue(() -> new BsonInt64(value), () -> idBsonBinaryWriter.writeInt64(name, value));
        super.writeInt64(name, value);
    }

    @Override
    public void writeInt64(final long value) {
        addBsonValue(() -> new BsonInt64(value), () -> idBsonBinaryWriter.writeInt64(value));
        super.writeInt64(value);
    }

    @Override
    public void writeDecimal128(final String name, final Decimal128 value) {
        setCurrentFieldName(name);
        addBsonValue(() -> new BsonDecimal128(value), () -> idBsonBinaryWriter.writeDecimal128(name, value));
        super.writeDecimal128(name, value);
    }

    @Override
    public void writeDecimal128(final Decimal128 value) {
        addBsonValue(() -> new BsonDecimal128(value), () -> idBsonBinaryWriter.writeDecimal128(value));
        super.writeDecimal128(value);
    }

    @Override
    public void writeJavaScript(final String name, final String code) {
        setCurrentFieldName(name);
        addBsonValue(() -> new BsonJavaScript(code), () -> idBsonBinaryWriter.writeJavaScript(name, code));
        super.writeJavaScript(name, code);
    }

    @Override
    public void writeJavaScript(final String code) {
        addBsonValue(() -> new BsonJavaScript(code), () -> idBsonBinaryWriter.writeJavaScript(code));
        super.writeJavaScript(code);
    }

    @Override
    public void writeJavaScriptWithScope(final String name, final String code) {
        addBsonValue(() -> new BsonJavaScriptWithScope(code, new BsonDocument()),
                () -> idBsonBinaryWriter.writeJavaScriptWithScope(name, code));
        super.writeJavaScriptWithScope(name, code);
    }

    @Override
    public void writeJavaScriptWithScope(final String code) {
        addBsonValue(() -> new BsonJavaScriptWithScope(code, new BsonDocument()), () -> idBsonBinaryWriter.writeJavaScriptWithScope(code));
        super.writeJavaScriptWithScope(code);
    }

    @Override
    public void writeMaxKey(final String name) {
        setCurrentFieldName(name);
        addBsonValue(BsonMaxKey::new, () -> idBsonBinaryWriter.writeMaxKey(name));
        super.writeMaxKey(name);
    }

    @Override
    public void writeMaxKey() {
        addBsonValue(BsonMaxKey::new, idBsonBinaryWriter::writeMaxKey);
        super.writeMaxKey();
    }

    @Override
    public void writeMinKey(final String name) {
        setCurrentFieldName(name);
        addBsonValue(BsonMinKey::new, () -> idBsonBinaryWriter.writeMinKey(name));
        super.writeMinKey(name);
    }

    @Override
    public void writeMinKey() {
        addBsonValue(BsonMinKey::new, idBsonBinaryWriter::writeMinKey);
        super.writeMinKey();
    }

    @Override
    public void writeName(final String name) {
        setCurrentFieldName(name);
        if (idBsonBinaryWriter.getCurrentLevel() >= 0) {
            idBsonBinaryWriter.writeName(name);
        }
        super.writeName(name);
    }

    @Override
    public void writeNull(final String name) {
        setCurrentFieldName(name);
        addBsonValue(BsonNull::new, () -> idBsonBinaryWriter.writeNull(name));
        super.writeNull(name);
    }

    @Override
    public void writeNull() {
        addBsonValue(BsonNull::new, idBsonBinaryWriter::writeNull);
        super.writeNull();
    }

    @Override
    public void writeObjectId(final String name, final ObjectId objectId) {
        setCurrentFieldName(name);
        addBsonValue(() -> new BsonObjectId(objectId), () -> idBsonBinaryWriter.writeObjectId(name, objectId));
        super.writeObjectId(name, objectId);
    }

    @Override
    public void writeObjectId(final ObjectId objectId) {
        addBsonValue(() -> new BsonObjectId(objectId), () -> idBsonBinaryWriter.writeObjectId(objectId));
        super.writeObjectId(objectId);
    }

    @Override
    public void writeRegularExpression(final String name, final BsonRegularExpression regularExpression) {
        setCurrentFieldName(name);
        addBsonValue(() -> regularExpression, () -> idBsonBinaryWriter.writeRegularExpression(name, regularExpression));
        super.writeRegularExpression(name, regularExpression);
    }

    @Override
    public void writeRegularExpression(final BsonRegularExpression regularExpression) {
        addBsonValue(() -> regularExpression, () -> idBsonBinaryWriter.writeRegularExpression(regularExpression));
        super.writeRegularExpression(regularExpression);
    }

    @Override
    public void writeString(final String name, final String value) {
        setCurrentFieldName(name);
        addBsonValue(() -> new BsonString(value), () -> idBsonBinaryWriter.writeString(name, value));
        super.writeString(name, value);
    }

    @Override
    public void writeString(final String value) {
        addBsonValue(() -> new BsonString(value), () -> idBsonBinaryWriter.writeString(value));
        super.writeString(value);
    }

    @Override
    public void writeSymbol(final String name, final String value) {
        setCurrentFieldName(name);
        addBsonValue(() -> new BsonSymbol(value), () -> idBsonBinaryWriter.writeSymbol(name, value));
        super.writeSymbol(name, value);
    }

    @Override
    public void writeSymbol(final String value) {
        addBsonValue(() -> new BsonSymbol(value), () -> idBsonBinaryWriter.writeSymbol(value));
        super.writeSymbol(value);
    }

    @Override
    public void writeTimestamp(final String name, final BsonTimestamp value) {
        setCurrentFieldName(name);
        addBsonValue(() -> value, () -> idBsonBinaryWriter.writeTimestamp(name, value));
        super.writeTimestamp(name, value);
    }

    @Override
    public void writeTimestamp(final BsonTimestamp value) {
        addBsonValue(() -> value, () -> idBsonBinaryWriter.writeTimestamp(value));
        super.writeTimestamp(value);
    }

    @Override
    public void writeUndefined(final String name) {
        setCurrentFieldName(name);
        addBsonValue(BsonUndefined::new, () -> idBsonBinaryWriter.writeUndefined(name));
        super.writeUndefined(name);
    }

    @Override
    public void writeUndefined() {
        addBsonValue(BsonUndefined::new, idBsonBinaryWriter::writeUndefined);
        super.writeUndefined();
    }

    @Override
    public void pipe(final BsonReader reader) {
        super.pipe(reader);
    }

    @Override
    public void flush() {
        super.flush();
    }

    public BsonValue getId() {
        return id;
    }

    private void setCurrentFieldName(final String name) {
        currentFieldName = name;
    }

    private boolean isWritingId() {
        return idBsonBinaryWriter.getCurrentLevel() >= 0 || (level == 0 && currentFieldName != null
                && currentFieldName.equals(ID_FIELD_NAME));
    }

    private void addBsonValue(final Supplier<BsonValue> value, final Runnable writeValue) {
        if (isWritingId()) {
            if (idBsonBinaryWriter.getCurrentLevel() >= 0) {
                writeValue.run();
            } else {
                id = value.get();
            }
        }
    }

    private byte[] getBytes() {
        return ((BasicOutputBuffer) idBsonBinaryWriter.getBsonBinaryWriter().getBsonOutput()).getInternalBuffer();
    }

}
