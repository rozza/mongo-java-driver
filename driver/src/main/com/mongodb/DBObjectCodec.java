/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.codecs.BinaryToByteArrayTransformer;
import com.mongodb.codecs.BinaryToUUIDTransformer;
import com.mongodb.codecs.CollectibleCodec;
import com.mongodb.codecs.IdGenerator;
import com.mongodb.codecs.ObjectIdGenerator;
import org.bson.BSON;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDbPointer;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.BasicBSONList;
import org.bson.types.Binary;
import org.bson.types.CodeWScope;
import org.bson.types.Symbol;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuppressWarnings("rawtypes")
class DBObjectCodec implements CollectibleCodec<DBObject> {
    private static final String ID_FIELD_NAME = "_id";

    private final CodecRegistry codecRegistry;
    private final Map<BsonType, Class<?>> bsonTypeClassMap;
    private final DB db;
    private final DBObjectFactory objectFactory;
    private final IdGenerator idGenerator = new ObjectIdGenerator();

    public DBObjectCodec(final CodecRegistry codecRegistry, final Map<BsonType, Class<?>> bsonTypeClassMap) {
        this.codecRegistry = codecRegistry;
        this.bsonTypeClassMap = bsonTypeClassMap;
        this.db = null;
        this.objectFactory = null;
    }

    public DBObjectCodec(final DB db, final DBObjectFactory objectFactory,
                         final CodecRegistry codecRegistry, final Map<BsonType, Class<?>> bsonTypeClassMap) {
        this.db = db;
        this.objectFactory = objectFactory;
        this.codecRegistry = codecRegistry;
        this.bsonTypeClassMap = bsonTypeClassMap;
    }

    @Override
    public void encode(final BsonWriter writer, final DBObject document, final EncoderContext encoderContext) {
        writer.writeStartDocument();

        beforeFields(writer, encoderContext, document);

        for (final String key : document.keySet()) {
            if (skipField(encoderContext, key)) {
                continue;
            }
            writer.writeName(key);
            writeValue(writer, encoderContext, document.get(key));
        }
        writer.writeEndDocument();
    }

    @Override
    public DBObject decode(final BsonReader reader, final DecoderContext decoderContext) {
        List<String> path = new ArrayList<String>(10);
        return readDocument(reader, decoderContext, path);
    }

    @Override
    public Class<DBObject> getEncoderClass() {
        return DBObject.class;
    }

    @Override
    public boolean documentHasId(final DBObject document) {
        return document.containsField(ID_FIELD_NAME);
    }

    @Override
    public BsonValue getDocumentId(final DBObject document) {
        if (!documentHasId(document)) {
            throw new IllegalStateException("The document does not contain an _id");
        }

        Object id = document.get(ID_FIELD_NAME);
        if (id instanceof BsonValue) {
            return (BsonValue) id;
        }

        BsonDocument idHoldingDocument = new BsonDocument();
        BsonWriter writer = new BsonDocumentWriter(idHoldingDocument);
        writer.writeStartDocument();
        writer.writeName(ID_FIELD_NAME);
        writeValue(writer, EncoderContext.builder().build(), id);
        writer.writeEndDocument();
        return idHoldingDocument.get(ID_FIELD_NAME);
    }

    @Override
    public void generateIdIfAbsentFromDocument(final DBObject document) {
        if (!documentHasId(document)) {
            document.put(ID_FIELD_NAME, idGenerator.generate());
        }
    }

    private void beforeFields(final BsonWriter bsonWriter, final EncoderContext encoderContext, final DBObject document) {
        if (encoderContext.isEncodingCollectibleDocument() && document.containsField(ID_FIELD_NAME)) {
            bsonWriter.writeName(ID_FIELD_NAME);
            writeValue(bsonWriter, null, document.get(ID_FIELD_NAME));
        }
    }

    private boolean skipField(final EncoderContext encoderContext, final String key) {
        return encoderContext.isEncodingCollectibleDocument() && key.equals(ID_FIELD_NAME);
    }

    @SuppressWarnings("unchecked")
    private void writeValue(final BsonWriter bsonWriter, final EncoderContext encoderContext, final Object initialValue) {
        Object value = BSON.applyEncodingHooks(initialValue);
        if (value == null) {
            bsonWriter.writeNull();
        } else if (value instanceof DBRefBase) {
            encodeDBRef(bsonWriter, (DBRefBase) value);
        } else if (value instanceof BasicBSONList) {
            encodeIterable(bsonWriter, (BasicBSONList) value);
        } else if (value instanceof DBObject) {
            encodeEmbeddedObject(bsonWriter, ((DBObject) value).toMap());
        } else if (value instanceof Map) {
            encodeEmbeddedObject(bsonWriter, (Map<String, Object>) value);
        } else if (value instanceof Iterable) {
            encodeIterable(bsonWriter, (Iterable) value);
        } else if (value instanceof CodeWScope) {
            encodeCodeWScope(bsonWriter, (CodeWScope) value);
        } else if (value instanceof byte[]) {
            encodeByteArray(bsonWriter, (byte[]) value);
        } else if (value.getClass().isArray()) {
            encodeArray(bsonWriter, value);
        } else if (value instanceof Symbol) {
            bsonWriter.writeSymbol(((Symbol) value).getSymbol());
        } else {
            Codec codec = codecRegistry.get(value.getClass());
            codec.encode(bsonWriter, value, encoderContext);
        }
    }

    private void encodeEmbeddedObject(final BsonWriter bsonWriter, final Map<String, Object> document) {
        bsonWriter.writeStartDocument();

        for (final Map.Entry<String, Object> entry : document.entrySet()) {
            bsonWriter.writeName(entry.getKey());
            writeValue(bsonWriter, null, entry.getValue());
        }
        bsonWriter.writeEndDocument();
    }

    private void encodeByteArray(final BsonWriter bsonWriter, final byte[] value) {
        bsonWriter.writeBinaryData(new BsonBinary(value));
    }

    private void encodeArray(final BsonWriter bsonWriter, final Object value) {
        bsonWriter.writeStartArray();

        int size = Array.getLength(value);
        for (int i = 0; i < size; i++) {
            writeValue(bsonWriter, null, Array.get(value, i));
        }

        bsonWriter.writeEndArray();
    }

    private void encodeDBRef(final BsonWriter bsonWriter, final DBRefBase dbRef) {
        bsonWriter.writeStartDocument();

        bsonWriter.writeString("$ref", dbRef.getRef());
        bsonWriter.writeName("$id");
        writeValue(bsonWriter, null, dbRef.getId());

        bsonWriter.writeEndDocument();
    }

    @SuppressWarnings("unchecked")
    private void encodeCodeWScope(final BsonWriter bsonWriter, final CodeWScope value) {
        bsonWriter.writeJavaScriptWithScope(value.getCode());
        encodeEmbeddedObject(bsonWriter, value.getScope().toMap());
    }

    private void encodeIterable(final BsonWriter bsonWriter, final Iterable iterable) {
        bsonWriter.writeStartArray();
        for (final Object cur : iterable) {
            writeValue(bsonWriter, null, cur);
        }
        bsonWriter.writeEndArray();
    }

    private Object readValue(final BsonReader reader, final DecoderContext decoderContext, final String fieldName,
                             final List<String> path) {
        Object initialRetVal;
        BsonType bsonType = reader.getCurrentBsonType();

        if (bsonType.isContainer() && fieldName != null) {
            //if we got into some new context like nested document or array
            path.add(fieldName);
        }

        switch (bsonType) {
            case DOCUMENT:
                initialRetVal = verifyForDBRef(readDocument(reader, decoderContext, path));
                break;
            case ARRAY:
                initialRetVal = readArray(reader, decoderContext, path);
                break;
            case JAVASCRIPT_WITH_SCOPE: //custom for driver-compat types
                initialRetVal = readCodeWScope(reader, decoderContext, path);
                break;
            case DB_POINTER: //custom for driver-compat types
                BsonDbPointer dbPointer = reader.readDBPointer();
                initialRetVal = new DBRef(db, dbPointer.getNamespace(), dbPointer.getId());
                break;
            case BINARY:
                initialRetVal = readBinary(reader);
                break;
            case NULL:
                reader.readNull();
                initialRetVal = null;
                break;
            default:
                initialRetVal = codecRegistry.get(bsonTypeClassMap.get(bsonType)).decode(reader, decoderContext);
        }

        if (bsonType.isContainer() && fieldName != null) {
            //step out of current context to a parent
            path.remove(fieldName);
        }

        return BSON.applyDecodingHooks(initialRetVal);
    }

    private Object readBinary(final BsonReader reader) {
        BsonBinary binary = reader.readBinaryData();
        if (binary.getType() == BsonBinarySubType.BINARY.getValue()) {
            return new BinaryToByteArrayTransformer().transform(binary);
        } else if (binary.getType() == BsonBinarySubType.OLD_BINARY.getValue()) {
            return new BinaryToByteArrayTransformer().transform(binary);
        } else if (binary.getType() == BsonBinarySubType.UUID_LEGACY.getValue()) {
            return new BinaryToUUIDTransformer().transform(binary);
        } else {
            return new Binary(binary.getType(), binary.getData());
        }
    }

    private List readArray(final BsonReader reader, final DecoderContext decoderContext, final List<String> path) {
        reader.readStartArray();
        BasicDBList list = new BasicDBList();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            list.add(readValue(reader, decoderContext, null, path));   // TODO: why is this a warning?
        }
        reader.readEndArray();
        return list;
    }

    private DBObject readDocument(final BsonReader reader, final DecoderContext decoderContext, final List<String> path) {
        DBObject document = objectFactory.getInstance(path);

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            document.put(fieldName, readValue(reader, decoderContext, fieldName, path));
        }

        reader.readEndDocument();
        return document;
    }

    private CodeWScope readCodeWScope(final BsonReader reader, final DecoderContext decoderContext, final List<String> path) {
        return new CodeWScope(reader.readJavaScriptWithScope(), readDocument(reader, decoderContext, path));
    }

    private Object verifyForDBRef(final DBObject document) {
        if (document.containsField("$ref") && document.containsField("$id")) {
            return new DBRef(db, document);
        } else {
            return document;
        }
    }
}

