/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.connection;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonType;
import org.bson.ByteBuf;

import java.io.ByteArrayOutputStream;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

class ByteBufPayloadBsonDocument extends AbstractByteBufBsonDocument implements Cloneable, Serializable {
    private static final long serialVersionUID = 3L;
    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

    private final ByteBufBsonDocument commandDocument;
    private final String payloadName;
    private final List<ByteBufBsonDocument> payload;
    private BsonDocument unwrapped;


    static ByteBufPayloadBsonDocument create(final ByteBufferBsonOutput bsonOutput, final int startPosition) {
        List<ByteBuf> duplicateByteBuffers = bsonOutput.getByteBuffers();
        CompositeByteBuf outputByteBuf = new CompositeByteBuf(duplicateByteBuffers);
        outputByteBuf.position(startPosition);

        int curDocumentStartPosition = startPosition;
        int documentSizeInBytes = outputByteBuf.getInt();
        ByteBuf slice = outputByteBuf.duplicate();
        slice.position(curDocumentStartPosition);
        slice.limit(curDocumentStartPosition + documentSizeInBytes);
        ByteBufBsonDocument commandDocument = new ByteBufBsonDocument(slice);

        curDocumentStartPosition += documentSizeInBytes;

        slice = outputByteBuf.duplicate();
        slice.position(curDocumentStartPosition);
        if (slice.get() != 1) {
            throw new IllegalArgumentException("Invalid payload type");
        }
        int sliceSize = slice.remaining();
        int payloadSize = slice.getInt();
        if (sliceSize != payloadSize) {
            throw new IllegalArgumentException("Invalid payload size");
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte nextByte = slice.get();
        while (nextByte != 0) {
            outputStream.write(nextByte);
            nextByte = slice.get();
        }
        String payloadName = new String(outputStream.toByteArray(), UTF8_CHARSET);

        List<ByteBufBsonDocument> payload = new ArrayList<ByteBufBsonDocument>();
        curDocumentStartPosition = slice.position();
        while (outputByteBuf.hasRemaining()) {
            documentSizeInBytes = slice.getInt();
            slice.position(curDocumentStartPosition);
            slice.limit(curDocumentStartPosition + documentSizeInBytes);
            ByteBufBsonDocument doc = new ByteBufBsonDocument(slice);
            payload.add(doc);
            curDocumentStartPosition += documentSizeInBytes;
            outputByteBuf.position(curDocumentStartPosition);
        }

        for (ByteBuf byteBuffer : duplicateByteBuffers) {
            byteBuffer.release();
        }
        return new ByteBufPayloadBsonDocument(commandDocument, payloadName, payload);
    }

    <T> T findInDocument(final Finder<T> finder) {
        BsonDocumentReader bsonReader = new BsonDocumentReader(toBsonDocument());
        try {
            bsonReader.readStartDocument();
            while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                T found = finder.find(bsonReader);
                if (found != null) {
                    return found;
                }
            }
            bsonReader.readEndDocument();
        } finally {
            bsonReader.close();
        }
        return finder.notFound();
    }

    @Override
    public BsonDocument clone() {
        return toBsonDocument();
    }

    BsonDocument toBsonDocument() {
        if (unwrapped == null) {
            unwrapped =  commandDocument.toBsonDocument().append(payloadName, new BsonArray(payload));
        }
        return unwrapped;
    }

    ByteBufPayloadBsonDocument(final ByteBufBsonDocument commandDocument, final String payloadName,
                               final List<ByteBufBsonDocument> payload) {
        this.commandDocument = commandDocument;
        this.payloadName = payloadName;
        this.payload = payload;
    }

    // see https://docs.oracle.com/javase/6/docs/platform/serialization/spec/input.html
    private void readObject(final ObjectInputStream stream) throws InvalidObjectException {
        throw new InvalidObjectException("Proxy required");
    }
}
