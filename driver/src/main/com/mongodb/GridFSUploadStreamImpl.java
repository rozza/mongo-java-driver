/*
 * Copyright 2015 MongoDB, Inc.
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

package com.mongodb;

import com.mongodb.client.GridFSUploadStream;
import com.mongodb.client.MongoCollection;
import org.bson.BsonBinary;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.util.Util.toHex;

final class GridFSUploadStreamImpl extends OutputStream implements GridFSUploadStream {
    private final MongoCollection<BsonDocument> filesCollection;
    private final MongoCollection<BsonDocument> chunksCollection;
    private final ObjectId fileId;
    private final String filename;
    private final int chunkSizeInBytes;
    private final BsonDocument metadata;
    private final MessageDigest md5;
    private boolean closed;
    private byte[] buffer;
    private long lengthInBytes;
    private int bufferOffset;
    private int chunkIndex;

    GridFSUploadStreamImpl(final MongoCollection<BsonDocument> filesCollection, final MongoCollection<BsonDocument> chunksCollection,
                           final ObjectId fileId, final String filename, final int chunkSizeInBytes, final BsonDocument metadata) {
        this.filesCollection = notNull("files collection", filesCollection);
        this.chunksCollection = notNull("chunks collection", chunksCollection);
        this.fileId = notNull("File Id", fileId);
        this.filename = notNull("filename", filename);
        this.chunkSizeInBytes = chunkSizeInBytes;
        this.metadata = metadata;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new MongoGridFSException("No MD5 message digest available, cannot upload file", e);
        }
        chunkIndex = 0;
        bufferOffset = 0;
        buffer = new byte[chunkSizeInBytes];
    }

    @Override
    public ObjectId getFileId() {
        return fileId;
    }

    @Override
    public OutputStream getOutputStream() {
        return this;
    }

    @Override
    public void write(final int b) throws IOException {
        checkClosed();
        buffer[bufferOffset++] = (byte) (0xFF & b);
        lengthInBytes++;
        if (bufferOffset == chunkSizeInBytes) {
            writeChunks();
        }
    }

    @Override
    public void close() throws IOException {
        checkClosed();
        writeChunks();
        closed = true;
        BsonDocument fileDocument = new BsonDocument("_id", new BsonObjectId(fileId))
                .append("length", new BsonInt64(lengthInBytes))
                .append("chunkSize", new BsonInt32(chunkSizeInBytes))
                .append("uploadDate", new BsonDateTime(System.currentTimeMillis()))
                .append("md5", new BsonString(toHex(md5.digest())))
                .append("filename", new BsonString(filename));

        if (!metadata.isEmpty()) {
            fileDocument.append("metadata", metadata);
        }
        filesCollection.insertOne(fileDocument);
        buffer = null;
    }

    private void writeChunks() throws IOException {
        if (bufferOffset > 0) {
            chunksCollection.insertOne(
                    new BsonDocument("files_id", new BsonObjectId(fileId))
                            .append("n", new BsonInt32(chunkIndex))
                            .append("data", getData()));
            md5.update(buffer);
            chunkIndex++;
            bufferOffset = 0;
            buffer = new byte[chunkSizeInBytes];
        }
    }

    private BsonBinary getData() {
        if (bufferOffset < chunkSizeInBytes) {
            byte[] sizedBuffer = new byte[bufferOffset];
            System.arraycopy(buffer, 0, sizedBuffer, 0, bufferOffset);
            buffer = sizedBuffer;
        }
        return new BsonBinary(buffer);
    }

    private void checkClosed() {
        if (closed) {
            throw new MongoGridFSException("The OutputStream has been closed");
        }
    }

}
