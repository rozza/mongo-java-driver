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

package com.mongodb.client.gridfs;

import com.mongodb.MongoGridFSException;
import com.mongodb.client.MongoCollection;
import org.bson.BsonBinary;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.types.ObjectId;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.util.Util.toHex;

final class GridFSUploadStreamImpl extends GridFSUploadStream {
    private final MongoCollection<BsonDocument> filesCollection;
    private final MongoCollection<BsonDocument> chunksCollection;
    private final ObjectId fileId;
    private final String filename;
    private final int chunkSizeBytes;
    private final BsonDocument metadata;
    private final MessageDigest md5;
    private byte[] buffer;
    private long lengthInBytes;
    private int bufferOffset;
    private int chunkIndex;

    private volatile boolean closed = false;

    GridFSUploadStreamImpl(final MongoCollection<BsonDocument> filesCollection, final MongoCollection<BsonDocument> chunksCollection,
                           final ObjectId fileId, final String filename, final int chunkSizeBytes, final BsonDocument metadata) {
        this.filesCollection = notNull("files collection", filesCollection);
        this.chunksCollection = notNull("chunks collection", chunksCollection);
        this.fileId = notNull("File Id", fileId);
        this.filename = notNull("filename", filename);
        this.chunkSizeBytes = chunkSizeBytes;
        this.metadata = metadata;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new MongoGridFSException("No MD5 message digest available, cannot upload file", e);
        }
        chunkIndex = 0;
        bufferOffset = 0;
        buffer = new byte[chunkSizeBytes];
    }

    @Override
    public ObjectId getFileId() {
        return fileId;
    }

    @Override
    public void write(final int b) {
        byte[] byteArray = new byte[1];
        byteArray[0] = (byte) (0xFF & b);
        write(byteArray, 0, 1);
    }

    @Override
    public void write(final byte[] b) {
        write(b, 0, b.length);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) {
        checkClosed();
        if (b == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > b.length) || (len < 0)
                || ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        int currentOffset = off;
        int lengthToWrite = len;
        int amountToCopy = 0;

        while (lengthToWrite > 0) {
            amountToCopy = lengthToWrite;
            if (amountToCopy > chunkSizeBytes - bufferOffset) {
                amountToCopy = chunkSizeBytes - bufferOffset;
            }
            System.arraycopy(b, currentOffset, buffer, bufferOffset, amountToCopy);

            bufferOffset += amountToCopy;
            currentOffset += amountToCopy;
            lengthToWrite -= amountToCopy;
            lengthInBytes += amountToCopy;

            if (bufferOffset == chunkSizeBytes) {
                writeChunk();
            }
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            } else {
                closed = true;
            }
        }
        writeChunk();
        BsonDocument fileDocument = new BsonDocument("_id", new BsonObjectId(fileId))
                .append("length", new BsonInt64(lengthInBytes))
                .append("chunkSize", new BsonInt32(chunkSizeBytes))
                .append("uploadDate", new BsonDateTime(System.currentTimeMillis()))
                .append("md5", new BsonString(toHex(md5.digest())))
                .append("filename", new BsonString(filename));

        if (metadata != null && !metadata.isEmpty()) {
            fileDocument.append("metadata", metadata);
        }
        filesCollection.insertOne(fileDocument);
        buffer = null;
    }

    private void writeChunk() {
        if (bufferOffset > 0) {
            chunksCollection.insertOne(
                    new BsonDocument("files_id", new BsonObjectId(fileId))
                            .append("n", new BsonInt32(chunkIndex))
                            .append("data", getData()));
            md5.update(buffer);
            chunkIndex++;
            bufferOffset = 0;
        }
    }

    private BsonBinary getData() {
        if (bufferOffset < chunkSizeBytes) {
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
