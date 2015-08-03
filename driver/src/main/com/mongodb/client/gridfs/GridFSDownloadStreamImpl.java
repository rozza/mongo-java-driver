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
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;

import java.io.IOException;
import java.io.InputStream;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

class GridFSDownloadStreamImpl extends InputStream implements GridFSDownloadStream {
    private final BsonDocument fileInfo;
    private final MongoCollection<BsonDocument> chunksCollection;
    private final BsonObjectId fileId;
    private final long lengthInBytes;
    private final int chunkSizeInBytes;
    private final int numberOfChunks;
    private int chunkIndex;
    private int bufferOffset;
    private long currentPosition;
    private byte[] buffer = null;
    private boolean closed;

    GridFSDownloadStreamImpl(final BsonDocument fileInfo, final MongoCollection<BsonDocument> chunksCollection) {
        this.fileInfo = notNull("file information", fileInfo);
        this.chunksCollection = notNull("chunks collection", chunksCollection);

        try {
            isTrue("file information contains _id", fileInfo.containsKey("_id") && fileInfo.get("_id").isObjectId());
            isTrue("file information contains length", fileInfo.containsKey("length") && fileInfo.get("length").isInt64());
            isTrue("file information contains chunkSize", fileInfo.containsKey("chunkSize") && fileInfo.get("chunkSize").isInt32());
        } catch (IllegalStateException e) {
            throw new MongoGridFSException("GridFS file information is not in the expected format", e);
        }
        fileId = fileInfo.getObjectId("_id");
        lengthInBytes = fileInfo.getInt64("length").getValue();
        chunkSizeInBytes = fileInfo.getInt32("chunkSize").getValue();
        numberOfChunks = (int) Math.ceil((double) lengthInBytes / chunkSizeInBytes);
    }

    @Override
    public BsonDocument getFileInformation() {
        return fileInfo;
    }

    @Override
    public InputStream getInputStream() {
        return this;
    }

    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int res = read(b);
        if (res < 0) {
            return -1;
        }
        return b[0] & 0xFF;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        checkClosed();

        if (currentPosition == lengthInBytes) {
            return -1;
        } else if (buffer == null) {
            buffer = getBuffer(chunkIndex);
            bufferOffset = 0;
        } else if (bufferOffset >= buffer.length) {
            chunkIndex += 1;
            buffer = getBuffer(chunkIndex);
            bufferOffset = 0;
        }

        int r = Math.min(len, buffer.length - bufferOffset);
        System.arraycopy(buffer, bufferOffset, b, off, r);
        bufferOffset += r;
        currentPosition += r;
        return r;
    }

    @Override
    public long skip(final long bytesToSkip) throws IOException {
        checkClosed();
        if (bytesToSkip <= 0) {
            return 0;
        }

        long skippedPosition = currentPosition + bytesToSkip;
        bufferOffset = (int) skippedPosition % chunkSizeInBytes;
        if (skippedPosition > lengthInBytes) {
            chunkIndex = numberOfChunks - 1;
            currentPosition = lengthInBytes;
            buffer = null;
            return skippedPosition - (skippedPosition - lengthInBytes);
        } else {
            chunkIndex = (int) Math.floor((float) skippedPosition / chunkSizeInBytes);
            currentPosition += bytesToSkip;
            buffer = getBuffer(chunkIndex);
            return bytesToSkip;
        }
    }

    @Override
    public int available() throws IOException {
        checkClosed();
        if (buffer == null) {
            return 0;
        } else {
            return buffer.length - bufferOffset;
        }
    }

    @Override
    public void close() throws IOException {
        checkClosed();
        closed = true;
    }

    private void checkClosed() {
        if (closed) {
            throw new MongoGridFSException("The InputStream has been closed");
        }
    }

    private byte[] getBuffer(final int chunkIndexToFetch)  {
        BsonDocument chunk = chunksCollection.find(new BsonDocument("files_id", fileId)
                .append("n", new BsonInt32(chunkIndexToFetch))).first();

        if (chunk == null) {
            throw new MongoGridFSException(format("Could not find file chunk for file_id: %s at chunk index %s.",
                        fileId.getValue().toHexString(), chunkIndexToFetch));
        } else {
            byte[] data = chunk.getBinary("data").getData();
            long expectedDataLength;
            if (chunkIndexToFetch + 1 == numberOfChunks) {
                expectedDataLength = lengthInBytes - (chunkIndexToFetch * (long) chunkSizeInBytes);
            } else {
                expectedDataLength = chunkSizeInBytes;
            }
            if (data.length != expectedDataLength) {
                throw new MongoGridFSException(format("Chunk size data length is not the expected size. "
                        + "The size was %s for file_id: %s chunk index %s it should be %s bytes.",
                        data.length, fileId.getValue().toHexString(), chunkIndexToFetch, expectedDataLength));
            }
            return data;
        }
    }
}
