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

package com.mongodb.client.gridfs;

import com.mongodb.MongoGridFSException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.gridfs.model.GridFSFile;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class GridFSDownloadStreamTest {

    private final GridFSFile fileInfo = new GridFSFile(new BsonObjectId(new ObjectId()), "filename", 3L, 2,
            new Date(), new Document());

    @Test
    @DisplayName("should return the file info")
    void shouldReturnFileInfo() {
        GridFSDownloadStreamImpl downloadStream = new GridFSDownloadStreamImpl(null, fileInfo,
                mock(MongoCollection.class), null);
        assertEquals(fileInfo, downloadStream.getGridFSFile());
    }

    @Test
    @DisplayName("should query the chunks collection as expected")
    void shouldQueryChunksCollection() throws IOException {
        for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
            byte[] twoBytes = new byte[2];
            byte[] oneByte = new byte[1];
            BsonDocument findQuery = new BsonDocument("files_id", fileInfo.getId())
                    .append("n", new BsonDocument("$gte", new BsonInt32(0)));
            BsonDocument sort = new BsonDocument("n", new BsonInt32(1));
            BsonDocument chunkDocument = new BsonDocument("files_id", fileInfo.getId())
                    .append("n", new BsonInt32(0))
                    .append("data", new BsonBinary(twoBytes));
            BsonDocument secondChunkDocument = new BsonDocument("files_id", fileInfo.getId())
                    .append("n", new BsonInt32(1))
                    .append("data", new BsonBinary(oneByte));

            MongoCursor<BsonDocument> mongoCursor = mock(MongoCursor.class);
            FindIterable<BsonDocument> findIterable = mock(FindIterable.class);
            MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
            GridFSDownloadStreamImpl downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo,
                    chunksCollection, null);

            assertEquals(0, downloadStream.available());

            // Setup mocks for first query
            if (clientSession != null) {
                when(chunksCollection.find(clientSession, findQuery)).thenReturn(findIterable);
            } else {
                when(chunksCollection.find(findQuery)).thenReturn(findIterable);
            }
            when(findIterable.sort(sort)).thenReturn(findIterable);
            when(findIterable.batchSize(0)).thenReturn(findIterable);
            when(findIterable.iterator()).thenReturn(mongoCursor);
            when(mongoCursor.hasNext()).thenReturn(true, true, false);
            when(mongoCursor.next()).thenReturn(chunkDocument, secondChunkDocument);

            int result = downloadStream.read();
            assertEquals(twoBytes[0] & 0xFF, result);
            assertEquals(1, downloadStream.available());

            result = downloadStream.read();
            assertEquals(twoBytes[1] & 0xFF, result);
            assertEquals(0, downloadStream.available());

            result = downloadStream.read();
            assertEquals(oneByte[0] & 0xFF, result);

            result = downloadStream.read();
            assertEquals(-1, result);
        }
    }

    @Test
    @DisplayName("should handle negative skip value correctly")
    void shouldHandleNegativeSkip() throws IOException {
        for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
            GridFSDownloadStreamImpl downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo,
                    mock(MongoCollection.class), null);
            assertEquals(0L, downloadStream.skip(-1));
        }
    }

    @Test
    @DisplayName("should handle skip that is larger or equal to the file length")
    void shouldHandleLargeSkip() throws IOException {
        for (long skipValue : new long[]{3, 100}) {
            for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
                MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
                GridFSDownloadStreamImpl downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo,
                        chunksCollection, null);

                long result = downloadStream.skip(skipValue);
                assertEquals(3L, result);
                verify(chunksCollection, never()).find(any(BsonDocument.class));

                assertEquals(-1, downloadStream.read());
            }
        }
    }

    @Test
    @DisplayName("should throw if trying to pass negative batchSize")
    void shouldThrowOnNegativeBatchSize() {
        for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
            GridFSDownloadStreamImpl downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo,
                    mock(MongoCollection.class), null);

            assertDoesNotThrow(() -> downloadStream.batchSize(0));
            assertThrows(IllegalArgumentException.class, () -> downloadStream.batchSize(-1));
        }
    }

    @Test
    @DisplayName("should throw if no chunks found when data is expected")
    void shouldThrowIfNoChunksFound() {
        for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
            MongoCursor<BsonDocument> mongoCursor = mock(MongoCursor.class);
            FindIterable<BsonDocument> findIterable = mock(FindIterable.class);
            MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
            GridFSDownloadStreamImpl downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo,
                    chunksCollection, null);

            if (clientSession != null) {
                when(chunksCollection.find(any(ClientSession.class), any(BsonDocument.class))).thenReturn(findIterable);
            } else {
                when(chunksCollection.find(any(BsonDocument.class))).thenReturn(findIterable);
            }
            when(findIterable.sort(any())).thenReturn(findIterable);
            when(findIterable.batchSize(0)).thenReturn(findIterable);
            when(findIterable.iterator()).thenReturn(mongoCursor);
            when(mongoCursor.hasNext()).thenReturn(false);

            assertThrows(MongoGridFSException.class, () -> downloadStream.read());
        }
    }

    @Test
    @DisplayName("should throw if chunk data differs from the expected")
    void shouldThrowIfChunkDataDiffers() {
        for (byte[] data : new byte[][]{new byte[1], new byte[100]}) {
            for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
                BsonDocument chunkDocument = new BsonDocument("files_id", fileInfo.getId())
                        .append("n", new BsonInt32(0))
                        .append("data", new BsonBinary(data));

                MongoCursor<BsonDocument> mongoCursor = mock(MongoCursor.class);
                FindIterable<BsonDocument> findIterable = mock(FindIterable.class);
                MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
                GridFSDownloadStreamImpl downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo,
                        chunksCollection, null);

                if (clientSession != null) {
                    when(chunksCollection.find(any(ClientSession.class), any(BsonDocument.class))).thenReturn(findIterable);
                } else {
                    when(chunksCollection.find(any(BsonDocument.class))).thenReturn(findIterable);
                }
                when(findIterable.sort(any())).thenReturn(findIterable);
                when(findIterable.batchSize(0)).thenReturn(findIterable);
                when(findIterable.iterator()).thenReturn(mongoCursor);
                when(mongoCursor.hasNext()).thenReturn(true);
                when(mongoCursor.next()).thenReturn(chunkDocument);

                assertThrows(MongoGridFSException.class, () -> downloadStream.read());
            }
        }
    }

    @Test
    @DisplayName("should throw an exception when trying to action post close")
    void shouldThrowExceptionPostClose() {
        for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
            GridFSDownloadStreamImpl downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo,
                    mock(MongoCollection.class), null);
            downloadStream.close();

            assertThrows(MongoGridFSException.class, () -> downloadStream.read());
            assertThrows(MongoGridFSException.class, () -> downloadStream.skip(10));
            assertThrows(MongoGridFSException.class, () -> downloadStream.reset());
            assertThrows(MongoGridFSException.class, () -> downloadStream.read(new byte[10]));
            assertThrows(MongoGridFSException.class, () -> downloadStream.read(new byte[10], 0, 10));
        }
    }

    @Test
    @DisplayName("should not throw an exception when trying to mark post close")
    void shouldNotThrowOnMarkPostClose() {
        for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
            GridFSDownloadStreamImpl downloadStream = new GridFSDownloadStreamImpl(clientSession, fileInfo,
                    mock(MongoCollection.class), null);
            downloadStream.close();

            assertDoesNotThrow(() -> downloadStream.mark());
            assertDoesNotThrow(() -> downloadStream.mark(1));
        }
    }
}
