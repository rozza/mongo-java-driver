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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.gridfs.model.GridFSFile;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class GridFSUploadStreamTest {

    private final BsonObjectId fileId = new BsonObjectId();
    private final String filename = "filename";
    private final Document metadata = new Document();

    @Test
    @DisplayName("should return the file id")
    void shouldReturnFileId() {
        MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
        MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
        GridFSUploadStreamImpl uploadStream = new GridFSUploadStreamImpl(null, filesCollection, chunksCollection,
                fileId, filename, 255, metadata, null);
        assertEquals(fileId, uploadStream.getId());
    }

    @Test
    @DisplayName("should write the buffer when it reaches the chunk size")
    void shouldWriteBufferAtChunkSize() {
        for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
            MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
            MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
            GridFSUploadStreamImpl uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection,
                    chunksCollection, fileId, filename, 2, metadata, null);

            uploadStream.write(1);
            verify(chunksCollection, never()).insertOne(any());
            if (clientSession != null) {
                verify(chunksCollection, never()).insertOne(any(ClientSession.class), any());
            }

            uploadStream.write(1);
            if (clientSession != null) {
                verify(chunksCollection, times(1)).insertOne(any(ClientSession.class), any());
            } else {
                verify(chunksCollection, times(1)).insertOne(any(BsonDocument.class));
            }
        }
    }

    @Test
    @DisplayName("should write to the files collection on close")
    void shouldWriteToFilesCollectionOnClose() {
        for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
            MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
            MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
            GridFSUploadStreamImpl uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection,
                    chunksCollection, fileId, filename, 255, null, null);

            uploadStream.write("file content ".getBytes());
            verify(chunksCollection, never()).insertOne(any());

            uploadStream.close();
            if (clientSession != null) {
                verify(chunksCollection, times(1)).insertOne(any(ClientSession.class), any());
                verify(filesCollection, times(1)).insertOne(any(ClientSession.class), any());
            } else {
                verify(chunksCollection, times(1)).insertOne(any(BsonDocument.class));
                verify(filesCollection, times(1)).insertOne(any(GridFSFile.class));
            }
        }
    }

    @Test
    @DisplayName("should not write an empty chunk")
    void shouldNotWriteEmptyChunk() {
        for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
            MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
            MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
            GridFSUploadStreamImpl uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection,
                    chunksCollection, fileId, filename, 255, metadata, null);

            uploadStream.close();
            verify(chunksCollection, never()).insertOne(any());
            if (clientSession != null) {
                verify(filesCollection, times(1)).insertOne(any(ClientSession.class), any());
            } else {
                verify(filesCollection, times(1)).insertOne(any(GridFSFile.class));
            }
        }
    }

    @Test
    @DisplayName("should delete any chunks when calling abort")
    void shouldDeleteChunksOnAbort() {
        for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
            MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
            MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
            when(chunksCollection.deleteMany(any(Document.class))).thenReturn(null);
            if (clientSession != null) {
                when(chunksCollection.deleteMany(any(ClientSession.class), any(Document.class))).thenReturn(null);
            }
            GridFSUploadStreamImpl uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection,
                    chunksCollection, fileId, filename, 255, metadata, null);

            uploadStream.write("file content ".getBytes());
            uploadStream.abort();

            if (clientSession != null) {
                verify(chunksCollection).deleteMany(clientSession, new Document("files_id", fileId));
            } else {
                verify(chunksCollection).deleteMany(new Document("files_id", fileId));
            }
        }
    }

    @Test
    @DisplayName("should close the stream on abort")
    void shouldCloseStreamOnAbort() {
        for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
            MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
            MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
            when(chunksCollection.deleteMany(any(Document.class))).thenReturn(null);
            if (clientSession != null) {
                when(chunksCollection.deleteMany(any(ClientSession.class), any(Document.class))).thenReturn(null);
            }
            GridFSUploadStreamImpl uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection,
                    chunksCollection, fileId, filename, 255, metadata, null);
            uploadStream.write("file content ".getBytes());
            uploadStream.abort();

            assertThrows(MongoGridFSException.class, () -> uploadStream.write(1));
        }
    }

    @Test
    @DisplayName("should not do anything when calling flush")
    void shouldNotDoAnythingOnFlush() {
        for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
            MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
            GridFSUploadStreamImpl uploadStream = new GridFSUploadStreamImpl(clientSession, mock(MongoCollection.class),
                    chunksCollection, fileId, filename, 255, metadata, null);

            uploadStream.write("file content ".getBytes());
            uploadStream.flush();

            verify(chunksCollection, never()).insertOne(any());
        }
    }

    @Test
    @DisplayName("should throw an exception when trying to action post close")
    void shouldThrowExceptionPostClose() {
        for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
            MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
            MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
            GridFSUploadStreamImpl uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection,
                    chunksCollection, fileId, filename, 255, metadata, null);

            uploadStream.close();
            assertThrows(MongoGridFSException.class, () -> uploadStream.write(1));
        }
    }

    @Test
    @DisplayName("should throw an exception when calling getObjectId and the fileId is not an ObjectId")
    void shouldThrowExceptionWhenFileIdNotObjectId() {
        for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
            BsonString stringFileId = new BsonString("myFile");
            MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
            MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
            GridFSUploadStreamImpl uploadStream = new GridFSUploadStreamImpl(clientSession, filesCollection,
                    chunksCollection, stringFileId, filename, 255, metadata, null);

            assertThrows(MongoGridFSException.class, () -> uploadStream.getObjectId());
        }
    }
}
