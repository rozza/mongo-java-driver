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

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoGridFSException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.internal.MongoDatabaseImpl;
import com.mongodb.client.internal.OperationExecutor;
import com.mongodb.client.internal.TestOperationExecutor;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.FindOperation;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.CustomMatchers.isTheSameAs;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.ReadPreference.secondary;
import static org.bson.UuidRepresentation.JAVA_LEGACY;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class GridFSBucketTest {

    private final ReadConcern readConcern = ReadConcern.DEFAULT;
    private final CodecRegistry registry = MongoClientSettings.getDefaultCodecRegistry();

    private MongoDatabaseImpl databaseWithExecutor(final OperationExecutor executor) {
        return new MongoDatabaseImpl("test", registry, primary(), WriteConcern.ACKNOWLEDGED, false, false,
                readConcern, JAVA_LEGACY, null, TIMEOUT_SETTINGS, executor);
    }

    @Test
    @DisplayName("should return the correct bucket name")
    void shouldReturnCorrectBucketName() {
        MongoDatabaseImpl database = databaseWithExecutor(mock(OperationExecutor.class));
        assertEquals("fs", new GridFSBucketImpl(database).getBucketName());
        assertEquals("custom", new GridFSBucketImpl(database, "custom").getBucketName());
    }

    @Test
    @DisplayName("should behave correctly when using withChunkSizeBytes")
    void shouldBehaveCorrectlyWithChunkSizeBytes() {
        MongoDatabaseImpl database = databaseWithExecutor(mock(OperationExecutor.class));
        GridFSBucket gridFSBucket = new GridFSBucketImpl(database).withChunkSizeBytes(200);
        assertEquals(200, gridFSBucket.getChunkSizeBytes());
    }

    @Test
    @DisplayName("should behave correctly when using withReadPreference")
    void shouldBehaveCorrectlyWithReadPreference() {
        MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
        MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
        when(filesCollection.withReadPreference(secondary())).thenReturn(filesCollection);
        when(chunksCollection.withReadPreference(secondary())).thenReturn(chunksCollection);

        GridFSBucket gridFSBucket = new GridFSBucketImpl("fs", 255, filesCollection, chunksCollection)
                .withReadPreference(secondary());
        verify(filesCollection).withReadPreference(secondary());
        verify(chunksCollection).withReadPreference(secondary());
    }

    @Test
    @DisplayName("should behave correctly when using withWriteConcern")
    void shouldBehaveCorrectlyWithWriteConcern() {
        MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
        MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
        when(filesCollection.withWriteConcern(WriteConcern.MAJORITY)).thenReturn(filesCollection);
        when(chunksCollection.withWriteConcern(WriteConcern.MAJORITY)).thenReturn(chunksCollection);

        new GridFSBucketImpl("fs", 255, filesCollection, chunksCollection)
                .withWriteConcern(WriteConcern.MAJORITY);
        verify(filesCollection).withWriteConcern(WriteConcern.MAJORITY);
        verify(chunksCollection).withWriteConcern(WriteConcern.MAJORITY);
    }

    @Test
    @DisplayName("should behave correctly when using withReadConcern")
    void shouldBehaveCorrectlyWithReadConcern() {
        MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
        MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
        when(filesCollection.withReadConcern(ReadConcern.MAJORITY)).thenReturn(filesCollection);
        when(chunksCollection.withReadConcern(ReadConcern.MAJORITY)).thenReturn(chunksCollection);
        when(filesCollection.getReadConcern()).thenReturn(ReadConcern.MAJORITY);

        GridFSBucket gridFSBucket = new GridFSBucketImpl("fs", 255, filesCollection, chunksCollection)
                .withReadConcern(ReadConcern.MAJORITY);
        assertEquals(ReadConcern.MAJORITY, gridFSBucket.getReadConcern());
    }

    @Test
    @DisplayName("should get defaults from MongoDatabase")
    void shouldGetDefaultsFromDatabase() {
        int defaultChunkSizeBytes = 255 * 1024;
        MongoDatabaseImpl database = new MongoDatabaseImpl("test", fromProviders(new DocumentCodecProvider()),
                secondary(), WriteConcern.ACKNOWLEDGED, false, false, readConcern, JAVA_LEGACY, null,
                new TimeoutSettings(0, 0, 0, null, 0), new TestOperationExecutor(Collections.emptyList()));

        GridFSBucket gridFSBucket = new GridFSBucketImpl(database);
        assertEquals(defaultChunkSizeBytes, gridFSBucket.getChunkSizeBytes());
        assertEquals(database.getReadPreference(), gridFSBucket.getReadPreference());
        assertEquals(database.getWriteConcern(), gridFSBucket.getWriteConcern());
        assertEquals(database.getReadConcern(), gridFSBucket.getReadConcern());
    }

    @Test
    @DisplayName("should create the expected GridFSUploadStream")
    void shouldCreateExpectedUploadStream() {
        for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
            MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
            MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
            when(filesCollection.getTimeout(TimeUnit.MILLISECONDS)).thenReturn(null);
            when(chunksCollection.getTimeout(TimeUnit.MILLISECONDS)).thenReturn(null);

            FindIterable<Document> findIterable = mock(FindIterable.class);
            MongoCollection<Document> docFilesCollection = mock(MongoCollection.class);
            when(filesCollection.withDocumentClass(Document.class)).thenReturn(docFilesCollection);
            when(docFilesCollection.withReadPreference(primary())).thenReturn(docFilesCollection);
            when(docFilesCollection.find()).thenReturn(findIterable);
            when(docFilesCollection.find(any(ClientSession.class))).thenReturn(findIterable);
            when(findIterable.projection(new Document("_id", 1))).thenReturn(findIterable);
            when(findIterable.first()).thenReturn(new Document());

            GridFSBucketImpl gridFSBucket = new GridFSBucketImpl("fs", 255, filesCollection, chunksCollection);

            GridFSUploadStream stream;
            if (clientSession != null) {
                stream = gridFSBucket.openUploadStream(clientSession, "filename");
            } else {
                stream = gridFSBucket.openUploadStream("filename");
            }
            assertThat(stream, isTheSameAs(new GridFSUploadStreamImpl(clientSession, filesCollection,
                    chunksCollection, stream.getId(), "filename", 255, null, null),
                    Arrays.asList("closeLock", "buffer")));
        }
    }

    @Test
    @DisplayName("should create the expected GridFSDownloadStream")
    void shouldCreateExpectedDownloadStream() {
        for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
            BsonObjectId fileId = new BsonObjectId(new ObjectId());
            GridFSFile fileInfo = new GridFSFile(fileId, "File 1", 10, 255, new Date(), new Document());
            FindIterable<GridFSFile> findIterable = mock(FindIterable.class);
            MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
            when(filesCollection.getTimeout(TimeUnit.MILLISECONDS)).thenReturn(null);
            MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
            when(chunksCollection.getTimeout(TimeUnit.MILLISECONDS)).thenReturn(null);
            GridFSBucketImpl gridFSBucket = new GridFSBucketImpl("fs", 255, filesCollection, chunksCollection);

            if (clientSession != null) {
                when(filesCollection.find(clientSession)).thenReturn(findIterable);
            } else {
                when(filesCollection.find()).thenReturn(findIterable);
            }
            when(findIterable.filter(any())).thenReturn(findIterable);
            when(findIterable.first()).thenReturn(fileInfo);

            GridFSDownloadStream stream;
            if (clientSession != null) {
                stream = gridFSBucket.openDownloadStream(clientSession, fileId.getValue());
            } else {
                stream = gridFSBucket.openDownloadStream(fileId.getValue());
            }
            assertThat(stream, isTheSameAs(new GridFSDownloadStreamImpl(clientSession, fileInfo,
                    chunksCollection, null), Arrays.asList("closeLock", "cursorLock")));
        }
    }

    @Test
    @DisplayName("should throw an exception if file not found")
    void shouldThrowIfFileNotFound() {
        for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
            ObjectId fileId = new ObjectId();
            BsonObjectId bsonFileId = new BsonObjectId(fileId);
            FindIterable<GridFSFile> findIterable = mock(FindIterable.class);
            MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
            when(filesCollection.getTimeout(TimeUnit.MILLISECONDS)).thenReturn(null);
            MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
            GridFSBucketImpl gridFSBucket = new GridFSBucketImpl("fs", 255, filesCollection, chunksCollection);

            if (clientSession != null) {
                when(filesCollection.find(clientSession)).thenReturn(findIterable);
            } else {
                when(filesCollection.find()).thenReturn(findIterable);
            }
            when(findIterable.filter(new Document("_id", bsonFileId))).thenReturn(findIterable);
            when(findIterable.first()).thenReturn(null);

            if (clientSession != null) {
                assertThrows(MongoGridFSException.class, () -> gridFSBucket.openDownloadStream(clientSession, fileId));
            } else {
                assertThrows(MongoGridFSException.class, () -> gridFSBucket.openDownloadStream(fileId));
            }
        }
    }

    @Test
    @DisplayName("should create the expected GridFSDownloadStream when opening by name with version")
    void shouldCreateDownloadStreamByNameWithVersion() {
        String filename = "filename";
        ObjectId fileId = new ObjectId();
        BsonObjectId bsonFileId = new BsonObjectId(fileId);
        GridFSFile fileInfo = new GridFSFile(bsonFileId, filename, 10, 255, new Date(), new Document());
        FindIterable<GridFSFile> findIterable = mock(FindIterable.class);
        MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
        when(filesCollection.getTimeout(TimeUnit.MILLISECONDS)).thenReturn(null);
        MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
        when(chunksCollection.getTimeout(TimeUnit.MILLISECONDS)).thenReturn(null);
        GridFSBucketImpl gridFSBucket = new GridFSBucketImpl("fs", 255, filesCollection, chunksCollection);

        // version, expectedSkip, expectedSortOrder
        int[][] testCases = {
                {0, 0, 1}, {1, 1, 1}, {2, 2, 1}, {3, 3, 1},
                {-1, 0, -1}, {-2, 1, -1}, {-3, 2, -1}
        };

        for (int[] testCase : testCases) {
            int version = testCase[0];
            int skip = testCase[1];
            int sortOrder = testCase[2];

            when(filesCollection.find()).thenReturn(findIterable);
            when(findIterable.filter(new Document("filename", filename))).thenReturn(findIterable);
            when(findIterable.skip(skip)).thenReturn(findIterable);
            when(findIterable.sort(new Document("uploadDate", sortOrder))).thenReturn(findIterable);
            when(findIterable.first()).thenReturn(fileInfo);

            GridFSDownloadStream stream = gridFSBucket.openDownloadStream(filename,
                    new GridFSDownloadOptions().revision(version));
            assertThat(stream, isTheSameAs(new GridFSDownloadStreamImpl(null, fileInfo, chunksCollection, null),
                    Arrays.asList("closeLock", "cursorLock")));
        }
    }

    @Test
    @DisplayName("should create the expected GridFSFindIterable")
    void shouldCreateExpectedFindIterable() {
        for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
            MongoCollection<GridFSFile> collection = mock(MongoCollection.class);
            when(collection.getTimeout(TimeUnit.MILLISECONDS)).thenReturn(null);
            FindIterable<GridFSFile> findIterable = mock(FindIterable.class);
            GridFSBucketImpl gridFSBucket = new GridFSBucketImpl("fs", 255, collection, mock(MongoCollection.class));

            if (clientSession != null) {
                when(collection.find(clientSession)).thenReturn(findIterable);
            } else {
                when(collection.find()).thenReturn(findIterable);
            }

            GridFSFindIterable result;
            if (clientSession != null) {
                result = gridFSBucket.find(clientSession);
            } else {
                result = gridFSBucket.find();
            }
            assertThat(result, isTheSameAs(new GridFSFindIterableImpl(findIterable)));
        }
    }

    @Test
    @DisplayName("should execute the expected FindOperation when finding a file")
    void shouldExecuteExpectedFindOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(mock(BatchCursor.class), mock(BatchCursor.class)));
        MongoDatabaseImpl database = databaseWithExecutor(executor);
        GridFSBucket gridFSBucket = new GridFSBucketImpl(database);
        GridFSFile decoder = null; // we check the operation, not the result

        gridFSBucket.find().iterator();
        assertEquals(primary(), executor.getReadPreference());
        assertThat(executor.getReadOperation(), isTheSameAs(new FindOperation<>(
                new MongoNamespace("test.fs.files"), registry.get(GridFSFile.class))
                .filter(new BsonDocument())));

        BsonDocument filter = new BsonDocument("filename", new BsonString("filename"));
        gridFSBucket.withReadPreference(secondary()).withReadConcern(ReadConcern.MAJORITY).find(filter).iterator();
        assertEquals(secondary(), executor.getReadPreference());
        assertThat(executor.getReadOperation(), isTheSameAs(new FindOperation<>(
                new MongoNamespace("test.fs.files"), registry.get(GridFSFile.class)).filter(filter)));
    }

    @Test
    @DisplayName("should throw an exception if file not found when opening by name")
    void shouldThrowIfFileNotFoundByName() {
        for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
            MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
            when(filesCollection.getTimeout(TimeUnit.MILLISECONDS)).thenReturn(null);
            FindIterable<GridFSFile> findIterable = mock(FindIterable.class);
            MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
            GridFSBucketImpl gridFSBucket = new GridFSBucketImpl("fs", 255, filesCollection, chunksCollection);

            if (clientSession != null) {
                when(filesCollection.find(clientSession)).thenReturn(findIterable);
            } else {
                when(filesCollection.find()).thenReturn(findIterable);
            }
            when(findIterable.filter(new Document("filename", "filename"))).thenReturn(findIterable);
            when(findIterable.skip(0)).thenReturn(findIterable);
            when(findIterable.sort(new Document("uploadDate", -1))).thenReturn(findIterable);
            when(findIterable.first()).thenReturn(null);

            if (clientSession != null) {
                assertThrows(MongoGridFSException.class, () -> gridFSBucket.openDownloadStream(clientSession, "filename"));
            } else {
                assertThrows(MongoGridFSException.class, () -> gridFSBucket.openDownloadStream("filename"));
            }
        }
    }

    @Test
    @DisplayName("should delete from files collection then chunks collection")
    void shouldDeleteCorrectly() {
        ObjectId fileId = new ObjectId();
        MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
        when(filesCollection.getTimeout(TimeUnit.MILLISECONDS)).thenReturn(null);
        MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
        GridFSBucketImpl gridFSBucket = new GridFSBucketImpl("fs", 255, filesCollection, chunksCollection);

        when(filesCollection.deleteOne(new BsonDocument("_id", new BsonObjectId(fileId)))).thenReturn(DeleteResult.acknowledged(1));

        gridFSBucket.delete(fileId);
        verify(filesCollection).deleteOne(new BsonDocument("_id", new BsonObjectId(fileId)));
        verify(chunksCollection).deleteMany(new BsonDocument("files_id", new BsonObjectId(fileId)));
    }

    @Test
    @DisplayName("should throw an exception when deleting if no record in the files collection")
    void shouldThrowWhenDeletingNonExistent() {
        ObjectId fileId = new ObjectId();
        MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
        when(filesCollection.getTimeout(TimeUnit.MILLISECONDS)).thenReturn(null);
        MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
        GridFSBucketImpl gridFSBucket = new GridFSBucketImpl("fs", 255, filesCollection, chunksCollection);

        when(filesCollection.deleteOne(new BsonDocument("_id", new BsonObjectId(fileId)))).thenReturn(DeleteResult.acknowledged(0));

        assertThrows(MongoGridFSException.class, () -> gridFSBucket.delete(fileId));
        verify(chunksCollection).deleteMany(new BsonDocument("files_id", new BsonObjectId(fileId)));
    }

    @Test
    @DisplayName("should rename a file")
    void shouldRenameFile() {
        ObjectId id = new ObjectId();
        BsonObjectId fileId = new BsonObjectId(id);
        MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
        when(filesCollection.getTimeout(TimeUnit.MILLISECONDS)).thenReturn(null);
        String newFilename = "newFilename";
        GridFSBucketImpl gridFSBucket = new GridFSBucketImpl("fs", 255, filesCollection, mock(MongoCollection.class));

        when(filesCollection.updateOne(any(BsonDocument.class), any(BsonDocument.class)))
                .thenReturn(UpdateResult.unacknowledged());

        gridFSBucket.rename(id, newFilename);
        verify(filesCollection).updateOne(new BsonDocument("_id", fileId),
                new BsonDocument("$set", new BsonDocument("filename", new BsonString(newFilename))));

        gridFSBucket.rename(fileId, newFilename);
        verify(filesCollection, times(2)).updateOne(new BsonDocument("_id", fileId),
                new BsonDocument("$set", new BsonDocument("filename", new BsonString(newFilename))));
    }

    @Test
    @DisplayName("should throw an exception renaming non existent file")
    void shouldThrowWhenRenamingNonExistent() {
        ObjectId fileId = new ObjectId();
        MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
        when(filesCollection.getTimeout(TimeUnit.MILLISECONDS)).thenReturn(null);
        when(filesCollection.updateOne(any(BsonDocument.class), any(BsonDocument.class))).thenReturn(UpdateResult.acknowledged(0, 0L, null));
        GridFSBucketImpl gridFSBucket = new GridFSBucketImpl("fs", 255, filesCollection, mock(MongoCollection.class));

        assertThrows(MongoGridFSException.class, () -> gridFSBucket.rename(fileId, "newFilename"));
    }

    @Test
    @DisplayName("should be able to drop the bucket")
    void shouldDropBucket() {
        MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
        when(filesCollection.getTimeout(TimeUnit.MILLISECONDS)).thenReturn(null);
        MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
        GridFSBucketImpl gridFSBucket = new GridFSBucketImpl("fs", 255, filesCollection, chunksCollection);

        gridFSBucket.drop();
        verify(filesCollection).drop();
        verify(chunksCollection).drop();
    }

    @Test
    @DisplayName("should validate the clientSession is not null")
    void shouldValidateClientSessionNotNull() {
        ObjectId objectId = new ObjectId();
        BsonObjectId bsonValue = new BsonObjectId(objectId);
        String filename = "filename";
        MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
        MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
        GridFSBucketImpl gridFSBucket = new GridFSBucketImpl("fs", 255, filesCollection, chunksCollection);

        assertThrows(IllegalArgumentException.class, () -> gridFSBucket.delete(null, objectId));
        assertThrows(IllegalArgumentException.class, () -> gridFSBucket.downloadToStream(null, filename, mock(OutputStream.class)));
        assertThrows(IllegalArgumentException.class, () -> gridFSBucket.downloadToStream(null, objectId, mock(OutputStream.class)));
        assertThrows(IllegalArgumentException.class, () -> gridFSBucket.drop(null));
        assertThrows(IllegalArgumentException.class, () -> gridFSBucket.find((ClientSession) null));
        assertThrows(IllegalArgumentException.class, () -> gridFSBucket.openDownloadStream(null, filename));
        assertThrows(IllegalArgumentException.class, () -> gridFSBucket.openDownloadStream(null, objectId));
        assertThrows(IllegalArgumentException.class, () -> gridFSBucket.openUploadStream((ClientSession) null, filename));
        assertThrows(IllegalArgumentException.class, () -> gridFSBucket.openUploadStream((ClientSession) null, bsonValue, filename));
        assertThrows(IllegalArgumentException.class, () -> gridFSBucket.rename(null, objectId, filename));
        assertThrows(IllegalArgumentException.class, () -> gridFSBucket.uploadFromStream((ClientSession) null, filename, mock(InputStream.class)));
        assertThrows(IllegalArgumentException.class, () -> gridFSBucket.uploadFromStream((ClientSession) null, bsonValue, filename, mock(InputStream.class)));
    }

    @Test
    @DisplayName("should upload from stream")
    void shouldUploadFromStream() {
        FindIterable<Document> findIterable = mock(FindIterable.class);
        MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
        when(filesCollection.getTimeout(TimeUnit.MILLISECONDS)).thenReturn(null);
        MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
        GridFSBucketImpl gridFSBucket = new GridFSBucketImpl("fs", 255, filesCollection, chunksCollection);
        byte[] contentBytes = "content".getBytes();
        InputStream inputStream = new ByteArrayInputStream(contentBytes);

        MongoCollection<Document> docFilesCollection = mock(MongoCollection.class);
        when(filesCollection.withDocumentClass(Document.class)).thenReturn(docFilesCollection);
        when(docFilesCollection.withReadPreference(primary())).thenReturn(docFilesCollection);
        when(docFilesCollection.find()).thenReturn(findIterable);
        when(findIterable.projection(new Document("_id", 1))).thenReturn(findIterable);
        when(findIterable.first()).thenReturn(new Document());

        gridFSBucket.uploadFromStream("filename", inputStream);

        verify(chunksCollection).insertOne(any());
        verify(filesCollection).insertOne(any());
    }

    @Test
    @DisplayName("should clean up any chunks when upload from stream throws an IOException")
    void shouldCleanUpChunksOnIOException() {
        FindIterable<Document> findIterable = mock(FindIterable.class);
        MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
        when(filesCollection.getTimeout(TimeUnit.MILLISECONDS)).thenReturn(null);
        MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
        GridFSBucketImpl gridFSBucket = new GridFSBucketImpl("fs", 255, filesCollection, chunksCollection);

        InputStream inputStream = mock(InputStream.class);
        try {
            when(inputStream.read(any(byte[].class))).thenReturn(255).thenThrow(new IOException("stream failure"));
        } catch (IOException e) {
            // won't happen in mock setup
        }

        MongoCollection<Document> docFilesCollection = mock(MongoCollection.class);
        when(filesCollection.withDocumentClass(Document.class)).thenReturn(docFilesCollection);
        when(docFilesCollection.withReadPreference(primary())).thenReturn(docFilesCollection);
        when(docFilesCollection.find()).thenReturn(findIterable);
        when(findIterable.projection(new Document("_id", 1))).thenReturn(findIterable);
        when(findIterable.first()).thenReturn(new Document());

        MongoGridFSException exception = assertThrows(MongoGridFSException.class, () ->
                gridFSBucket.uploadFromStream("filename", inputStream));
        assertEquals("IOException when reading from the InputStream", exception.getMessage());

        verify(chunksCollection).insertOne(any());
        verify(chunksCollection).deleteMany(any());
        verify(filesCollection, never()).insertOne(any(GridFSFile.class));
    }

    @Test
    @DisplayName("should download to stream")
    void shouldDownloadToStream() throws IOException {
        for (ClientSession clientSession : new ClientSession[]{null, mock(ClientSession.class)}) {
            ObjectId fileId = new ObjectId();
            BsonObjectId bsonFileId = new BsonObjectId(fileId);
            GridFSFile fileInfo = new GridFSFile(bsonFileId, "filename", 10, 255, new Date(), new Document());
            MongoCursor<BsonDocument> mongoCursor = mock(MongoCursor.class);
            FindIterable<BsonDocument> findIterable = mock(FindIterable.class);
            MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
            when(filesCollection.getTimeout(TimeUnit.MILLISECONDS)).thenReturn(null);
            byte[] tenBytes = new byte[10];
            BsonDocument chunkDocument = new BsonDocument("files_id", fileInfo.getId())
                    .append("n", new BsonInt32(0))
                    .append("data", new BsonBinary(tenBytes));
            MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
            when(chunksCollection.getTimeout(TimeUnit.MILLISECONDS)).thenReturn(null);
            GridFSBucketImpl gridFSBucket = new GridFSBucketImpl("fs", 255, filesCollection, chunksCollection);

            FindIterable<GridFSFile> fileFindIterable = mock(FindIterable.class);
            if (clientSession != null) {
                when(filesCollection.find(clientSession)).thenReturn(fileFindIterable);
            } else {
                when(filesCollection.find()).thenReturn(fileFindIterable);
            }
            when(fileFindIterable.filter(new Document("_id", bsonFileId))).thenReturn(fileFindIterable);
            when(fileFindIterable.first()).thenReturn(fileInfo);

            if (clientSession != null) {
                when(chunksCollection.find(any(ClientSession.class), any(BsonDocument.class))).thenReturn(findIterable);
            } else {
                when(chunksCollection.find(any(BsonDocument.class))).thenReturn(findIterable);
            }
            when(findIterable.sort(any())).thenReturn(findIterable);
            when(findIterable.batchSize(any(int.class))).thenReturn(findIterable);
            when(findIterable.iterator()).thenReturn(mongoCursor);
            when(mongoCursor.hasNext()).thenReturn(true);
            when(mongoCursor.next()).thenReturn(chunkDocument);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(10);
            if (clientSession != null) {
                gridFSBucket.downloadToStream(clientSession, fileId, outputStream);
            } else {
                gridFSBucket.downloadToStream(fileId, outputStream);
            }
            outputStream.close();

            assertArrayEquals(tenBytes, outputStream.toByteArray());
        }
    }
}
