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
import com.mongodb.client.FunctionalSpecification;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.UuidCodec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static com.mongodb.client.Fixture.getDefaultDatabase;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.unset;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GridFSBucketSmokeTest extends FunctionalSpecification {
    private MongoDatabase mongoDatabase;
    private MongoCollection<GridFSFile> filesCollection;
    private MongoCollection<Document> chunksCollection;
    private GridFSBucket gridFSBucket;
    private final String singleChunkString = "GridFS";
    private final String multiChunkString;

    GridFSBucketSmokeTest() {
        StringBuilder sb = new StringBuilder();
        int targetLen = 1024 * 255 * 5;
        while (sb.length() < targetLen - singleChunkString.length()) {
            sb.append(' ');
        }
        sb.append(singleChunkString);
        multiChunkString = sb.toString();
    }

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        mongoDatabase = getDefaultDatabase();
        filesCollection = mongoDatabase.getCollection("fs.files", GridFSFile.class);
        chunksCollection = mongoDatabase.getCollection("fs.chunks");
        filesCollection.drop();
        chunksCollection.drop();
        gridFSBucket = new GridFSBucketImpl(mongoDatabase);
    }

    @Override
    @AfterEach
    public void tearDown() {
        if (filesCollection != null) {
            filesCollection.drop();
            chunksCollection.drop();
        }
        super.tearDown();
    }

    @ParameterizedTest(name = "should round trip {0}")
    @MethodSource("roundTripArgs")
    @DisplayName("should round trip a file")
    void shouldRoundTripAFile(final String description, final boolean multiChunk, final int chunkCount, final boolean direct) throws IOException {
        String content = multiChunk ? multiChunkString : singleChunkString;
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        long expectedLength = contentBytes.length;
        ObjectId fileId;
        byte[] gridFSContentBytes;

        if (direct) {
            fileId = gridFSBucket.uploadFromStream("myFile", new ByteArrayInputStream(contentBytes));
        } else {
            GridFSUploadStream outputStream = gridFSBucket.openUploadStream("myFile");
            outputStream.write(contentBytes);
            outputStream.close();
            outputStream.close(); // check for close idempotency
            fileId = outputStream.getObjectId();
        }

        assertEquals(1, filesCollection.countDocuments());
        assertEquals(chunkCount, chunksCollection.countDocuments());

        GridFSFile file = filesCollection.find().first();

        assertEquals(fileId, file.getObjectId());
        assertEquals(gridFSBucket.getChunkSizeBytes(), file.getChunkSize());
        assertEquals(expectedLength, file.getLength());
        assertNull(file.getMetadata());

        if (direct) {
            gridFSContentBytes = readAllBytes(gridFSBucket.openDownloadStream(fileId));
        } else {
            ByteArrayOutputStream baos = new ByteArrayOutputStream((int) expectedLength);
            gridFSBucket.downloadToStream(fileId, baos);
            baos.close();
            gridFSContentBytes = baos.toByteArray();
        }

        assertArrayEquals(contentBytes, gridFSContentBytes);
    }

    static Stream<Arguments> roundTripArgs() {
        return Stream.of(
                Arguments.of("a small file directly", false, 1, true),
                Arguments.of("a small file to stream", false, 1, false),
                Arguments.of("a large file directly", true, 5, true),
                Arguments.of("a large file to stream", true, 5, false)
        );
    }

    @Test
    @DisplayName("should round trip with a batchSize of 1")
    void shouldRoundTripWithBatchSizeOfOne() throws IOException {
        String content = multiChunkString;
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        long expectedLength = contentBytes.length;

        ObjectId fileId = gridFSBucket.uploadFromStream("myFile", new ByteArrayInputStream(contentBytes));

        assertEquals(1, filesCollection.countDocuments());
        assertEquals(5, chunksCollection.countDocuments());

        GridFSFile file = filesCollection.find().first();

        assertEquals(fileId, file.getObjectId());
        assertEquals(gridFSBucket.getChunkSizeBytes(), file.getChunkSize());
        assertEquals(expectedLength, file.getLength());
        assertNull(file.getMetadata());

        byte[] gridFSContentBytes = readAllBytes(gridFSBucket.openDownloadStream(fileId).batchSize(1));

        assertArrayEquals(contentBytes, gridFSContentBytes);
    }

    @Test
    @DisplayName("should handle custom ids")
    void shouldHandleCustomIds() throws IOException {
        String content = multiChunkString;
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        BsonString fileId = new BsonString("myFile");

        gridFSBucket.uploadFromStream(fileId, "myFile", new ByteArrayInputStream(contentBytes));
        byte[] gridFSContentBytes = readAllBytes(gridFSBucket.openDownloadStream(fileId).batchSize(1));

        assertArrayEquals(contentBytes, gridFSContentBytes);

        gridFSBucket.rename(fileId, "newName");

        assertArrayEquals(contentBytes, readAllBytes(gridFSBucket.openDownloadStream("newName")));

        gridFSBucket.delete(fileId);

        assertEquals(0, filesCollection.countDocuments());
        assertEquals(0, chunksCollection.countDocuments());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("should use custom uploadOptions when uploading")
    void shouldUseCustomUploadOptions(final boolean direct) throws IOException {
        int chunkSize = 20;
        Document metadata = new Document("archived", false);
        GridFSUploadOptions options = new GridFSUploadOptions()
                .chunkSizeBytes(chunkSize)
                .metadata(metadata);
        StringBuilder sb = new StringBuilder(6144);
        for (int i = 0; i < 1024; i++) {
            sb.append("qwerty");
        }
        String content = sb.toString();
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        long expectedLength = contentBytes.length;
        int expectedNoChunks = (int) Math.ceil((double) expectedLength / chunkSize);
        BsonValue fileId;
        byte[] gridFSContentBytes;

        if (direct) {
            fileId = new BsonObjectId(gridFSBucket.uploadFromStream("myFile", new ByteArrayInputStream(contentBytes), options));
        } else {
            GridFSUploadStream outputStream = gridFSBucket.openUploadStream("myFile", options);
            outputStream.write(contentBytes);
            outputStream.close();
            fileId = outputStream.getId();
        }

        assertEquals(1, filesCollection.countDocuments());
        assertEquals(expectedNoChunks, chunksCollection.countDocuments());

        GridFSFile fileInfo = filesCollection.find().first();

        assertEquals(fileId, fileInfo.getId());
        assertEquals(options.getChunkSizeBytes(), fileInfo.getChunkSize());
        assertEquals(expectedLength, fileInfo.getLength());
        assertEquals(options.getMetadata(), fileInfo.getMetadata());

        if (direct) {
            gridFSContentBytes = readAllBytes(gridFSBucket.openDownloadStream(fileId));
        } else {
            ByteArrayOutputStream baos = new ByteArrayOutputStream((int) expectedLength);
            gridFSBucket.downloadToStream(fileId, baos);
            baos.close();
            gridFSContentBytes = baos.toByteArray();
        }

        assertArrayEquals(contentBytes, gridFSContentBytes);
    }

    @Test
    @DisplayName("should be able to open by name")
    void shouldBeAbleToOpenByName() throws IOException {
        String content = "Hello GridFS";
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        String filename = "myFile";
        gridFSBucket.uploadFromStream(filename, new ByteArrayInputStream(contentBytes));

        // Direct to a stream
        byte[] gridFSContentBytes = readAllBytes(gridFSBucket.openDownloadStream(filename));
        assertArrayEquals(contentBytes, gridFSContentBytes);

        // To supplied stream
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(contentBytes.length);
        gridFSBucket.downloadToStream(filename, outputStream);
        outputStream.close();
        gridFSContentBytes = outputStream.toByteArray();
        assertArrayEquals(contentBytes, gridFSContentBytes);
    }

    @ParameterizedTest(name = "should be able to open by name with selected version: {0}")
    @MethodSource("versionValues")
    @DisplayName("should be able to open by name with selected version")
    void shouldBeAbleToOpenByNameWithSelectedVersion(final int version) throws IOException {
        List<byte[]> contentBytes = new ArrayList<>();
        for (int i = 0; i <= 3; i++) {
            contentBytes.add(("Hello GridFS - " + i).getBytes(StandardCharsets.UTF_8));
        }
        String filename = "myFile";
        for (byte[] bytes : contentBytes) {
            gridFSBucket.uploadFromStream(filename, new ByteArrayInputStream(bytes));
        }
        int index = version >= 0 ? version : contentBytes.size() + version;
        byte[] expectedContentBytes = contentBytes.get(index);
        GridFSDownloadOptions options = new GridFSDownloadOptions().revision(version);

        // Direct to a stream
        byte[] gridFSContentBytes = readAllBytes(gridFSBucket.openDownloadStream(filename, options));
        assertArrayEquals(expectedContentBytes, gridFSContentBytes);

        // To supplied stream
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(expectedContentBytes.length);
        gridFSBucket.downloadToStream(filename, outputStream, options);
        outputStream.close();
        gridFSContentBytes = outputStream.toByteArray();
        assertArrayEquals(expectedContentBytes, gridFSContentBytes);
    }

    static Stream<Integer> versionValues() {
        return Stream.of(0, 1, 2, 3, -1, -2, -3, -4);
    }

    @Test
    @DisplayName("should throw an exception if cannot open by name")
    void shouldThrowIfCannotOpenByName() {
        String filename = "FileDoesNotExist";

        // Direct to a stream
        assertThrows(MongoGridFSException.class, () -> gridFSBucket.openDownloadStream(filename));

        // To supplied stream
        assertThrows(MongoGridFSException.class, () ->
                gridFSBucket.downloadToStream(filename, new ByteArrayOutputStream(1024)));
    }

    @Test
    @DisplayName("should throw an exception if cannot open by name with selected version")
    void shouldThrowIfCannotOpenByNameWithSelectedVersion() {
        String filename = "myFile";
        GridFSDownloadOptions options = new GridFSDownloadOptions().revision(1);
        gridFSBucket.uploadFromStream(filename, new ByteArrayInputStream("Hello GridFS".getBytes(StandardCharsets.UTF_8)));

        // Direct to a stream
        assertThrows(MongoGridFSException.class, () -> gridFSBucket.openDownloadStream(filename, options));

        // To supplied stream
        assertThrows(MongoGridFSException.class, () ->
                gridFSBucket.downloadToStream(filename, new ByteArrayOutputStream(1024), options));
    }

    @Test
    @DisplayName("should delete a file")
    void shouldDeleteAFile() {
        String filename = "myFile";

        ObjectId fileId = gridFSBucket.uploadFromStream(filename,
                new ByteArrayInputStream("Hello GridFS".getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, filesCollection.countDocuments());
        assertEquals(1, chunksCollection.countDocuments());

        gridFSBucket.delete(fileId);

        assertEquals(0, filesCollection.countDocuments());
        assertEquals(0, chunksCollection.countDocuments());
    }

    @Test
    @DisplayName("should throw when deleting nonexistent file")
    void shouldThrowWhenDeletingNonexistentFile() {
        assertThrows(MongoGridFSException.class, () -> gridFSBucket.delete(new ObjectId()));
    }

    @Test
    @DisplayName("should delete a file data orphan chunks")
    void shouldDeleteFileDataOrphanChunks() {
        String filename = "myFile";
        ObjectId fileId = gridFSBucket.uploadFromStream(filename,
                new ByteArrayInputStream("Hello GridFS".getBytes(StandardCharsets.UTF_8)));

        filesCollection.drop();

        assertEquals(0, filesCollection.countDocuments());
        assertEquals(1, chunksCollection.countDocuments());

        assertThrows(MongoGridFSException.class, () -> gridFSBucket.delete(fileId));

        assertEquals(0, filesCollection.countDocuments());
        assertEquals(0, chunksCollection.countDocuments());
    }

    @Test
    @DisplayName("should rename a file")
    void shouldRenameAFile() throws IOException {
        String filename = "myFile";
        String newFileName = "newFileName";

        ObjectId fileId = gridFSBucket.uploadFromStream(filename,
                new ByteArrayInputStream("Hello GridFS".getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, filesCollection.countDocuments());
        assertEquals(1, chunksCollection.countDocuments());

        gridFSBucket.rename(fileId, "newFileName");

        assertEquals(1, filesCollection.countDocuments());
        assertEquals(1, chunksCollection.countDocuments());

        // Should not throw
        assertDoesNotThrow(() -> gridFSBucket.openDownloadStream(newFileName));
    }

    @Test
    @DisplayName("should throw an exception when rename a nonexistent file")
    void shouldThrowWhenRenamingNonexistentFile() {
        assertThrows(MongoGridFSException.class, () -> gridFSBucket.rename(new ObjectId(), "newFileName"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("should only create indexes on first write")
    void shouldOnlyCreateIndexesOnFirstWrite(final boolean direct) {
        byte[] contentBytes = "Hello GridFS".getBytes(StandardCharsets.UTF_8);

        assertEquals(0, filesCollection.listIndexes().into(new ArrayList<>()).size());
        assertEquals(0, chunksCollection.listIndexes().into(new ArrayList<>()).size());

        if (direct) {
            gridFSBucket.uploadFromStream("myFile", new ByteArrayInputStream(contentBytes));
        } else {
            GridFSUploadStream outputStream = gridFSBucket.openUploadStream("myFile");
            outputStream.write(contentBytes);
            outputStream.close();
        }

        assertEquals(2, filesCollection.listIndexes().into(new ArrayList<>()).size());
        assertEquals(2, chunksCollection.listIndexes().into(new ArrayList<>()).size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("should not create indexes if the files collection is not empty")
    void shouldNotCreateIndexesIfFilesCollectionNotEmpty(final boolean direct) {
        filesCollection.withDocumentClass(Document.class).insertOne(new Document("filename", "bad file"));
        byte[] contentBytes = "Hello GridFS".getBytes(StandardCharsets.UTF_8);

        assertEquals(1, filesCollection.listIndexes().into(new ArrayList<>()).size());
        assertEquals(0, chunksCollection.listIndexes().into(new ArrayList<>()).size());

        if (direct) {
            gridFSBucket.uploadFromStream("myFile", new ByteArrayInputStream(contentBytes));
        } else {
            GridFSUploadStream outputStream = gridFSBucket.openUploadStream("myFile");
            outputStream.write(contentBytes);
            outputStream.close();
        }

        assertEquals(1, filesCollection.listIndexes().into(new ArrayList<>()).size());
        assertEquals(1, chunksCollection.listIndexes().into(new ArrayList<>()).size());
    }

    @ParameterizedTest(name = "should not create if index is numerically the same: direct={0}, v1={1}, v2={2}")
    @MethodSource("indexCombinations")
    @DisplayName("should not create if index is numerically the same")
    void shouldNotCreateIfIndexIsNumericallyTheSame(final boolean direct, final Number indexValue1, final Number indexValue2) {
        filesCollection.createIndex(new Document("filename", indexValue1).append("uploadDate", indexValue2));
        chunksCollection.createIndex(new Document("files_id", indexValue1).append("n", indexValue2));
        byte[] contentBytes = "Hello GridFS".getBytes(StandardCharsets.UTF_8);

        assertEquals(2, filesCollection.listIndexes().into(new ArrayList<>()).size());
        assertEquals(2, chunksCollection.listIndexes().into(new ArrayList<>()).size());

        if (direct) {
            gridFSBucket.uploadFromStream("myFile", new ByteArrayInputStream(contentBytes));
        } else {
            GridFSUploadStream outputStream = gridFSBucket.openUploadStream("myFile");
            outputStream.write(contentBytes);
            outputStream.close();
        }

        assertEquals(2, filesCollection.listIndexes().into(new ArrayList<>()).size());
        assertEquals(2, chunksCollection.listIndexes().into(new ArrayList<>()).size());
    }

    static Stream<Arguments> indexCombinations() {
        List<Boolean> directValues = Arrays.asList(true, false);
        List<Number> numericValues = Arrays.asList(1, 1.0, 1L);
        List<Arguments> args = new ArrayList<>();
        for (boolean direct : directValues) {
            for (Number v1 : numericValues) {
                for (Number v2 : numericValues) {
                    args.add(Arguments.of(direct, v1, v2));
                }
            }
        }
        return args.stream();
    }

    @Test
    @DisplayName("should mark and reset")
    void shouldMarkAndReset() throws IOException {
        byte[] content = new byte[1000];
        for (int i = 0; i < 1000; i++) {
            content[i] = (byte) (i + 1);
        }
        byte[] readByte = new byte[500];

        ObjectId fileId = gridFSBucket.uploadFromStream("myFile", new ByteArrayInputStream(content),
                new GridFSUploadOptions().chunkSizeBytes(500));

        assertEquals(1, filesCollection.countDocuments());
        assertEquals(2, chunksCollection.countDocuments());

        GridFSDownloadStream gridFSDownloadStream = gridFSBucket.openDownloadStream(fileId);
        gridFSDownloadStream.read(readByte);

        byte[] expectedFirst500 = new byte[500];
        for (int i = 0; i < 500; i++) {
            expectedFirst500[i] = (byte) (i + 1);
        }
        assertArrayEquals(expectedFirst500, readByte);

        gridFSDownloadStream.mark();

        gridFSDownloadStream.read(readByte);

        byte[] expectedLast500 = new byte[500];
        for (int i = 0; i < 500; i++) {
            expectedLast500[i] = (byte) (i + 501);
        }
        assertArrayEquals(expectedLast500, readByte);

        gridFSDownloadStream.reset();

        gridFSDownloadStream.read(readByte);
        assertArrayEquals(expectedLast500, readByte);
    }

    @Test
    @DisplayName("should drop the bucket")
    void shouldDropTheBucket() {
        gridFSBucket.uploadFromStream("fileName",
                new ByteArrayInputStream("Hello GridFS".getBytes(StandardCharsets.UTF_8)));

        gridFSBucket.drop();

        List<String> collectionNames = mongoDatabase.listCollectionNames().into(new ArrayList<>());
        assertTrue(!collectionNames.contains(filesCollection.getNamespace().getCollectionName()));
        assertTrue(!collectionNames.contains(chunksCollection.getNamespace().getCollectionName()));
    }

    @Test
    @DisplayName("should use the user provided codec registries for encoding / decoding data")
    void shouldUseUserProvidedCodecRegistries() {
        MongoClient client = MongoClients.create(getMongoClientSettingsBuilder()
                .uuidRepresentation(UuidRepresentation.STANDARD)
                .build());
        try {
            CodecRegistry codecRegistry = fromRegistries(fromCodecs(new UuidCodec(UuidRepresentation.STANDARD)),
                    MongoClientSettings.getDefaultCodecRegistry());

            MongoDatabase db = client.getDatabase(getDefaultDatabaseName()).withCodecRegistry(codecRegistry);
            UUID uuid = UUID.randomUUID();
            Document fileMeta = new Document("uuid", uuid);
            GridFSBucket bucket = GridFSBuckets.create(db);

            ObjectId fileId = bucket.uploadFromStream("myFile",
                    new ByteArrayInputStream(multiChunkString.getBytes(StandardCharsets.UTF_8)),
                    new GridFSUploadOptions().metadata(fileMeta));

            GridFSFile file = bucket.find(new Document("_id", fileId)).first();

            assertEquals(fileMeta, file.getMetadata());
            assertEquals((byte) 4,
                    filesCollection.find(BsonDocument.class).first().getDocument("metadata").getBinary("uuid").getType());
        } finally {
            client.close();
        }
    }

    @ParameterizedTest(name = "should handle missing file name data when downloading {0}")
    @MethodSource("directValues")
    @DisplayName("should handle missing file name data when downloading")
    void shouldHandleMissingFileNameDataWhenDownloading(final String description, final boolean direct) throws IOException {
        String content = multiChunkString;
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        ObjectId fileId;
        byte[] gridFSContentBytes;

        if (direct) {
            fileId = gridFSBucket.uploadFromStream("myFile", new ByteArrayInputStream(contentBytes));
        } else {
            GridFSUploadStream outputStream = gridFSBucket.openUploadStream("myFile");
            outputStream.write(contentBytes);
            outputStream.close();
            fileId = outputStream.getObjectId();
        }

        assertEquals(1, filesCollection.countDocuments());

        // Remove filename
        filesCollection.updateOne(eq("_id", fileId), unset("filename"));

        if (direct) {
            gridFSContentBytes = readAllBytes(gridFSBucket.openDownloadStream(fileId));
        } else {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(contentBytes.length);
            gridFSBucket.downloadToStream(fileId, baos);
            baos.close();
            gridFSContentBytes = baos.toByteArray();
        }

        assertArrayEquals(contentBytes, gridFSContentBytes);
    }

    static Stream<Arguments> directValues() {
        return Stream.of(
                Arguments.of("directly", true),
                Arguments.of("a stream", false)
        );
    }

    private static byte[] readAllBytes(final InputStream inputStream) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] data = new byte[4096];
        int bytesRead;
        while ((bytesRead = inputStream.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, bytesRead);
        }
        buffer.flush();
        inputStream.close();
        return buffer.toByteArray();
    }
}
