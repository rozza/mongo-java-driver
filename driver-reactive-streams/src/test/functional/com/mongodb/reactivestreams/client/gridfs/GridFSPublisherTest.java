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

package com.mongodb.reactivestreams.client.gridfs;

import com.mongodb.MongoGridFSException;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.reactivestreams.client.FunctionalSpecification;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.UuidCodec;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.unset;
import static com.mongodb.reactivestreams.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.reactivestreams.client.Fixture.getMongoClient;
import static com.mongodb.reactivestreams.client.Fixture.getMongoClientBuilderFromConnectionString;
import static com.mongodb.reactivestreams.client.MongoClients.getDefaultCodecRegistry;
import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GridFSPublisherTest extends FunctionalSpecification {

    private static final String SINGLE_CHUNK_STRING = "GridFS";
    private static final String MULTI_CHUNK_STRING = String.format("%" + (1024 * 255 * 5) + "s", SINGLE_CHUNK_STRING);

    private MongoDatabase mongoDatabase;
    private MongoCollection<GridFSFile> filesCollection;
    private MongoCollection<Document> chunksCollection;
    private GridFSBucket gridFSBucket;

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        mongoDatabase = getMongoClient().getDatabase(getDefaultDatabaseName());
        filesCollection = mongoDatabase.getCollection("fs.files", GridFSFile.class);
        chunksCollection = mongoDatabase.getCollection("fs.chunks");
        Mono.from(filesCollection.drop()).block(TIMEOUT_DURATION);
        Mono.from(chunksCollection.drop()).block(TIMEOUT_DURATION);
        gridFSBucket = GridFSBuckets.create(mongoDatabase);
    }

    @Override
    @AfterEach
    public void tearDown() {
        if (filesCollection != null) {
            Mono.from(filesCollection.drop()).block(TIMEOUT_DURATION);
            Mono.from(chunksCollection.drop()).block(TIMEOUT_DURATION);
        }
        super.tearDown();
    }

    static Stream<Arguments> shouldRoundTripFile() {
        return Stream.of(
                Arguments.of("a small file", false, 1),
                Arguments.of("a large file", true, 5)
        );
    }

    @ParameterizedTest(name = "should round trip {0}")
    @MethodSource("shouldRoundTripFile")
    void shouldRoundTripAFile(String description, boolean multiChunk, int chunkCount) {
        String content = multiChunk ? MULTI_CHUNK_STRING : SINGLE_CHUNK_STRING;
        byte[] contentBytes = content.getBytes();
        long expectedLength = contentBytes.length;

        ObjectId fileId = Mono.from(gridFSBucket.uploadFromPublisher("myFile",
                createPublisher(ByteBuffer.wrap(contentBytes)))).block(TIMEOUT_DURATION);

        assertEquals(1, Mono.from(filesCollection.countDocuments()).block(TIMEOUT_DURATION));
        assertEquals(chunkCount, Mono.from(chunksCollection.countDocuments()).block(TIMEOUT_DURATION));

        GridFSFile fileInfo = Mono.from(gridFSBucket.find().filter(eq("_id", fileId)).first()).block(TIMEOUT_DURATION);

        assertEquals(fileId, fileInfo.getId().getValue());
        assertEquals(gridFSBucket.getChunkSizeBytes(), fileInfo.getChunkSize());
        assertEquals(expectedLength, fileInfo.getLength());
        assertNull(fileInfo.getMetadata());

        List<ByteBuffer> data = Flux.from(gridFSBucket.downloadToPublisher(fileId)).collectList().block(TIMEOUT_DURATION);

        assertArrayEquals(contentBytes, concatByteBuffers(data));
    }

    @Test
    void shouldRoundTripWithSmallChunks() {
        int contentSize = 1024 * 10;
        int chunkSize = 10;
        byte[] contentBytes = new byte[contentSize];
        new SecureRandom().nextBytes(contentBytes);
        GridFSUploadOptions options = new GridFSUploadOptions().chunkSizeBytes(chunkSize);

        ObjectId fileId = Mono.from(gridFSBucket.uploadFromPublisher("myFile",
                createPublisher(ByteBuffer.wrap(contentBytes)), options)).block(TIMEOUT_DURATION);

        assertEquals(1, Mono.from(filesCollection.countDocuments()).block(TIMEOUT_DURATION));
        assertEquals(contentSize / chunkSize, Mono.from(chunksCollection.countDocuments()).block(TIMEOUT_DURATION));

        List<ByteBuffer> data = Flux.from(gridFSBucket.downloadToPublisher(fileId)).collectList().block(TIMEOUT_DURATION);

        assertArrayEquals(contentBytes, concatByteBuffers(data));
    }

    @Test
    void shouldRespectTheOuterSubscriptionRequestAmount() {
        byte[] contentBytes = MULTI_CHUNK_STRING.getBytes();
        GridFSUploadOptions options = new GridFSUploadOptions().chunkSizeBytes(contentBytes.length);

        ObjectId fileId = Mono.from(gridFSBucket.uploadFromPublisher("myFile",
                createPublisher(ByteBuffer.wrap(contentBytes), ByteBuffer.wrap(contentBytes),
                        ByteBuffer.wrap(contentBytes)), options)).block(TIMEOUT_DURATION);

        assertEquals(1, Mono.from(filesCollection.countDocuments()).block(TIMEOUT_DURATION));
        assertEquals(3, Mono.from(chunksCollection.countDocuments()).block(TIMEOUT_DURATION));

        ByteBuffer data = Mono.from(gridFSBucket.downloadToPublisher(fileId)
                .bufferSizeBytes(contentBytes.length * 3)).block(TIMEOUT_DURATION);

        byte[] expected = concatByteBuffers(asList(ByteBuffer.wrap(contentBytes), ByteBuffer.wrap(contentBytes),
                ByteBuffer.wrap(contentBytes)));
        assertArrayEquals(expected, data.array());
    }

    @Test
    void shouldUploadFromSourcePublisherWithMultiplePartsSmallTotalSize() {
        byte[] contentBytes = SINGLE_CHUNK_STRING.getBytes();

        ObjectId fileId = Mono.from(gridFSBucket.uploadFromPublisher("myFile",
                createPublisher(ByteBuffer.wrap(contentBytes), ByteBuffer.wrap(contentBytes)))).block(TIMEOUT_DURATION);

        assertEquals(1, Mono.from(filesCollection.countDocuments()).block(TIMEOUT_DURATION));
        assertEquals(1, Mono.from(chunksCollection.countDocuments()).block(TIMEOUT_DURATION));

        ByteBuffer data = Mono.from(gridFSBucket.downloadToPublisher(fileId)).block(TIMEOUT_DURATION);

        byte[] expected = concatByteBuffers(asList(ByteBuffer.wrap(contentBytes), ByteBuffer.wrap(contentBytes)));
        assertArrayEquals(expected, data.array());
    }

    @Test
    void shouldRoundTripWithDataLargerThanInternalBufferSize() {
        int contentSize = 1024 * 1024 * 5;
        int chunkSize = 1024 * 1024;
        byte[] contentBytes = new byte[contentSize];
        new SecureRandom().nextBytes(contentBytes);
        GridFSUploadOptions options = new GridFSUploadOptions().chunkSizeBytes(chunkSize);

        ObjectId fileId = Mono.from(gridFSBucket.uploadFromPublisher("myFile",
                createPublisher(ByteBuffer.wrap(contentBytes)), options)).block(TIMEOUT_DURATION);

        assertEquals(1, Mono.from(filesCollection.countDocuments()).block(TIMEOUT_DURATION));
        assertEquals(contentSize / chunkSize, Mono.from(chunksCollection.countDocuments()).block(TIMEOUT_DURATION));

        List<ByteBuffer> data = Flux.from(gridFSBucket.downloadToPublisher(fileId)).collectList().block(TIMEOUT_DURATION);

        assertArrayEquals(contentBytes, concatByteBuffers(data));
    }

    @Test
    void shouldHandleCustomIds() {
        byte[] contentBytes = MULTI_CHUNK_STRING.getBytes();
        BsonString fileId = new BsonString("myFile");

        Mono.from(gridFSBucket.uploadFromPublisher(fileId, "myFile",
                createPublisher(ByteBuffer.wrap(contentBytes)))).block(TIMEOUT_DURATION);
        List<ByteBuffer> data = Flux.from(gridFSBucket.downloadToPublisher(fileId)).collectList().block(TIMEOUT_DURATION);

        assertArrayEquals(contentBytes, concatByteBuffers(data));

        Mono.from(gridFSBucket.rename(fileId, "newName")).block(TIMEOUT_DURATION);
        data = Flux.from(gridFSBucket.downloadToPublisher("newName")).collectList().block(TIMEOUT_DURATION);

        assertArrayEquals(contentBytes, concatByteBuffers(data));

        Mono.from(gridFSBucket.delete(fileId)).block(TIMEOUT_DURATION);

        assertEquals(0, Mono.from(filesCollection.countDocuments()).block(TIMEOUT_DURATION));
        assertEquals(0, Mono.from(chunksCollection.countDocuments()).block(TIMEOUT_DURATION));
    }

    @Test
    void shouldThrowChunkNotFoundErrorWhenNoChunks() {
        int contentSize = 1024 * 1024;
        byte[] contentBytes = new byte[contentSize];
        new SecureRandom().nextBytes(contentBytes);

        ObjectId fileId = Mono.from(gridFSBucket.uploadFromPublisher("myFile",
                createPublisher(ByteBuffer.wrap(contentBytes)))).block(TIMEOUT_DURATION);
        Mono.from(chunksCollection.deleteMany(eq("files_id", fileId))).block(TIMEOUT_DURATION);

        assertThrows(MongoGridFSException.class, () ->
                Mono.from(gridFSBucket.downloadToPublisher(fileId)).block(TIMEOUT_DURATION));
    }

    @Test
    void shouldRoundTripWithByteBufferSizeOf4096() {
        int contentSize = 1024 * 1024;
        byte[] contentBytes = new byte[contentSize];
        new SecureRandom().nextBytes(contentBytes);
        GridFSUploadOptions options = new GridFSUploadOptions().chunkSizeBytes(1024);

        ObjectId fileId = Mono.from(gridFSBucket.uploadFromPublisher("myFile",
                createPublisher(ByteBuffer.wrap(contentBytes)), options)).block(TIMEOUT_DURATION);

        assertEquals(1, Mono.from(filesCollection.countDocuments()).block(TIMEOUT_DURATION));
        assertEquals(1024, Mono.from(chunksCollection.countDocuments()).block(TIMEOUT_DURATION));

        GridFSFile fileInfo = Mono.from(gridFSBucket.find().filter(eq("_id", fileId)).first()).block(TIMEOUT_DURATION);

        assertEquals(fileId, fileInfo.getObjectId());
        assertEquals(1024, fileInfo.getChunkSize());
        assertEquals(contentSize, fileInfo.getLength());
        assertNull(fileInfo.getMetadata());

        List<ByteBuffer> data = Flux.from(gridFSBucket.downloadToPublisher(fileId).bufferSizeBytes(4096))
                .collectList().block(TIMEOUT_DURATION);

        assertEquals(256, data.size());
        assertArrayEquals(contentBytes, concatByteBuffers(data));
    }

    @Test
    void shouldHandleUploadingPublisherErroring() {
        String errorMessage = "Failure Propagated";
        Publisher<ByteBuffer> source = new Publisher<ByteBuffer>() {
            @Override
            public void subscribe(final Subscriber<? super ByteBuffer> s) {
                s.onError(new IllegalArgumentException(errorMessage));
            }
        };

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                Mono.from(gridFSBucket.uploadFromPublisher("myFile", source)).block(TIMEOUT_DURATION));
        assertEquals(errorMessage, ex.getMessage());
    }

    @Test
    void shouldUseCustomUploadOptionsWhenUploading() {
        int chunkSize = 20;
        Document metadata = new Document("archived", false);
        GridFSUploadOptions options = new GridFSUploadOptions()
                .chunkSizeBytes(chunkSize)
                .metadata(metadata);
        String content = "qwerty".repeat(1024);
        byte[] contentBytes = content.getBytes();
        long expectedLength = contentBytes.length;
        int expectedNoChunks = (int) Math.ceil((double) expectedLength / chunkSize);

        ObjectId fileId = Mono.from(gridFSBucket.uploadFromPublisher("myFile",
                createPublisher(ByteBuffer.wrap(contentBytes)), options)).block(TIMEOUT_DURATION);

        assertEquals(1, Mono.from(filesCollection.countDocuments()).block(TIMEOUT_DURATION));
        assertEquals(expectedNoChunks, Mono.from(chunksCollection.countDocuments()).block(TIMEOUT_DURATION));

        GridFSFile fileInfo = Mono.from(gridFSBucket.find().filter(eq("_id", fileId)).first()).block(TIMEOUT_DURATION);

        assertEquals(fileId, fileInfo.getId().getValue());
        assertEquals(options.getChunkSizeBytes(), fileInfo.getChunkSize());
        assertEquals(expectedLength, fileInfo.getLength());
        assertEquals(options.getMetadata(), fileInfo.getMetadata());

        List<ByteBuffer> data = Flux.from(gridFSBucket.downloadToPublisher(fileId)).collectList().block(TIMEOUT_DURATION);

        assertArrayEquals(contentBytes, concatByteBuffers(data));
    }

    @Test
    void shouldBeAbleToOpenByName() {
        String content = "Hello GridFS";
        byte[] contentBytes = content.getBytes();
        String filename = "myFile";
        Mono.from(gridFSBucket.uploadFromPublisher(filename,
                createPublisher(ByteBuffer.wrap(contentBytes)))).block(TIMEOUT_DURATION);

        List<ByteBuffer> data = Flux.from(gridFSBucket.downloadToPublisher(filename)).collectList().block(TIMEOUT_DURATION);

        assertArrayEquals(contentBytes, concatByteBuffers(data));
    }

    @Test
    void shouldBeAbleToHandleMissingFile() {
        String filename = "myFile";
        assertThrows(MongoGridFSException.class, () ->
                Mono.from(gridFSBucket.downloadToPublisher(filename)).block(TIMEOUT_DURATION));
    }

    @Test
    void shouldCreateTheIndexesAsExpected() {
        Document filesIndexKey = Document.parse("{ filename: 1, uploadDate: 1 }");
        Document chunksIndexKey = Document.parse("{ files_id: 1, n: 1 }");

        List<Object> filesIndexKeys = Flux.from(filesCollection.listIndexes()).collectList().block(TIMEOUT_DURATION)
                .stream().map(doc -> doc.get("key")).collect(Collectors.toList());
        List<Object> chunksIndexKeys = Flux.from(chunksCollection.listIndexes()).collectList().block(TIMEOUT_DURATION)
                .stream().map(doc -> doc.get("key")).collect(Collectors.toList());
        assertFalse(filesIndexKeys.contains(filesIndexKey));
        assertFalse(chunksIndexKeys.contains(chunksIndexKey));

        Mono.from(gridFSBucket.uploadFromPublisher("myFile",
                createPublisher(ByteBuffer.wrap(MULTI_CHUNK_STRING.getBytes())))).block(TIMEOUT_DURATION);

        filesIndexKeys = Flux.from(filesCollection.listIndexes()).collectList().block(TIMEOUT_DURATION)
                .stream().map(doc -> doc.get("key")).collect(Collectors.toList());
        chunksIndexKeys = Flux.from(chunksCollection.listIndexes()).collectList().block(TIMEOUT_DURATION)
                .stream().map(doc -> doc.get("key")).collect(Collectors.toList());
        assertTrue(filesIndexKeys.contains(Document.parse("{ filename: 1, uploadDate: 1 }")));
        assertTrue(chunksIndexKeys.contains(Document.parse("{ files_id: 1, n: 1 }")));
    }

    @Test
    void shouldNotCreateIndexesIfFilesCollectionIsNotEmpty() {
        Mono.from(filesCollection.withDocumentClass(Document.class).insertOne(
                new Document("filename", "bad file"))).block(TIMEOUT_DURATION);
        byte[] contentBytes = "Hello GridFS".getBytes();

        assertEquals(1, Flux.from(filesCollection.listIndexes()).collectList().block(TIMEOUT_DURATION).size());
        assertEquals(0, Flux.from(chunksCollection.listIndexes()).collectList().block(TIMEOUT_DURATION).size());

        Mono.from(gridFSBucket.uploadFromPublisher("myFile",
                createPublisher(ByteBuffer.wrap(contentBytes)))).block(TIMEOUT_DURATION);

        assertEquals(1, Flux.from(filesCollection.listIndexes()).collectList().block(TIMEOUT_DURATION).size());
        assertEquals(1, Flux.from(chunksCollection.listIndexes()).collectList().block(TIMEOUT_DURATION).size());
    }

    @Test
    void shouldUseUserProvidedCodecRegistriesForEncodingDecoding() {
        com.mongodb.reactivestreams.client.MongoClient client = MongoClients.create(getMongoClientBuilderFromConnectionString()
                .uuidRepresentation(UuidRepresentation.STANDARD)
                .codecRegistry(fromRegistries(fromCodecs(new UuidCodec(UuidRepresentation.STANDARD)), getDefaultCodecRegistry()))
                .build());
        try {
            MongoDatabase database = client.getDatabase(getDefaultDatabaseName());

            UUID uuid = UUID.randomUUID();
            Document fileMeta = new Document("uuid", uuid);
            GridFSBucket localGridFSBucket = GridFSBuckets.create(database);

            ObjectId fileId = Mono.from(localGridFSBucket.uploadFromPublisher("myFile",
                    createPublisher(ByteBuffer.wrap(MULTI_CHUNK_STRING.getBytes())),
                    new GridFSUploadOptions().metadata(fileMeta))).block(TIMEOUT_DURATION);

            GridFSFile file = Mono.from(localGridFSBucket.find(new Document("_id", fileId)).first()).block(TIMEOUT_DURATION);

            assertEquals(fileMeta, file.getMetadata());

            BsonDocument fileAsDocument = Mono.from(filesCollection.find(BsonDocument.class).first()).block(TIMEOUT_DURATION);

            assertEquals((byte) 4, fileAsDocument.getDocument("metadata").getBinary("uuid").getType());
        } finally {
            client.close();
        }
    }

    @Test
    void shouldHandleMissingFileNameDataWhenDownloading() {
        byte[] contentBytes = MULTI_CHUNK_STRING.getBytes();

        ObjectId fileId = Mono.from(gridFSBucket.uploadFromPublisher("myFile",
                createPublisher(ByteBuffer.wrap(contentBytes)))).block(TIMEOUT_DURATION);

        assertEquals(1, Mono.from(filesCollection.countDocuments()).block(TIMEOUT_DURATION));

        // Remove filename
        Mono.from(filesCollection.updateOne(eq("_id", fileId), unset("filename"))).block(TIMEOUT_DURATION);
        List<ByteBuffer> data = Flux.from(gridFSBucket.downloadToPublisher(fileId)).collectList().block(TIMEOUT_DURATION);

        assertArrayEquals(contentBytes, concatByteBuffers(data));
    }

    @Test
    void shouldCleanupWhenUnsubscribing() {
        int contentSize = 1024;
        byte[] contentBytes = new byte[contentSize];
        new SecureRandom().nextBytes(contentBytes);
        GridFSUploadOptions options = new GridFSUploadOptions().chunkSizeBytes(1024);
        List<ByteBuffer> bufferList = IntStream.rangeClosed(0, 1024)
                .mapToObj(i -> ByteBuffer.wrap(contentBytes))
                .collect(Collectors.toList());
        Flux<ByteBuffer> publisher = Flux.fromIterable(bufferList).delayElements(Duration.ofMillis(1000));

        AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
        Subscriber<ObjectId> subscriber = new Subscriber<ObjectId>() {
            @Override
            public void onSubscribe(final Subscription s) {
                subscriptionRef.set(s);
            }

            @Override
            public void onNext(final ObjectId o) {
            }

            @Override
            public void onError(final Throwable t) {
            }

            @Override
            public void onComplete() {
            }
        };

        gridFSBucket.uploadFromPublisher("myFile", publisher, options)
                .subscribe(subscriber);
        subscriptionRef.get().request(1);

        retry(10, () -> Mono.from(chunksCollection.countDocuments()).block(TIMEOUT_DURATION) > 0);
        assertEquals(0, Mono.from(filesCollection.countDocuments()).block(TIMEOUT_DURATION));

        subscriptionRef.get().cancel();

        retry(50, () -> Mono.from(chunksCollection.countDocuments()).block(TIMEOUT_DURATION) == 0);
        assertEquals(0, Mono.from(filesCollection.countDocuments()).block(TIMEOUT_DURATION));
    }

    private static void retry(int times, BooleanSupplier condition) {
        boolean result = condition.getAsBoolean();
        if (!result && times > 0) {
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            retry(times - 1, condition);
        } else {
            assertTrue(result);
        }
    }

    private static byte[] concatByteBuffers(List<ByteBuffer> buffers) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            WritableByteChannel channel = Channels.newChannel(outputStream);
            for (ByteBuffer buffer : buffers) {
                channel.write(buffer);
            }
            outputStream.close();
            channel.close();
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Flux<ByteBuffer> createPublisher(final ByteBuffer... byteBuffers) {
        return Flux.fromIterable(asList(byteBuffers));
    }
}
