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

import com.mongodb.CursorType;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.client.gridfs.codecs.GridFSFileCodec;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.internal.FindIterableImpl;
import com.mongodb.client.internal.TestOperationExecutor;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.FindOperation;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.CustomMatchers.isTheSameAs;
import static com.mongodb.ReadPreference.secondary;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GridFSFindIterableTest {

    private final CodecRegistry codecRegistry = MongoClientSettings.getDefaultCodecRegistry();
    private final GridFSFileCodec gridFSFileCodec = new GridFSFileCodec(codecRegistry);
    private final ReadConcern readConcern = ReadConcern.DEFAULT;
    private final Collation collation = Collation.builder().locale("en").build();
    private final MongoNamespace namespace = new MongoNamespace("test", "fs.files");

    @Test
    @DisplayName("should build the expected findOperation")
    void shouldBuildExpectedFindOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null));
        FindIterableImpl<GridFSFile, GridFSFile> underlying = new FindIterableImpl<>(null, namespace,
                GridFSFile.class, GridFSFile.class, codecRegistry, secondary(), readConcern, executor,
                new Document(), true, TIMEOUT_SETTINGS);
        GridFSFindIterableImpl findIterable = new GridFSFindIterableImpl(underlying);

        // default
        findIterable.iterator();
        FindOperation<?> operation = (FindOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new FindOperation<>(namespace, gridFSFileCodec)
                .filter(new BsonDocument()).retryReads(true)));
        assertEquals(secondary(), executor.getReadPreference());

        // overriding
        findIterable.filter(new Document("filter", 2))
                .sort(new Document("sort", 2))
                .maxTime(100, MILLISECONDS)
                .batchSize(99)
                .limit(99)
                .skip(9)
                .noCursorTimeout(true)
                .collation(collation)
                .iterator();
        operation = (FindOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new FindOperation<>(namespace, gridFSFileCodec)
                .filter(new BsonDocument("filter", new BsonInt32(2)))
                .sort(new BsonDocument("sort", new BsonInt32(2)))
                .batchSize(99)
                .limit(99)
                .skip(9)
                .noCursorTimeout(true)
                .collation(collation)
                .retryReads(true)));
    }

    @Test
    @DisplayName("should handle mixed types")
    void shouldHandleMixedTypes() {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null));
        FindIterableImpl<GridFSFile, GridFSFile> findIterable = new FindIterableImpl<>(null, namespace,
                GridFSFile.class, GridFSFile.class, codecRegistry, secondary(), readConcern, executor,
                new Document("filter", 1), true, TIMEOUT_SETTINGS);

        findIterable.filter(new Document("filter", 1))
                .sort(new BsonDocument("sort", new BsonInt32(1)))
                .iterator();
        FindOperation<?> operation = (FindOperation<?>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new FindOperation<>(namespace, gridFSFileCodec)
                .filter(new BsonDocument("filter", new BsonInt32(1)))
                .sort(new BsonDocument("sort", new BsonInt32(1)))
                .cursorType(CursorType.NonTailable)
                .retryReads(true)));
    }

    @Test
    @DisplayName("should follow the MongoIterable interface as expected")
    void shouldFollowMongoIterableInterface() {
        List<GridFSFile> cannedResults = Arrays.asList(
                new GridFSFile(new BsonObjectId(new ObjectId()), "File 1", 123L, 255, new Date(1438679434041L), null),
                new GridFSFile(new BsonObjectId(new ObjectId()), "File 2", 999999L, 255, new Date(1438679434050L), null),
                new GridFSFile(new BsonObjectId(new ObjectId()), "File 3", 1L, 255, new Date(1438679434090L), null));

        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(
                createBatchCursor(cannedResults), createBatchCursor(cannedResults),
                createBatchCursor(cannedResults), createBatchCursor(cannedResults)));
        FindIterableImpl<GridFSFile, GridFSFile> underlying = new FindIterableImpl<>(null, namespace,
                GridFSFile.class, GridFSFile.class, codecRegistry, secondary(), readConcern, executor,
                new Document(), true, TIMEOUT_SETTINGS);
        GridFSFindIterableImpl mongoIterable = new GridFSFindIterableImpl(underlying);

        assertEquals(cannedResults.get(0), mongoIterable.first());

        final int[] count = {0};
        mongoIterable.forEach(document -> count[0]++);
        assertEquals(3, count[0]);

        List<GridFSFile> target = new ArrayList<>();
        mongoIterable.into(target);
        assertEquals(cannedResults, target);

        List<String> mapped = new ArrayList<>();
        mongoIterable.map(GridFSFile::getFilename).into(mapped);
        assertEquals(Arrays.asList("File 1", "File 2", "File 3"), mapped);
    }

    @SuppressWarnings("unchecked")
    private BatchCursor<GridFSFile> createBatchCursor(final List<GridFSFile> results) {
        BatchCursor<GridFSFile> batchCursor = mock(BatchCursor.class);
        when(batchCursor.hasNext()).thenReturn(true, true, false);
        when(batchCursor.next()).thenReturn(new ArrayList<>(results));
        return batchCursor;
    }
}
