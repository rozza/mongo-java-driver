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

package com.mongodb.client.internal;

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.operation.AggregateResponseBatchCursor;
import com.mongodb.internal.operation.ChangeStreamOperation;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.CustomMatchers.isTheSameAs;
import static com.mongodb.ReadPreference.secondary;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ChangeStreamIterableTest {

    private final MongoNamespace namespace = new MongoNamespace("db", "coll");
    private final CodecRegistry codecRegistry = fromProviders(Arrays.asList(new ValueCodecProvider(),
            new DocumentCodecProvider(), new BsonValueCodecProvider()));
    private final ReadConcern readConcern = ReadConcern.MAJORITY;
    private final Collation collation = Collation.builder().locale("en").build();

    @Test
    @DisplayName("should build the expected ChangeStreamOperation")
    void shouldBuildExpectedChangeStreamOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(null, null, null, null, null));
        List<Document> pipeline = Collections.singletonList(new Document("$match", 1));
        ChangeStreamIterableImpl<Document> changeStreamIterable = new ChangeStreamIterableImpl<>(null, namespace,
                codecRegistry, secondary(), readConcern, executor, pipeline, Document.class,
                ChangeStreamLevel.COLLECTION, true, TIMEOUT_SETTINGS);

        // default
        changeStreamIterable.iterator();
        RawBsonDocumentCodec codec = new RawBsonDocumentCodec();
        ChangeStreamOperation<Document> operation = (ChangeStreamOperation<Document>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new ChangeStreamOperation<>(namespace,
                FullDocument.DEFAULT, FullDocumentBeforeChange.DEFAULT,
                Collections.singletonList(BsonDocument.parse("{$match: 1}")), codec,
                ChangeStreamLevel.COLLECTION)
                .retryReads(true)));
        assertEquals(secondary(), executor.getReadPreference());

        // overriding
        RawBsonDocument resumeToken = RawBsonDocument.parse("{_id: {a: 1}}");
        BsonTimestamp startAtOperationTime = new BsonTimestamp(99);
        changeStreamIterable.collation(collation)
                .maxAwaitTime(101, MILLISECONDS)
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .fullDocumentBeforeChange(FullDocumentBeforeChange.WHEN_AVAILABLE)
                .resumeAfter(resumeToken).startAtOperationTime(startAtOperationTime)
                .startAfter(resumeToken).iterator();

        operation = (ChangeStreamOperation<Document>) executor.getReadOperation();
        assertThat(operation, isTheSameAs(new ChangeStreamOperation<>(namespace,
                FullDocument.UPDATE_LOOKUP, FullDocumentBeforeChange.WHEN_AVAILABLE,
                Collections.singletonList(BsonDocument.parse("{$match: 1}")), codec,
                ChangeStreamLevel.COLLECTION)
                .retryReads(true)
                .collation(collation)
                .resumeAfter(resumeToken)
                .startAtOperationTime(startAtOperationTime)
                .startAfter(resumeToken)));
    }

    @Test
    @DisplayName("should use ClientSession")
    void shouldUseClientSession() {
        for (ClientSession session : new ClientSession[]{null, mock(ClientSession.class)}) {
            @SuppressWarnings("unchecked")
            AggregateResponseBatchCursor<RawBsonDocument> batchCursor = mock(AggregateResponseBatchCursor.class);
            when(batchCursor.hasNext()).thenReturn(false);
            TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(batchCursor, batchCursor));
            ChangeStreamIterableImpl<Document> changeStreamIterable = new ChangeStreamIterableImpl<>(session,
                    namespace, codecRegistry, secondary(), readConcern, executor, Collections.emptyList(),
                    Document.class, ChangeStreamLevel.COLLECTION, true, TIMEOUT_SETTINGS);

            changeStreamIterable.first();
            assertEquals(session, executor.getClientSession());

            changeStreamIterable.iterator();
            assertEquals(session, executor.getClientSession());
        }
    }

    @Test
    @DisplayName("should handle exceptions correctly")
    void shouldHandleExceptions() {
        CodecRegistry altRegistry = fromProviders(Arrays.asList(new ValueCodecProvider(), new BsonValueCodecProvider()));
        TestOperationExecutor executor = new TestOperationExecutor(Collections.singletonList(new MongoException("failure")));
        List<BsonDocument> pipeline = Collections.singletonList(new BsonDocument("$match", new BsonInt32(1)));
        ChangeStreamIterableImpl<BsonDocument> changeStreamIterable = new ChangeStreamIterableImpl<>(null, namespace,
                codecRegistry, secondary(), readConcern, executor, pipeline, BsonDocument.class,
                ChangeStreamLevel.COLLECTION, true, TIMEOUT_SETTINGS);

        assertThrows(MongoException.class, () -> changeStreamIterable.iterator());

        assertThrows(CodecConfigurationException.class, () ->
                new ChangeStreamIterableImpl<>(null, namespace, altRegistry, secondary(), readConcern, executor,
                        pipeline, Document.class, ChangeStreamLevel.COLLECTION, true, TIMEOUT_SETTINGS).iterator());

        assertThrows(IllegalArgumentException.class, () ->
                new ChangeStreamIterableImpl<>(null, namespace, codecRegistry, secondary(), readConcern, executor,
                        Collections.singletonList(null), Document.class, ChangeStreamLevel.COLLECTION, true,
                        TIMEOUT_SETTINGS).iterator());
    }

    @Test
    @DisplayName("should follow the MongoIterable interface as expected")
    void shouldFollowMongoIterableInterface() {
        List<RawBsonDocument> cannedResults = Arrays.asList(
                RawBsonDocument.parse("{_id: {_data: 1}}"),
                RawBsonDocument.parse("{_id: {_data: 2}}"),
                RawBsonDocument.parse("{_id: {_data: 3}}"));
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(
                createCursor(cannedResults), createCursor(cannedResults),
                createCursor(cannedResults), createCursor(cannedResults)));
        ChangeStreamIterableImpl<Document> mongoIterable = new ChangeStreamIterableImpl<>(null, namespace,
                codecRegistry, secondary(), readConcern, executor, Collections.emptyList(), Document.class,
                ChangeStreamLevel.COLLECTION, true, TIMEOUT_SETTINGS);

        ChangeStreamDocument<Document> results = mongoIterable.first();
        assertEquals(cannedResults.get(0).getDocument("_id"), results.getResumeToken());

        final int[] count = {0};
        mongoIterable.forEach(result -> count[0]++);
        assertEquals(3, count[0]);

        List<ChangeStreamDocument<Document>> target = new ArrayList<>();
        mongoIterable.into(target);
        assertEquals(cannedResults.get(0).getDocument("_id"), target.get(0).getResumeToken());
        assertEquals(cannedResults.get(1).getDocument("_id"), target.get(1).getResumeToken());
        assertEquals(cannedResults.get(2).getDocument("_id"), target.get(2).getResumeToken());

        List<Integer> mapped = new ArrayList<>();
        mongoIterable.map(document -> document.getResumeToken().getInt32("_data").intValue()).into(mapped);
        assertEquals(Arrays.asList(1, 2, 3), mapped);
    }

    @Test
    @DisplayName("should be able to return the raw results")
    void shouldReturnRawResults() {
        List<RawBsonDocument> cannedResults = Arrays.asList(
                RawBsonDocument.parse("{_id: { _data: 1}}"),
                RawBsonDocument.parse("{_id: {_data: 2}}"),
                RawBsonDocument.parse("{_id: {_data: 3}}"));
        TestOperationExecutor executor = new TestOperationExecutor(Arrays.asList(
                createCursor(cannedResults), createCursor(cannedResults),
                createCursor(cannedResults), createCursor(cannedResults),
                createCursor(cannedResults)));
        ChangeStreamIterableImpl<Document> baseIterable = new ChangeStreamIterableImpl<>(null, namespace,
                codecRegistry, secondary(), readConcern, executor, Collections.emptyList(), Document.class,
                ChangeStreamLevel.COLLECTION, true, TIMEOUT_SETTINGS);
        MongoIterable<RawBsonDocument> mongoIterable = baseIterable.withDocumentClass(RawBsonDocument.class);

        RawBsonDocument results = mongoIterable.first();
        assertEquals(cannedResults.get(0), results);

        final int[] count = {0};
        mongoIterable.forEach(rawBsonDocument -> count[0]++);
        assertEquals(3, count[0]);

        List<RawBsonDocument> target = new ArrayList<>();
        mongoIterable.into(target);
        assertEquals(cannedResults, target);

        List<Integer> mapped = new ArrayList<>();
        mongoIterable.map(document -> document.getDocument("_id").getInt32("_data").intValue()).into(mapped);
        assertEquals(Arrays.asList(1, 2, 3), mapped);
    }

    @Test
    @DisplayName("should get and set batchSize as expected")
    void shouldGetAndSetBatchSize() {
        ChangeStreamIterableImpl<BsonDocument> mongoIterable = new ChangeStreamIterableImpl<>(null, namespace,
                codecRegistry, secondary(), readConcern, mock(OperationExecutor.class),
                Collections.singletonList(BsonDocument.parse("{$match: 1}")), BsonDocument.class,
                ChangeStreamLevel.COLLECTION, true, TIMEOUT_SETTINGS);

        assertNull(mongoIterable.getBatchSize());
        mongoIterable.batchSize(5);
        assertEquals(Integer.valueOf(5), mongoIterable.getBatchSize());
    }

    @SuppressWarnings("unchecked")
    private AggregateResponseBatchCursor<RawBsonDocument> createCursor(final List<RawBsonDocument> cannedResults) {
        AggregateResponseBatchCursor<RawBsonDocument> cursor = mock(AggregateResponseBatchCursor.class);
        when(cursor.hasNext()).thenReturn(true, true, false);
        when(cursor.next()).thenReturn(new ArrayList<>(cannedResults));
        return cursor;
    }
}
