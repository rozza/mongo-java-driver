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

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.internal.operation.AggregateResponseBatchCursor;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.RawBsonDocument;
import org.bson.codecs.Decoder;
import org.bson.codecs.RawBsonDocumentCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MongoChangeStreamCursorTest {

    @Test
    @DisplayName("should get server cursor and address")
    void shouldGetServerCursorAndAddress() {
        @SuppressWarnings("unchecked")
        AggregateResponseBatchCursor<RawBsonDocument> batchCursor = mock(AggregateResponseBatchCursor.class);
        @SuppressWarnings("unchecked")
        Decoder<RawBsonDocument> decoder = mock(Decoder.class);
        BsonDocument resumeToken = mock(BsonDocument.class);
        ServerAddress address = new ServerAddress("host", 27018);
        ServerCursor serverCursor = new ServerCursor(5, address);
        when(batchCursor.getServerAddress()).thenReturn(address);
        when(batchCursor.getServerCursor()).thenReturn(serverCursor);

        MongoChangeStreamCursorImpl<RawBsonDocument> cursor = new MongoChangeStreamCursorImpl<>(batchCursor, decoder, resumeToken);

        assertSame(address, cursor.getServerAddress());
        assertSame(serverCursor, cursor.getServerCursor());
    }

    @Test
    @DisplayName("should throw on remove")
    void shouldThrowOnRemove() {
        @SuppressWarnings("unchecked")
        AggregateResponseBatchCursor<RawBsonDocument> batchCursor = mock(AggregateResponseBatchCursor.class);
        @SuppressWarnings("unchecked")
        Decoder<RawBsonDocument> decoder = mock(Decoder.class);
        BsonDocument resumeToken = mock(BsonDocument.class);

        MongoChangeStreamCursorImpl<RawBsonDocument> cursor = new MongoChangeStreamCursorImpl<>(batchCursor, decoder, resumeToken);

        assertThrows(UnsupportedOperationException.class, cursor::remove);
    }

    @Test
    @DisplayName("should close batch cursor")
    void shouldCloseBatchCursor() {
        @SuppressWarnings("unchecked")
        AggregateResponseBatchCursor<RawBsonDocument> batchCursor = mock(AggregateResponseBatchCursor.class);
        @SuppressWarnings("unchecked")
        Decoder<RawBsonDocument> decoder = mock(Decoder.class);
        BsonDocument resumeToken = mock(BsonDocument.class);

        MongoChangeStreamCursorImpl<RawBsonDocument> cursor = new MongoChangeStreamCursorImpl<>(batchCursor, decoder, resumeToken);

        cursor.close();

        verify(batchCursor).close();
    }

    @Test
    @DisplayName("next should throw if there is no next")
    void nextShouldThrowIfThereIsNoNext() {
        @SuppressWarnings("unchecked")
        AggregateResponseBatchCursor<RawBsonDocument> batchCursor = mock(AggregateResponseBatchCursor.class);
        RawBsonDocumentCodec codec = new RawBsonDocumentCodec();
        BsonDocument resumeToken = mock(BsonDocument.class);
        when(batchCursor.hasNext()).thenReturn(false);

        MongoChangeStreamCursorImpl<RawBsonDocument> cursor = new MongoChangeStreamCursorImpl<>(batchCursor, codec, resumeToken);

        assertThrows(NoSuchElementException.class, cursor::next);
    }

    @Test
    @DisplayName("should get next from batch cursor")
    void shouldGetNextFromBatchCursor() {
        List<RawBsonDocument> firstBatch = Arrays.asList(
                RawBsonDocument.parse("{ _id: { _data: 1 }, x: 1 }"),
                RawBsonDocument.parse("{ _id: { _data: 2 }, x: 1 }"));
        List<RawBsonDocument> secondBatch = Arrays.asList(
                RawBsonDocument.parse("{ _id: { _data: 3 }, x: 2 }"));

        @SuppressWarnings("unchecked")
        AggregateResponseBatchCursor<RawBsonDocument> batchCursor = mock(AggregateResponseBatchCursor.class);
        RawBsonDocumentCodec codec = new RawBsonDocumentCodec();
        BsonDocument resumeToken = mock(BsonDocument.class);

        when(batchCursor.hasNext()).thenReturn(true, true, true, true, false);
        when(batchCursor.next()).thenReturn(firstBatch, secondBatch);

        MongoChangeStreamCursorImpl<RawBsonDocument> cursor = new MongoChangeStreamCursorImpl<>(batchCursor, codec, resumeToken);

        assertTrue(cursor.hasNext());
        assertEquals(firstBatch.get(0), cursor.next());
        assertTrue(cursor.hasNext());
        assertEquals(firstBatch.get(1), cursor.next());
        assertTrue(cursor.hasNext());
        assertEquals(secondBatch.get(0), cursor.next());
        assertFalse(cursor.hasNext());
    }

    @Test
    @DisplayName("should try next from batch cursor")
    void shouldTryNextFromBatchCursor() {
        List<RawBsonDocument> firstBatch = Arrays.asList(
                RawBsonDocument.parse("{ _id: { _data: 1 }, x: 1 }"),
                RawBsonDocument.parse("{ _id: { _data: 2 }, x: 1 }"));
        List<RawBsonDocument> secondBatch = Arrays.asList(
                RawBsonDocument.parse("{ _id: { _data: 3 }, x: 2 }"));

        @SuppressWarnings("unchecked")
        AggregateResponseBatchCursor<RawBsonDocument> batchCursor = mock(AggregateResponseBatchCursor.class);
        RawBsonDocumentCodec codec = new RawBsonDocumentCodec();
        BsonDocument resumeToken = mock(BsonDocument.class);

        when(batchCursor.tryNext()).thenReturn(firstBatch, null, secondBatch, null);

        MongoChangeStreamCursorImpl<RawBsonDocument> cursor = new MongoChangeStreamCursorImpl<>(batchCursor, codec, resumeToken);

        assertEquals(firstBatch.get(0), cursor.tryNext());
        assertEquals(firstBatch.get(1), cursor.tryNext());
        assertNull(cursor.tryNext());
        assertEquals(secondBatch.get(0), cursor.tryNext());
        assertNull(cursor.tryNext());
    }

    @Test
    @DisplayName("should get cached resume token after next")
    void shouldGetCachedResumeTokenAfterNext() {
        List<RawBsonDocument> firstBatch = Arrays.asList(
                RawBsonDocument.parse("{ _id: { _data: 1 }, x: 1 }"),
                RawBsonDocument.parse("{ _id: { _data: 2 }, x: 1 }"));
        List<RawBsonDocument> secondBatch = Arrays.asList(
                RawBsonDocument.parse("{ _id: { _data: 3 }, x: 2 }"));

        @SuppressWarnings("unchecked")
        AggregateResponseBatchCursor<RawBsonDocument> batchCursor = mock(AggregateResponseBatchCursor.class);
        RawBsonDocumentCodec codec = new RawBsonDocumentCodec();
        BsonDocument resumeToken = new BsonDocument("_data", new BsonInt32(1));

        when(batchCursor.hasNext()).thenReturn(true, true, true, false);
        when(batchCursor.next()).thenReturn(firstBatch, secondBatch);
        when(batchCursor.getPostBatchResumeToken()).thenReturn(
                new BsonDocument("_data", new BsonInt32(2)),
                new BsonDocument("_data", new BsonInt32(2)),
                new BsonDocument("_data", new BsonInt32(3)),
                new BsonDocument("_data", new BsonInt32(3)));

        MongoChangeStreamCursorImpl<RawBsonDocument> cursor = new MongoChangeStreamCursorImpl<>(batchCursor, codec, resumeToken);

        assertEquals(resumeToken, cursor.getResumeToken());
        assertEquals(firstBatch.get(0), cursor.next());
        assertEquals(new BsonDocument("_data", new BsonInt32(1)), cursor.getResumeToken());
        assertEquals(firstBatch.get(1), cursor.next());
        assertEquals(new BsonDocument("_data", new BsonInt32(2)), cursor.getResumeToken());
        assertEquals(secondBatch.get(0), cursor.next());
        assertEquals(new BsonDocument("_data", new BsonInt32(3)), cursor.getResumeToken());
    }

    @Test
    @DisplayName("should get cached resume token after tryNext")
    void shouldGetCachedResumeTokenAfterTryNext() {
        List<RawBsonDocument> firstBatch = Arrays.asList(
                RawBsonDocument.parse("{ _id: { _data: 1 }, x: 1 }"),
                RawBsonDocument.parse("{ _id: { _data: 2 }, x: 1 }"));
        List<RawBsonDocument> secondBatch = Arrays.asList(
                RawBsonDocument.parse("{ _id: { _data: 3 }, x: 2 }"));

        @SuppressWarnings("unchecked")
        AggregateResponseBatchCursor<RawBsonDocument> batchCursor = mock(AggregateResponseBatchCursor.class);
        RawBsonDocumentCodec codec = new RawBsonDocumentCodec();
        BsonDocument resumeToken = new BsonDocument("_data", new BsonInt32(1));

        when(batchCursor.hasNext()).thenReturn(true, true, true, false);
        when(batchCursor.tryNext()).thenReturn(firstBatch, null, secondBatch, null);
        when(batchCursor.getPostBatchResumeToken()).thenReturn(
                new BsonDocument("_data", new BsonInt32(2)),
                new BsonDocument("_data", new BsonInt32(2)),
                new BsonDocument("_data", new BsonInt32(2)),
                new BsonDocument("_data", new BsonInt32(2)),
                new BsonDocument("_data", new BsonInt32(3)),
                new BsonDocument("_data", new BsonInt32(3)),
                new BsonDocument("_data", new BsonInt32(3)));

        MongoChangeStreamCursorImpl<RawBsonDocument> cursor = new MongoChangeStreamCursorImpl<>(batchCursor, codec, resumeToken);

        assertEquals(resumeToken, cursor.getResumeToken());
        assertEquals(firstBatch.get(0), cursor.tryNext());
        assertEquals(new BsonDocument("_data", new BsonInt32(1)), cursor.getResumeToken());
        assertEquals(firstBatch.get(1), cursor.tryNext());
        assertEquals(new BsonDocument("_data", new BsonInt32(2)), cursor.getResumeToken());
        assertNull(cursor.tryNext());
        assertEquals(new BsonDocument("_data", new BsonInt32(2)), cursor.getResumeToken());
        assertEquals(secondBatch.get(0), cursor.tryNext());
        assertEquals(new BsonDocument("_data", new BsonInt32(3)), cursor.getResumeToken());
        assertNull(cursor.tryNext());
        assertEquals(new BsonDocument("_data", new BsonInt32(3)), cursor.getResumeToken());
    }

    @Test
    @DisplayName("should report available documents")
    void shouldReportAvailableDocuments() {
        List<RawBsonDocument> firstBatch = Arrays.asList(
                RawBsonDocument.parse("{ _id: { _data: 1 }, x: 1 }"),
                RawBsonDocument.parse("{ _id: { _data: 2 }, x: 1 }"));
        List<RawBsonDocument> secondBatch = Arrays.asList(
                RawBsonDocument.parse("{ _id: { _data: 3 }, x: 2 }"));

        @SuppressWarnings("unchecked")
        AggregateResponseBatchCursor<RawBsonDocument> batchCursor = mock(AggregateResponseBatchCursor.class);

        when(batchCursor.hasNext()).thenReturn(true, true, true, true, false);
        when(batchCursor.next()).thenReturn(firstBatch, secondBatch);
        when(batchCursor.available()).thenReturn(2, 2, 0, 0, 0, 1, 0, 0, 0);

        MongoChangeStreamCursorImpl<RawBsonDocument> cursor =
                new MongoChangeStreamCursorImpl<>(batchCursor, new RawBsonDocumentCodec(), new BsonDocument("_data", new BsonInt32(1)));

        assertEquals(2, cursor.available());
        cursor.hasNext();
        assertEquals(2, cursor.available());
        cursor.next();
        assertEquals(1, cursor.available());
        cursor.hasNext();
        assertEquals(1, cursor.available());
        cursor.next();
        assertEquals(0, cursor.available());
        cursor.hasNext();
        assertEquals(1, cursor.available());
        cursor.next();
        assertEquals(0, cursor.available());
        cursor.hasNext();
        assertEquals(0, cursor.available());
        cursor.close();
        assertEquals(0, cursor.available());
    }
}
