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
import com.mongodb.internal.operation.BatchCursor;
import org.bson.Document;
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

class MongoBatchCursorAdapterTest {

    @Test
    @DisplayName("should get server cursor and address")
    void shouldGetServerCursorAndAddress() {
        @SuppressWarnings("unchecked")
        BatchCursor<Document> batchCursor = mock(BatchCursor.class);
        ServerAddress address = new ServerAddress("host", 27018);
        ServerCursor serverCursor = new ServerCursor(5, address);
        when(batchCursor.getServerAddress()).thenReturn(address);
        when(batchCursor.getServerCursor()).thenReturn(serverCursor);

        MongoBatchCursorAdapter<Document> cursor = new MongoBatchCursorAdapter<>(batchCursor);

        assertSame(address, cursor.getServerAddress());
        assertSame(serverCursor, cursor.getServerCursor());
    }

    @Test
    @DisplayName("should throw on remove")
    void shouldThrowOnRemove() {
        @SuppressWarnings("unchecked")
        BatchCursor<Document> batchCursor = mock(BatchCursor.class);
        MongoBatchCursorAdapter<Document> cursor = new MongoBatchCursorAdapter<>(batchCursor);

        assertThrows(UnsupportedOperationException.class, cursor::remove);
    }

    @Test
    @DisplayName("should close batch cursor")
    void shouldCloseBatchCursor() {
        @SuppressWarnings("unchecked")
        BatchCursor<Document> batchCursor = mock(BatchCursor.class);
        MongoBatchCursorAdapter<Document> cursor = new MongoBatchCursorAdapter<>(batchCursor);

        cursor.close();

        verify(batchCursor).close();
    }

    @Test
    @DisplayName("next should throw if there is no next")
    void nextShouldThrowIfThereIsNoNext() {
        @SuppressWarnings("unchecked")
        BatchCursor<Document> batchCursor = mock(BatchCursor.class);
        when(batchCursor.hasNext()).thenReturn(false);

        MongoBatchCursorAdapter<Document> cursor = new MongoBatchCursorAdapter<>(batchCursor);

        assertThrows(NoSuchElementException.class, cursor::next);
    }

    @Test
    @DisplayName("should get next from batch cursor")
    void shouldGetNextFromBatchCursor() {
        List<Document> firstBatch = Arrays.asList(new Document("x", 1), new Document("x", 1));
        List<Document> secondBatch = Arrays.asList(new Document("x", 2));

        @SuppressWarnings("unchecked")
        BatchCursor<Document> batchCursor = mock(BatchCursor.class);
        when(batchCursor.hasNext()).thenReturn(true, true, true, true, false);
        when(batchCursor.next()).thenReturn(firstBatch, secondBatch);

        MongoBatchCursorAdapter<Document> cursor = new MongoBatchCursorAdapter<>(batchCursor);

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
        List<Document> firstBatch = Arrays.asList(new Document("x", 1), new Document("x", 1));
        List<Document> secondBatch = Arrays.asList(new Document("x", 2));

        @SuppressWarnings("unchecked")
        BatchCursor<Document> batchCursor = mock(BatchCursor.class);
        when(batchCursor.tryNext()).thenReturn(firstBatch, null, secondBatch, null);

        MongoBatchCursorAdapter<Document> cursor = new MongoBatchCursorAdapter<>(batchCursor);

        assertEquals(firstBatch.get(0), cursor.tryNext());
        assertEquals(firstBatch.get(1), cursor.tryNext());
        assertNull(cursor.tryNext());
        assertEquals(secondBatch.get(0), cursor.tryNext());
        assertNull(cursor.tryNext());
    }

    @Test
    @DisplayName("should report available documents")
    void shouldReportAvailableDocuments() {
        List<Document> firstBatch = Arrays.asList(new Document("x", 1), new Document("x", 1));
        List<Document> secondBatch = Arrays.asList(new Document("x", 2));

        @SuppressWarnings("unchecked")
        BatchCursor<Document> batchCursor = mock(BatchCursor.class);
        when(batchCursor.hasNext()).thenReturn(true, true, true, true, false);
        when(batchCursor.next()).thenReturn(firstBatch, secondBatch);
        when(batchCursor.available()).thenReturn(2, 2, 0, 0, 0, 1, 0, 0, 0);

        MongoBatchCursorAdapter<Document> cursor = new MongoBatchCursorAdapter<>(batchCursor);

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

    @Test
    @DisplayName("should close cursor in forEachRemaining if there is an exception")
    void shouldCloseCursorInForEachRemainingIfException() {
        List<Document> firstBatch = Arrays.asList(new Document("x", 1));

        @SuppressWarnings("unchecked")
        BatchCursor<Document> batchCursor = mock(BatchCursor.class);
        when(batchCursor.hasNext()).thenReturn(true, true);
        when(batchCursor.next()).thenReturn(firstBatch);

        MongoBatchCursorAdapter<Document> cursor = new MongoBatchCursorAdapter<>(batchCursor);

        assertThrows(IllegalStateException.class, () ->
                cursor.forEachRemaining(doc -> {
                    throw new IllegalStateException("test");
                }));

        verify(batchCursor).close();
    }
}
