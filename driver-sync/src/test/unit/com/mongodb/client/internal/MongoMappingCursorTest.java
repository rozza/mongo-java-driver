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
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MongoMappingCursorTest {

    @Test
    @DisplayName("should get server cursor and address")
    void shouldGetServerCursorAndAddress() {
        @SuppressWarnings("unchecked")
        MongoCursor<Document> cursor = mock(MongoCursor.class);
        ServerAddress address = new ServerAddress("host", 27018);
        ServerCursor serverCursor = new ServerCursor(5, address);
        when(cursor.getServerAddress()).thenReturn(address);
        when(cursor.getServerCursor()).thenReturn(serverCursor);

        MongoMappingCursor<Document, Document> mappingCursor = new MongoMappingCursor<>(cursor, d -> d);

        assertSame(address, mappingCursor.getServerAddress());
        assertSame(serverCursor, mappingCursor.getServerCursor());
    }

    @Test
    @DisplayName("should throw on remove")
    void shouldThrowOnRemove() {
        @SuppressWarnings("unchecked")
        MongoCursor<Document> cursor = mock(MongoCursor.class);
        doThrow(new UnsupportedOperationException()).when(cursor).remove();

        MongoMappingCursor<Document, Document> mappingCursor = new MongoMappingCursor<>(cursor, d -> d);

        assertThrows(UnsupportedOperationException.class, mappingCursor::remove);
    }

    @Test
    @DisplayName("should close cursor")
    void shouldCloseCursor() {
        @SuppressWarnings("unchecked")
        MongoCursor<Document> cursor = mock(MongoCursor.class);
        MongoMappingCursor<Document, Document> mappingCursor = new MongoMappingCursor<>(cursor, d -> d);

        mappingCursor.close();

        verify(cursor).close();
    }

    @Test
    @DisplayName("should have next if cursor does")
    void shouldHaveNextIfCursorDoes() {
        @SuppressWarnings("unchecked")
        MongoCursor<Document> cursor = mock(MongoCursor.class);
        when(cursor.hasNext()).thenReturn(true, false);

        MongoMappingCursor<Document, Document> mappingCursor = new MongoMappingCursor<>(cursor, d -> d);

        assertTrue(mappingCursor.hasNext());
        assertFalse(mappingCursor.hasNext());
    }

    @Test
    @DisplayName("should map next")
    void shouldMapNext() {
        @SuppressWarnings("unchecked")
        MongoCursor<Document> cursor = mock(MongoCursor.class);
        when(cursor.next()).thenReturn(new Document("_id", 1));

        MongoMappingCursor<Document, Object> mappingCursor = new MongoMappingCursor<>(cursor, d -> d.get("_id"));

        assertEquals(1, mappingCursor.next());
    }

    @Test
    @DisplayName("should map try next")
    void shouldMapTryNext() {
        @SuppressWarnings("unchecked")
        MongoCursor<Document> cursor = mock(MongoCursor.class);
        when(cursor.tryNext()).thenReturn(new Document("_id", 1), null);

        MongoMappingCursor<Document, Object> mappingCursor = new MongoMappingCursor<>(cursor, d -> d.get("_id"));

        assertEquals(1, mappingCursor.tryNext());
        assertNull(mappingCursor.tryNext());
    }
}
