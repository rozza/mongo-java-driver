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

package com.mongodb;

import org.bson.BSONDecoder;
import org.bson.BasicBSONDecoder;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DBEncoderDecoderDBRefTest {

    @Test
    @DisplayName("should encode and decode DBRefs")
    void shouldEncodeAndDecodeDBRefs() {
        DBRef reference = new DBRef("coll", "hello world");
        DBObject document = new BasicDBObject("!", reference);
        OutputBuffer buffer = new BasicOutputBuffer();

        DefaultDBEncoder.FACTORY.create().writeObject(buffer, document);
        DefaultDBCallback callback = new DefaultDBCallback(null);
        BSONDecoder decoder = new BasicBSONDecoder();
        decoder.decode(buffer.toByteArray(), callback);
        DBRef decoded = (DBRef) ((DBObject) callback.get()).get("!");

        assertNull(decoded.getDatabaseName());
        assertEquals("coll", decoded.getCollectionName());
        assertEquals("hello world", decoded.getId());
    }

    @Test
    @DisplayName("should encode and decode DBRefs with a database name")
    void shouldEncodeAndDecodeDBRefsWithADatabaseName() {
        DBRef reference = new DBRef("db", "coll", "hello world");
        DBObject document = new BasicDBObject("!", reference);
        OutputBuffer buffer = new BasicOutputBuffer();

        DefaultDBEncoder.FACTORY.create().writeObject(buffer, document);
        DefaultDBCallback callback = new DefaultDBCallback(null);
        BSONDecoder decoder = new BasicBSONDecoder();
        decoder.decode(buffer.toByteArray(), callback);
        DBRef decoded = (DBRef) ((DBObject) callback.get()).get("!");

        assertEquals("db", decoded.getDatabaseName());
        assertEquals("coll", decoded.getCollectionName());
        assertEquals("hello world", decoded.getId());
    }
}
