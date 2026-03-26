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

package com.mongodb.client.gridfs.model;

import com.mongodb.MongoGridFSException;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GridFSFileTest {

    @Test
    void shouldReturnExpectedValues() {
        BsonObjectId id = new BsonObjectId(new ObjectId());
        String filename = "filename";
        long length = 100L;
        int chunkSize = 255;
        Date uploadDate = new Date();
        Document metadata = new Document("id", id);

        GridFSFile gridFSFile = new GridFSFile(id, filename, length, chunkSize, uploadDate, metadata);

        assertEquals(id, gridFSFile.getId());
        assertEquals(filename, gridFSFile.getFilename());
        assertEquals(length, gridFSFile.getLength());
        assertEquals(chunkSize, gridFSFile.getChunkSize());
        assertEquals(uploadDate, gridFSFile.getUploadDate());
        assertEquals(metadata, gridFSFile.getMetadata());
    }

    @Test
    void shouldThrowExceptionWhenUsingGetObjectIdWithCustomIdTypes() {
        GridFSFile gridFSFile = new GridFSFile(new BsonString("id"), "test", 10L, 225, new Date(), null);
        assertThrows(MongoGridFSException.class, gridFSFile::getObjectId);
    }
}
