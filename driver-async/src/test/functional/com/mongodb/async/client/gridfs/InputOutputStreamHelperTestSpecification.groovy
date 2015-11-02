/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.async.client.gridfs

import com.mongodb.async.client.FunctionalSpecification
import com.mongodb.async.client.MongoCollection
import com.mongodb.async.client.MongoDatabase
import org.bson.Document

import static com.mongodb.async.client.Fixture.getDefaultDatabaseName
import static com.mongodb.async.client.Fixture.getMongoClient
import static com.mongodb.async.client.gridfs.InputOutputStreamHelper.toAsyncInputStream
import static com.mongodb.async.client.gridfs.InputOutputStreamHelper.toAsyncOutputStream
import static com.mongodb.async.client.gridfs.GridFSTestHelper.run

class InputOutputStreamHelperTestSpecification extends FunctionalSpecification {
    protected MongoDatabase mongoDatabase;
    protected MongoCollection<Document> filesCollection;
    protected MongoCollection<Document> chunksCollection;
    protected GridFSBucket gridFSBucket;

    def setup() {
        mongoDatabase = getMongoClient().getDatabase(getDefaultDatabaseName())
        filesCollection = mongoDatabase.getCollection('fs.files')
        chunksCollection = mongoDatabase.getCollection('fs.chunks')
        run(filesCollection.&drop)
        run(chunksCollection.&drop)
        gridFSBucket = new GridFSBucketImpl(mongoDatabase)
    }

    def cleanup() {
        if (filesCollection != null) {
            run(filesCollection.&drop)
            run(chunksCollection.&drop)
        }
    }

    def 'should round trip a InputOutputStreams'() {
        when:
        def content = 'Hello GridFS Byte Channels'
        def objectId = run(gridFSBucket.&uploadFromStream, 'myFile', toAsyncInputStream(new ByteArrayInputStream(content.getBytes())))

        then:
        run(filesCollection.&count) == 1
        run(chunksCollection.&count) == 1

        when:
        def outputStream = new ByteArrayOutputStream()
        run(gridFSBucket.&downloadToStream, objectId, toAsyncOutputStream(outputStream))

        then:
        content == new String(outputStream.toByteArray())
    }
}
