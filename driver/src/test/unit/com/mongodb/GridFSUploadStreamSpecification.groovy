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

package com.mongodb

import com.mongodb.client.MongoCollection
import org.bson.BsonDocument
import org.bson.types.ObjectId
import spock.lang.Specification

class GridFSUploadStreamSpecification extends Specification {
    def fileId = new ObjectId()
    def filename = 'filename'
    def metadata = new BsonDocument()

    def 'should return the file id'() {
        when:
        def uploadStream = new GridFSUploadStreamImpl(Stub(MongoCollection), Stub(MongoCollection), fileId, filename, 255, metadata)

        then:
        uploadStream.getFileId() == fileId
    }

    def 'should return an output stream'() {
        when:
        def uploadStream = new GridFSUploadStreamImpl(Stub(MongoCollection), Stub(MongoCollection), fileId, filename, 255, metadata)

        then:
        uploadStream.getOutputStream() == uploadStream
    }

    def 'should write the buffer it reaches the chunk size'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(filesCollection, chunksCollection, fileId, filename, 2, metadata)

        when:
        uploadStream.write(1)

        then:
        0 * chunksCollection.insertOne(_)

        when:
        uploadStream.write(1)

        then:
        1 * chunksCollection.insertOne(_)
    }

    def 'should write to the files collection on close'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(filesCollection, chunksCollection, fileId, filename, 255, metadata)

        when:
        uploadStream.write('file content ' as byte[])

        then:
        0 * chunksCollection.insertOne(_)

        when:
        uploadStream.close()

        then:
        1 * chunksCollection.insertOne(_)
        1 * filesCollection.insertOne(_)
    }

    def 'should not write an empty chunk'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(filesCollection, chunksCollection, fileId, filename, 255, metadata)

        when:
        uploadStream.close()

        then:
        0 * chunksCollection.insertOne(_)
        1 * filesCollection.insertOne(_)
    }

    def 'should not do anything when calling flush'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(filesCollection, chunksCollection, fileId, filename, 255, metadata)

        when:
        uploadStream.write('file content ' as byte[])
        uploadStream.flush()

        then:
        0 * chunksCollection.insertOne(_)
    }

    def 'should throw an exception when trying to action post close'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def uploadStream = new GridFSUploadStreamImpl(filesCollection, chunksCollection, fileId, filename, 255, metadata)

        when:
        uploadStream.close()
        uploadStream.write(1)

        then:
        thrown(MongoGridFSException)

        when:
        uploadStream.close()

        then:
        thrown(MongoGridFSException)
    }
}
