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

import com.mongodb.MongoGridFSException
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.async.client.MongoCollection
import org.bson.Document
import org.bson.types.Binary
import org.bson.types.ObjectId
import spock.lang.Specification

import java.nio.ByteBuffer
import java.security.MessageDigest

class GridFSUploadStreamSpecification  extends Specification {
    def fileId = new ObjectId()
    def filename = 'filename'
    def metadata = new Document()
    def content = 'file content ' as byte[]

    def 'should return the file id'() {
        when:
        def uploadStream = new GridFSUploadStreamImpl(Stub(MongoCollection), Stub(MongoCollection), fileId, filename, 255, metadata)

        then:
        uploadStream.getFileId() == fileId
    }

    def 'should write the buffer it reaches the chunk size'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def callback = Stub(SingleResultCallback)
        def uploadStream = new GridFSUploadStreamImpl(filesCollection, chunksCollection, fileId, filename, 2, metadata)

        when:
        uploadStream.write(ByteBuffer.wrap(new byte[1]), callback)


        then:
        0 * chunksCollection.insertOne(_, _)

        when:
        uploadStream.write(ByteBuffer.wrap(new byte[1]), callback)

        then:
        1 * chunksCollection.insertOne(_, _)
    }

    def 'should write to the files collection on close'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def callback = Stub(SingleResultCallback)
        def uploadStream = new GridFSUploadStreamImpl(filesCollection, chunksCollection, fileId, filename, 255, null)
        def byteBuffer = ByteBuffer.wrap(new byte[10])

        when:
        uploadStream.write(byteBuffer, callback)

        then:
        0 * chunksCollection.insertOne(_, _)

        when:
        uploadStream.close(callback)

        then:
        1 * chunksCollection.insertOne(_, _) >> { it[1].onResult(null, null) }
        1 * filesCollection.insertOne(_, _)
    }

    def 'should write to the files and chunks collection as expected close'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def callback = Stub(SingleResultCallback)
        def metadata = new Document('contentType', 'text/txt')
        def uploadStream = new GridFSUploadStreamImpl(filesCollection, chunksCollection, fileId, filename, 255, metadata)

        when:
        uploadStream.write(ByteBuffer.wrap(content), callback )
        uploadStream.close(callback)

        then:
        1 * chunksCollection.insertOne(_, _) >> { Document chunksData, SingleResultCallback<Void> insertCallback ->
            chunksData.getObjectId('files_id') == fileId
            chunksData.getInteger('n') == 0
            chunksData.get('data', Binary).getData() == content
            insertCallback.onResult(null, null)
        }

        1 * filesCollection.insertOne(_, _) >> { Document fileData, SingleResultCallback<Void> insertCallback ->
            fileData.getObjectId('_id') == fileId &&
            fileData.getString('filename') == filename &&
            fileData.getLong('length') == content.length as Long &&
            fileData.getInteger('chunkSize') == 255 &&
            fileData.getString('md5') == MessageDigest.getInstance('MD5').digest(content).encodeHex().toString()
            fileData.get('metadata', Document) == metadata
        }
    }

    def 'should not write an empty chunk'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def callback = Stub(SingleResultCallback)
        def uploadStream = new GridFSUploadStreamImpl(filesCollection, chunksCollection, fileId, filename, 255, metadata)

        when:
        uploadStream.close(callback)

        then:
        0 * chunksCollection.insertOne(_, _)
        1 * filesCollection.insertOne(_, _)
    }

    def 'should delete any chunks when calling abort'() {
        given:
        def chunksCollection = Mock(MongoCollection)
        def callback = Stub(SingleResultCallback)
        def uploadStream = new GridFSUploadStreamImpl(Stub(MongoCollection), chunksCollection, fileId, filename, 255, metadata)

        when:
        uploadStream.write(ByteBuffer.wrap(content), callback)
        uploadStream.abort(callback)

        then:
        1 * chunksCollection.deleteMany(new Document('files_id', fileId), _)
    }

    def 'should close the stream on abort'() {
        given:
        def callback = Stub(SingleResultCallback)
        def uploadStream = new GridFSUploadStreamImpl(Stub(MongoCollection), Stub(MongoCollection), fileId, filename, 255, metadata)
        uploadStream.write(ByteBuffer.wrap(content), callback)
        uploadStream.abort(callback)

        when:
        def futureResults = new FutureResultCallback()
        uploadStream.write(ByteBuffer.wrap(content), futureResults)
        futureResults.get()

        then:
        thrown(MongoGridFSException)
    }

    def 'should throw an exception when trying to action post close'() {
        given:
        def callback = Stub(SingleResultCallback)
        def uploadStream = new GridFSUploadStreamImpl(Stub(MongoCollection), Stub(MongoCollection), fileId, filename, 255, metadata)
        uploadStream.close(callback)

        when:
        def futureResults = new FutureResultCallback()
        uploadStream.write(ByteBuffer.wrap(content), futureResults)
        futureResults.get()

        then:
        thrown(MongoGridFSException)
    }
}
