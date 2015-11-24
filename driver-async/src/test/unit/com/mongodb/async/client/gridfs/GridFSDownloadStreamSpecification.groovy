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
import com.mongodb.async.AsyncBatchCursor
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.client.FindIterable
import com.mongodb.async.client.MongoCollection
import com.mongodb.client.gridfs.model.GridFSFile
import org.bson.BsonObjectId
import org.bson.Document
import org.bson.types.Binary
import org.bson.types.ObjectId
import spock.lang.Specification

import java.nio.ByteBuffer

class GridFSDownloadStreamSpecification extends Specification {
    def fileInfo = new GridFSFile(new BsonObjectId(new ObjectId()), 'filename', 3L, 2, new Date(), 'abc', new Document())

    def 'should return the file info'() {
        given:
        def gridFSFindIterable = Mock(GridFSFindIterable)
        def downloadStream = new GridFSDownloadStreamImpl(gridFSFindIterable, Stub(MongoCollection))

        when:
        def futureResults = new FutureResultCallback()
        downloadStream.getGridFSFile(futureResults)

        then:
        1 * gridFSFindIterable.first(_) >> { it[0].onResult(fileInfo, null) }
        futureResults.get() == fileInfo

        when: 'Ensure that the fileInfo is cached'
        futureResults = new FutureResultCallback()
        downloadStream.getGridFSFile(futureResults)

        then:
        0 * gridFSFindIterable.first(_)
        futureResults.get() == fileInfo
    }

    def 'should query the chunks collection as expected'() {
        given:
        def twoBytes = new byte[2]
        def oneByte = new byte[1]
        def findQuery = new Document('files_id', fileInfo.getId()).append('n', new Document('$gte', 0))
        def sort = new Document('n', 1)
        def chunkDocument = new Document('files_id', fileInfo.getId())
                .append('n', 0)
                .append('data', new Binary(twoBytes))

        def secondChunkDocument = new Document('files_id', fileInfo.getId())
                .append('n', 1)
                .append('data', new Binary(oneByte))

        def gridFSFindIterable = Mock(GridFSFindIterable)
        def batchCursor = Mock(AsyncBatchCursor)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(gridFSFindIterable, chunksCollection)

        when:
        def firstByteBuffer = ByteBuffer.allocate(2)
        def futureResults = new FutureResultCallback()
        downloadStream.read(firstByteBuffer, futureResults)

        then:
        1 * gridFSFindIterable.first(_) >> { it[0].onResult(fileInfo, null) }
        1 * chunksCollection.find(findQuery) >> findIterable
        1 * findIterable.sort(sort) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.batchCursor(_) >> { it[0].onResult(batchCursor, null) }
        1 * batchCursor.next(_) >> { it[0].onResult([chunkDocument, secondChunkDocument], null) }

        then:
        futureResults.get() == 2
        firstByteBuffer.flip() == ByteBuffer.wrap(twoBytes)

        when:
        def secondByteBuffer = ByteBuffer.allocate(1)
        futureResults = new FutureResultCallback()
        downloadStream.read(secondByteBuffer, futureResults)

        then:
        futureResults.get() == 1
        0 * batchCursor.next(_)
        secondByteBuffer.flip() == ByteBuffer.wrap(oneByte)

        when:
        def thirdByteBuffer = ByteBuffer.allocate(1)
        futureResults = new FutureResultCallback()
        downloadStream.read(thirdByteBuffer, futureResults)

        then:
        futureResults.get() == -1
        0 * batchCursor.next(_)
        thirdByteBuffer == ByteBuffer.allocate(1)
    }

    def 'should create a new cursor each time when using batchSize 1'() {
        given:
        def twoBytes = new byte[2]
        def oneByte = new byte[1]
        def findQuery = new Document('files_id', fileInfo.getId()).append('n', new Document('$gte', 0))
        def sort = new Document('n', 1)
        def chunkDocument = new Document('files_id', fileInfo.getId())
                .append('n', 0)
                .append('data', new Binary(twoBytes))

        def secondChunkDocument = new Document('files_id', fileInfo.getId())
                .append('n', 1)
                .append('data', new Binary(oneByte))

        def gridFSFindIterable = Mock(GridFSFindIterable)
        def batchCursor = Mock(AsyncBatchCursor)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(gridFSFindIterable, chunksCollection).batchSize(1)

        when:
        def firstByteBuffer = ByteBuffer.allocate(2)
        def futureResults = new FutureResultCallback()
        downloadStream.read(firstByteBuffer, futureResults)

        then:
        1 * gridFSFindIterable.first(_) >> { it[0].onResult(fileInfo, null) }
        1 * chunksCollection.find(findQuery) >> findIterable
        1 * findIterable.sort(sort) >> findIterable
        1 * findIterable.batchSize(1) >> findIterable
        1 * findIterable.batchCursor(_) >> { it[0].onResult(batchCursor, null) }
        1 * batchCursor.next(_) >> { it[0].onResult([chunkDocument], null) }

        then:
        futureResults.get() == 2
        firstByteBuffer.flip() == ByteBuffer.wrap(twoBytes)

        when:
        def secondByteBuffer = ByteBuffer.allocate(1)
        futureResults = new FutureResultCallback()
        findQuery.put('n', new Document('$gte', 1))
        downloadStream.read(secondByteBuffer, futureResults)

        then:
        1 * chunksCollection.find(findQuery) >> findIterable
        1 * findIterable.batchSize(1) >> findIterable
        1 * findIterable.sort(sort) >> findIterable
        1 * findIterable.batchCursor(_) >> { it[0].onResult(batchCursor, null) }
        1 * batchCursor.next(_) >> { it[0].onResult([secondChunkDocument], null) }

        then:
        futureResults.get() == 1
        secondByteBuffer.flip() == ByteBuffer.wrap(oneByte)

        when:
        def thirdByteBuffer = ByteBuffer.allocate(1)
        futureResults = new FutureResultCallback()
        downloadStream.read(thirdByteBuffer, futureResults)

        then:
        0 * chunksCollection.find(_) >> findIterable

        then:
        futureResults.get() == -1
        thirdByteBuffer == ByteBuffer.allocate(1)
    }

    def 'should throw if trying to pass negative batchSize'() {
        given:
        def downloadStream = new GridFSDownloadStreamImpl(Stub(GridFSFindIterable), Stub(MongoCollection))

        when:
        downloadStream.batchSize(0)

        then:
        notThrown(IllegalArgumentException)


        when:
        downloadStream.batchSize(-1)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should throw if no chunks found when data is expected'() {
        given:
        def gridFSFindIterable = Mock(GridFSFindIterable)
        def batchCursor = Mock(AsyncBatchCursor)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(gridFSFindIterable, chunksCollection).batchSize(1)

        when:
        def firstByteBuffer = ByteBuffer.allocate(2)
        def futureResults = new FutureResultCallback()
        downloadStream.read(firstByteBuffer, futureResults)

        then:
        1 * gridFSFindIterable.first(_) >> { it[0].onResult(fileInfo, null) }
        1 * chunksCollection.find(_) >> findIterable
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.batchSize(1) >> findIterable
        1 * findIterable.batchCursor(_) >> { it[0].onResult(batchCursor, null) }
        1 * batchCursor.next(_) >> { it[0].onResult([], null) }

        when:
        futureResults.get()

        then:
        thrown(MongoGridFSException)

    }

    def 'should throw if chunk data differs from the expected'() {
        given:
        def findQuery = new Document('files_id', fileInfo.getId()).append('n', new Document('$gte', 0))
        def sort = new Document('n', 1)
        def chunkDocument = new Document('files_id', fileInfo.getId())
                .append('n', 0)
                .append('data', new Binary(data))

        def gridFSFindIterable = Mock(GridFSFindIterable)
        def batchCursor = Mock(AsyncBatchCursor)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(gridFSFindIterable, chunksCollection)

        when:
        def firstByteBuffer = ByteBuffer.allocate(2)
        def futureResults = new FutureResultCallback()
        downloadStream.read(firstByteBuffer, futureResults)

        then:
        1 * gridFSFindIterable.first(_) >> { it[0].onResult(fileInfo, null) }
        1 * chunksCollection.find(findQuery) >> findIterable
        1 * findIterable.sort(sort) >> findIterable
        1 * findIterable.batchSize(0) >> findIterable
        1 * findIterable.batchCursor(_) >> { it[0].onResult(batchCursor, null) }
        1 * batchCursor.next(_) >> { it[0].onResult([chunkDocument], null) }

        when:
        futureResults.get()

        then:
        thrown(MongoGridFSException)

        where:
        data << [new byte[1], new byte[100]]
    }

    def 'should throw an exception when trying to action post close'() {
        given:
        def downloadStream = new GridFSDownloadStreamImpl(Stub(GridFSFindIterable), Stub(MongoCollection))
        def futureResults = new FutureResultCallback()
        downloadStream.close(futureResults)
        futureResults.get()

        when:
        futureResults = new FutureResultCallback()
        downloadStream.read(ByteBuffer.allocate(1), futureResults)
        futureResults.get()

        then:
        thrown(MongoGridFSException)

        when:
        futureResults = new FutureResultCallback()
        downloadStream.getGridFSFile(futureResults)
        futureResults.get()

        then:
        thrown(MongoGridFSException)

        when:
        futureResults = new FutureResultCallback()
        downloadStream.close(futureResults)
        futureResults.get()

        then:
        notThrown(MongoGridFSException)
    }
}
