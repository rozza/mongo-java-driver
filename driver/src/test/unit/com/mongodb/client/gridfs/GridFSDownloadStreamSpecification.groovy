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

package com.mongodb.client.gridfs

import com.mongodb.MongoGridFSException
import com.mongodb.client.FindIterable
import com.mongodb.client.MongoCollection
import com.mongodb.client.gridfs.model.GridFSFile
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonObjectId
import org.bson.Document
import org.bson.types.ObjectId
import spock.lang.Specification

class GridFSDownloadStreamSpecification extends Specification {
    def fileInfo = new GridFSFile(new BsonObjectId(new ObjectId()), 'filename', 3L, 2, new Date(), 'abc', new Document())

    def 'should return the file info'() {
        when:
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, Stub(MongoCollection))

        then:
        downloadStream.getGridFSFile() == fileInfo
    }

    def 'should query the chunks collection as expected'() {
        when:
        def twoBytes = new byte[2]
        def oneByte = new byte[1]
        def findQuery = new BsonDocument('files_id', fileInfo.getId()).append('n', new BsonInt32(0))
        def chunkDocument = new BsonDocument('files_id', fileInfo.getId())
                .append('n', new BsonInt32(0))
                .append('data', new BsonBinary(twoBytes))

        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, chunksCollection)

        then:
        downloadStream.available() == 0

        when:
        def result = downloadStream.read()

        then:
        result == (twoBytes[0] & 0xFF)
        1 * chunksCollection.find(findQuery) >> findIterable
        1 * findIterable.first() >> chunkDocument
        downloadStream.available() == 1

        when:
        result = downloadStream.read()

        then:
        result == (twoBytes[1] & 0xFF)
        0 * chunksCollection.find(_)
        0 * findIterable.first()
        downloadStream.available() == 0

        when:
        result = downloadStream.read()

        then:
        result == (oneByte[0] & 0xFF)
        1 * chunksCollection.find(findQuery.append('n', new BsonInt32(1))) >> findIterable
        1 * findIterable.first() >> chunkDocument.append('data', new BsonBinary(oneByte))

        when:
        result = downloadStream.read()

        then:
        result == -1
        0 * chunksCollection.find(_)
        0 * findIterable.first()
    }

    def 'should skip to the correct point'() {
        given:
        def fileInfo = new GridFSFile(new BsonObjectId(new ObjectId()), 'filename', 60L, 25, new Date(), 'abc', new Document())

        def firstChunkBytes = 1..25 as byte[]
        def thirdChunkBytes = 51 .. 60 as byte[]
        def findQueries = [new BsonDocument('files_id', fileInfo.getId()).append('n', new BsonInt32(0)),
                           new BsonDocument('files_id', fileInfo.getId()).append('n', new BsonInt32(2))]
        def chunkDocuments =
                [new BsonDocument('files_id', fileInfo.getId()).append('n', new BsonInt32(0))
                         .append('data', new BsonBinary(firstChunkBytes)),
                 new BsonDocument('files_id', fileInfo.getId()).append('n', new BsonInt32(2))
                         .append('data', new BsonBinary(thirdChunkBytes))]

        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, chunksCollection)

        when:
        def skipResult = downloadStream.skip(15)

        then:
        skipResult == 15L
        0 * chunksCollection.find(_)
        0 * findIterable.first()

        when:
        def readByte = new byte[5]
        downloadStream.read(readByte)

        then:
        1 * chunksCollection.find(findQueries[0]) >> findIterable
        1 * findIterable.first() >> chunkDocuments[0]

        then:
        readByte == [16, 17, 18, 19, 20] as byte[]

        when:
        skipResult = downloadStream.skip(35)

        then:
        skipResult == 35L
        0 * chunksCollection.find(_)
        0 * findIterable.first()

        when:
        downloadStream.read(readByte)

        then:
        1 * chunksCollection.find(findQueries[1]) >> findIterable
        1 * findIterable.first() >> chunkDocuments[1]

        then:
        readByte == [56, 57, 58, 59, 60] as byte[]

        when:
        skipResult = downloadStream.skip(1)

        then:
        skipResult == 0L
        0 * chunksCollection.find(_)
        0 * findIterable.first()
    }

    def 'should handle negative skip value correctly '() {
        given:
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, Stub(MongoCollection))

        when:
        def result = downloadStream.skip(-1)

        then:
        result == 0L
    }

    def 'should handle skip that is larger or equal to the file length'() {
        given:
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, chunksCollection)

        when:
        def result = downloadStream.skip(skipValue)

        then:
        result == 3L
        0 * chunksCollection.find(_)

        when:
        result = downloadStream.read()

        then:
        result == -1
        0 * chunksCollection.find(_)

        where:
        skipValue << [3, 100]
    }

    def 'should throw if no chunks found when data is expected'() {
        given:
        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, chunksCollection)

        when:
        downloadStream.read()

        then:
        1 * chunksCollection.find(_) >> findIterable
        1 * findIterable.first() >> null

        then:
        thrown(MongoGridFSException)
    }

    def 'should throw if chunk data is the wrong size'() {
        given:
        def oneByte = new byte[1]
        def findQuery = new BsonDocument('files_id', fileInfo.getId()).append('n', new BsonInt32(0))
        def chunkDocument = new BsonDocument('files_id', fileInfo.getId())
                .append('n', new BsonInt32(0))
                .append('data', new BsonBinary(oneByte))

        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, chunksCollection)

        when:
        downloadStream.read()

        then:
        1 * chunksCollection.find(findQuery) >> findIterable
        1 * findIterable.first() >> chunkDocument

        then:
        thrown(MongoGridFSException)
    }

    def 'should throw an exception when trying to action post close'() {
        given:
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, Stub(MongoCollection))
        downloadStream.close()

        when:
        downloadStream.read()

        then:
        thrown(MongoGridFSException)

        when:
        downloadStream.skip(10)

        then:
        thrown(MongoGridFSException)


        when:
        downloadStream.read(new byte[10])

        then:
        thrown(MongoGridFSException)

        when:
        downloadStream.read(new byte[10], 0, 10)

        then:
        thrown(MongoGridFSException)
    }
}
