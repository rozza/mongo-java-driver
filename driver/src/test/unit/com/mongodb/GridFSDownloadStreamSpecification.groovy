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

import com.mongodb.client.FindIterable
import com.mongodb.client.MongoCollection
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.types.ObjectId
import spock.lang.Specification

class GridFSDownloadStreamSpecification extends Specification {
    def fileInfo = new BsonDocument('_id', new BsonObjectId(new ObjectId()))
            .append('filename', new BsonString('filename'))
            .append('chunkSize', new BsonInt32(2))
            .append('length', new BsonInt64(3))

    def 'should return the file info'() {
        when:
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, Stub(MongoCollection))

        then:
        downloadStream.getFileInformation() == fileInfo
    }

    def 'should return an input stream'() {
        when:
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, Stub(MongoCollection))

        then:
        downloadStream.getInputStream() == downloadStream
    }

    def 'should query the chunks collection as expected'() {
        when:
        def twoBytes = new byte[2]
        def oneByte = new byte[1]
        def findQuery = new BsonDocument('files_id', fileInfo.getObjectId('_id')).append('n', new BsonInt32(0))
        def chunkDocument = new BsonDocument('files_id', fileInfo.getObjectId('_id'))
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
        def oneByte = new byte[1]
        def findQuery = new BsonDocument('files_id', fileInfo.getObjectId('_id')).append('n', new BsonInt32(1))
        def chunkDocument = new BsonDocument('files_id', fileInfo.getObjectId('_id'))
                .append('n', new BsonInt32(1))
                .append('data', new BsonBinary(oneByte))

        def findIterable = Mock(FindIterable)
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, chunksCollection)

        when:
        def result = downloadStream.skip(2)

        then:
        result == 2L
        1 * chunksCollection.find(findQuery) >> findIterable
        1 * findIterable.first() >> chunkDocument

        when:
        result = downloadStream.read()

        then:
        result == (oneByte[0] & 0xFF)
        0 * chunksCollection.find(_)
        0 * findIterable.first()

        when:
        result = downloadStream.read()

        then:
        result == -1
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

    def 'should handle skip that is larger than the file'() {
        given:
        def chunksCollection = Mock(MongoCollection)
        def downloadStream = new GridFSDownloadStreamImpl(fileInfo, chunksCollection)

        when:
        def result = downloadStream.skip(100)

        then:
        result == 3L

        when:
        result = downloadStream.read()

        then:
        result == -1
        0 * chunksCollection.find(_)
    }

    def 'should thrown an exception if passed invalid file info'() {
        when:
        new GridFSDownloadStreamImpl(new BsonDocument(), Stub(MongoCollection))

        then:
        thrown(MongoGridFSException)

        when:
        def badFileInfo = new BsonDocument('_id', new BsonString('123'))
                .append('filename', new BsonString('filename'))
                .append('length', new BsonString("2"))
                .append('chunkSize', new BsonString("2"))
        new GridFSDownloadStreamImpl(badFileInfo, Stub(MongoCollection))

        then:
        thrown(MongoGridFSException)

        when:
        badFileInfo.put("_id", fileInfo.get("_id"))
        new GridFSDownloadStreamImpl(badFileInfo, Stub(MongoCollection))

        then:
        thrown(MongoGridFSException)

        when:
        badFileInfo.put("length", fileInfo.get("length"))
        new GridFSDownloadStreamImpl(badFileInfo, Stub(MongoCollection))

        then:
        thrown(MongoGridFSException)
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
        def findQuery = new BsonDocument('files_id', fileInfo.getObjectId('_id')).append('n', new BsonInt32(0))
        def chunkDocument = new BsonDocument('files_id', fileInfo.getObjectId('_id'))
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
