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

import com.mongodb.MongoDatabaseImpl
import com.mongodb.MongoGridFSException
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.TestOperationExecutor
import com.mongodb.WriteConcern
import com.mongodb.client.FindIterable
import com.mongodb.client.ListIndexesIterable
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoCursor
import com.mongodb.client.MongoDatabase
import com.mongodb.client.gridfs.model.GridFSDownloadByNameOptions
import com.mongodb.client.gridfs.model.GridFSFile
import com.mongodb.client.result.DeleteResult
import com.mongodb.client.result.UpdateResult
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.configuration.CodecRegistry
import org.bson.types.Binary
import org.bson.types.ObjectId
import spock.lang.Specification
import spock.lang.Unroll

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondary
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

@SuppressWarnings('ClosureAsLastMethodParameter')
class GridFSBucketSpecification extends Specification {

    def readConcern = ReadConcern.DEFAULT

    def 'should return the correct bucket name'() {
        given:
        def database = Stub(MongoDatabase) {
            getReadConcern() >> ReadConcern.DEFAULT
        }

        when:
        def bucketName = new GridFSBucketImpl(database).getBucketName()

        then:
        bucketName == 'fs'

        when:
        bucketName = new GridFSBucketImpl(database, 'custom').getBucketName()

        then:
        bucketName == 'custom'
    }

    def 'should behave correctly when using withChunkSizeBytes'() {
        given:
        def newChunkSize = 200
        def database = Stub(MongoDatabase) {
            getReadConcern() >> ReadConcern.DEFAULT
        }

        when:
        def gridFSBucket = new GridFSBucketImpl(database).withChunkSizeBytes(newChunkSize)

        then:
        gridFSBucket.getChunkSizeBytes() == newChunkSize
    }

    def 'should behave correctly when using withReadPreference'() {
        given:
        def newReadPreference = primary()
        def database = Stub(MongoDatabase) {
            getReadConcern() >> ReadConcern.DEFAULT
        }

        when:
        def gridFSBucket = new GridFSBucketImpl(database).withReadPreference(newReadPreference)

        then:
        gridFSBucket.getReadPreference() == newReadPreference
    }

    def 'should behave correctly when using withWriteConcern'() {
        given:
        def newWriteConcern = WriteConcern.MAJORITY
        def database = Stub(MongoDatabase) {
            getReadConcern() >> ReadConcern.DEFAULT
        }

        when:
        def gridFSBucket = new GridFSBucketImpl(database).withWriteConcern(newWriteConcern)

        then:
        gridFSBucket.getWriteConcern() == newWriteConcern
    }

    def 'should behave correctly when using withReadConcern'() {
        given:
        def newReadConcern = ReadConcern.MAJORITY
        def database = Stub(MongoDatabase) {
            getReadConcern() >> ReadConcern.DEFAULT
        }

        when:
        def gridFSBucket = new GridFSBucketImpl(database).withReadConcern(newReadConcern)

        then:
        gridFSBucket.getReadConcern() == newReadConcern
    }

    def 'should get defaults from MongoDatabase'() {
        given:
        def defaultChunkSize = 255
        def database = new MongoDatabaseImpl('test', fromProviders(new DocumentCodecProvider()), secondary(), WriteConcern.ACKNOWLEDGED,
                readConcern, new TestOperationExecutor([]))

        when:
        def gridFSBucket = new GridFSBucketImpl(database)

        then:
        gridFSBucket.getChunkSizeBytes() == defaultChunkSize
        gridFSBucket.getReadPreference() == database.getReadPreference()
        gridFSBucket.getWriteConcern() == database.getWriteConcern()
        gridFSBucket.getReadConcern() == database.getReadConcern()
    }

    def 'should create the expected GridFSUploadStream'() {
        given:
        def filesCollection = Stub(MongoCollection)
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)

        when:
        def stream = gridFSBucket.openUploadStream('filename')

        then:
        expect stream, isTheSameAs(new GridFSUploadStreamImpl(filesCollection, chunksCollection, stream.getFileId(), 'filename',
                255, null), ['md5', 'closeLock'])
    }

    def 'should upload from stream'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)
        def contentBytes = 'content' as byte[]
        def inputStream = new ByteArrayInputStream(contentBytes)

        when:
        gridFSBucket.uploadFromStream('filename', inputStream)

        then:
        1 * chunksCollection.insertOne(_)

        then:
        1 * filesCollection.insertOne(_)
    }

    def 'should clean up any chunks when upload from stream throws an IOException'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)
        def inputStream = Mock(InputStream) {
            2 * read(_) >> 255 >> { throw new IOException('stream failure') }
        }

        when:
        gridFSBucket.uploadFromStream('filename', inputStream)

        then:
        1 * chunksCollection.insertOne(_)

        then:
        1 * chunksCollection.deleteMany(_)

        then:
        0 * filesCollection.insertOne(_)

        then:
        def exception = thrown(MongoGridFSException)
        exception.getMessage() == 'IOException when reading from the InputStream'
    }

    def 'should not clean up any chunks when upload throws an exception'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def alternativeException = new MongoGridFSException('Alternative failure')
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)
        def inputStream = Mock(InputStream) {
            2 * read(_) >> 255 >> { throw alternativeException }
        }

        when:
        gridFSBucket.uploadFromStream('filename', inputStream)

        then:
        1 * chunksCollection.insertOne(_)

        then:
        0 * chunksCollection.deleteMany(_)

        then:
        0 * filesCollection.insertOne(_)

        then:
        def exception = thrown(MongoGridFSException)
        exception == alternativeException
    }

    def 'should create the expected GridFSDownloadStream'() {
        given:
        def fileId = new BsonObjectId(new ObjectId())
        def fileInfo = new GridFSFile(fileId, 'File 1', 10, 255, new Date(), '1234', new Document())
        def findIterable =  Mock(FindIterable)
        def filesCollection = Mock(MongoCollection) {
            1 * find() >> findIterable
        }
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255,  Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)

        when:
        def stream = gridFSBucket.openDownloadStream(fileId.getValue())

        then:
        1 * findIterable.filter(_) >> findIterable
        1 * findIterable.map(_) >> findIterable
        1 * findIterable.first() >> fileInfo

        then:
        expect stream, isTheSameAs(new GridFSDownloadStreamImpl(fileInfo, chunksCollection), ['closeLock', 'cursorLock'])
    }

    def 'should download to stream'() {
        given:
        def fileId = new ObjectId()
        def bsonFileId = new BsonObjectId(fileId)
        def fileInfo = new GridFSFile(bsonFileId, 'filename', 10, 255, new Date(), '1234', new Document())
        def mongoCursor = Mock(MongoCursor)
        def findIterable =  Mock(FindIterable)
        def filesCollection = Mock(MongoCollection) {
            1 * find() >> findIterable
        }
        def tenBytes = new byte[10]
        def chunkDocument = new Document('files_id', fileInfo.getId()).append('n', 0).append('data', new Binary(tenBytes))
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255,  Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)
        def outputStream = new ByteArrayOutputStream(10)

        when:
        gridFSBucket.downloadToStream(fileId, outputStream)
        outputStream.close()

        then:
        1 * findIterable.filter(new Document('_id', bsonFileId)) >> findIterable
        1 * findIterable.map(_) >> findIterable
        1 * findIterable.first() >> fileInfo
        1 * chunksCollection.find(_) >> findIterable
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.batchSize(_) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocument

        then:
        outputStream.toByteArray() == tenBytes
    }

    def 'should download to stream using BsonValue'() {
        given:
        def bsonFileId = new BsonString('1')
        def fileInfo = new GridFSFile(bsonFileId, 'filename', 10L, 255, new Date(), '1234', new Document())
        def mongoCursor =  Mock(MongoCursor)
        def findIterable =  Mock(FindIterable)
        def filesCollection = Mock(MongoCollection) {
            1 * find() >> findIterable
        }
        def tenBytes = new byte[10]
        def chunkDocument = new Document('files_id', fileInfo.getId()).append('n', 0).append('data', new Binary(tenBytes))
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255,  Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)
        def outputStream = new ByteArrayOutputStream(10)

        when:
        gridFSBucket.downloadToStream(bsonFileId, outputStream)
        outputStream.close()

        then:
        1 * findIterable.filter(new Document('_id', bsonFileId)) >> findIterable
        1 * findIterable.map(_) >> findIterable
        1 * findIterable.first() >> fileInfo
        1 * chunksCollection.find(_) >> findIterable
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.batchSize(_) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocument

        then:
        outputStream.toByteArray() == tenBytes
    }

    def 'should download to stream by name'() {
        given:
        def filename = 'filename'
        def fileId = new ObjectId()
        def bsonFileId = new BsonObjectId(fileId)
        def fileInfo = new GridFSFile(bsonFileId, filename, 10, 255, new Date(), '1234', new Document())
        def mongoCursor =  Mock(MongoCursor)
        def findIterable =  Mock(FindIterable)
        def filesCollection = Mock(MongoCollection) {
            1 * find() >> findIterable
        }
        def tenBytes = new byte[10]
        def chunkDocument = new Document('files_id', fileInfo.getId()).append('n', 0).append('data', new Binary(tenBytes))
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)
        def outputStream = new ByteArrayOutputStream(10)

        when:
        gridFSBucket.downloadToStreamByName(filename, outputStream)
        outputStream.close()

        then:
        1 * findIterable.filter(new Document('filename', filename)) >> findIterable
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.map(_) >> findIterable
        1 * findIterable.first() >> fileInfo

        then:
        1 * chunksCollection.find(_) >> findIterable
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.batchSize(_) >> findIterable
        1 * findIterable.iterator() >> mongoCursor
        1 * mongoCursor.hasNext() >> true
        1 * mongoCursor.next() >> chunkDocument

        then:
        outputStream.toByteArray() == tenBytes
    }

    def 'should throw an exception if file not found'() {
        given:
        def fileId = new ObjectId()
        def bsonFileId = new BsonObjectId(fileId)
        def findIterable =  Mock(FindIterable)
        def filesCollection = Mock(MongoCollection) {
            1 * find() >> findIterable
        }
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)

        when:
        gridFSBucket.openDownloadStream(fileId)

        then:
        1 * findIterable.filter(new Document('_id', bsonFileId)) >> findIterable
        1 * findIterable.map(_) >> findIterable
        1 * findIterable.first() >> null

        then:
        thrown(MongoGridFSException)
    }

    @Unroll
    def 'should create the expected GridFSDownloadStream when opening by name with version: #version'() {
        given:
        def filename = 'filename'
        def fileId = new ObjectId()
        def bsonFileId = new BsonObjectId(fileId)
        def fileInfo = new GridFSFile(bsonFileId, filename, 10, 255, new Date(), '1234', new Document())
        def findIterable =  Mock(FindIterable)
        def filesCollection = Mock(MongoCollection) {
            1 * find() >> findIterable
        }
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)

        when:
        def stream = gridFSBucket.openDownloadStreamByName(filename, new GridFSDownloadByNameOptions().revision(version))

        then:
        1 * findIterable.filter(new Document('filename', filename)) >> findIterable
        1 * findIterable.skip(skip) >> findIterable
        1 * findIterable.sort(new Document('uploadDate', sortOrder)) >> findIterable
        1 * findIterable.map(_) >> findIterable
        1 * findIterable.first() >> fileInfo

        then:
        expect stream, isTheSameAs(new GridFSDownloadStreamImpl(fileInfo, chunksCollection), ['closeLock', 'cursorLock'])

        where:
        version | skip | sortOrder
          0     |  0   | 1
          1     |  1   | 1
          2     |  2   | 1
          3     |  3   | 1
          -1    |  0   | -1
          -2    |  1   | -1
          -3    |  2   | -1
    }

    def 'should create the expected GridFSFindIterable'() {
        given:
        def database = Mock(MongoDatabase)
        def collection = Mock(MongoCollection)
        def findIterable = Mock(FindIterable)
        def filter = new Document('filename', 'filename')
        def gridFSBucket = new GridFSBucketImpl(database, 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, collection, Stub(MongoCollection), true)

        when:
        def result = gridFSBucket.find()


        then:
        1 * collection.find() >> findIterable
        expect result, isTheSameAs(new GridFSFindIterableImpl(findIterable))

        when:
        result = gridFSBucket.find(filter)

        then:
        1 * collection.find() >> findIterable
        1 * findIterable.filter(filter)
        expect result, isTheSameAs(new GridFSFindIterableImpl(findIterable))
    }

    def 'should throw an exception if file not found when opening by name'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)
        when:
        gridFSBucket.openDownloadStreamByName('filename')

        then:
        1 * filesCollection.find() >> findIterable
        1 * findIterable.filter(new Document('filename', 'filename')) >> findIterable
        1 * findIterable.skip(0) >> findIterable
        1 * findIterable.sort(new Document('uploadDate', -1)) >> findIterable
        1 * findIterable.map(_) >> findIterable
        1 * findIterable.first() >> null

        then:
        thrown(MongoGridFSException)
    }

    def 'should create indexes on write'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def listIndexesIterable = Mock(ListIndexesIterable)
        def findIterable = Mock(FindIterable)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, false)

        when:
        gridFSBucket.openUploadStream('filename')

        then:
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        1 * filesCollection.find() >> findIterable
        1 * findIterable.projection(new Document('_id', 1)) >> findIterable
        1 * findIterable.first() >> null

        then:
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        1 * filesCollection.listIndexes() >> listIndexesIterable
        1 * listIndexesIterable.into(_) >> []

        then:
        1 * filesCollection.createIndex({ index -> index == Document.parse('{"filename": 1, "uploadDate": 1 }') })

        then:
        1 * chunksCollection.withReadPreference(primary()) >> chunksCollection
        1 * chunksCollection.listIndexes() >> listIndexesIterable
        1 * listIndexesIterable.into(_) >> []

        then:
        1 * chunksCollection.createIndex({ index -> index == Document.parse('{"files_id": 1, "n": 1}') },
                { indexOptions -> indexOptions.isUnique() })
    }

    def 'should not create indexes if they already exist'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def listIndexesIterable = Mock(ListIndexesIterable)
        def findIterable = Mock(FindIterable)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, false)

        when:
        gridFSBucket.openUploadStream('filename')

        then:
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        1 * filesCollection.find() >> findIterable
        1 * findIterable.projection(new Document('_id', 1)) >> findIterable
        1 * findIterable.first() >> null

        then:
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        1 * filesCollection.listIndexes() >> listIndexesIterable
        1 * listIndexesIterable.into(_) >> [Document.parse('{"key": {"_id": 1}}'),
                                            Document.parse('{"key": {"filename": 1, "uploadDate": 1 }}')]

        then:
        0 * filesCollection.createIndex(_)

        then:
        1 * chunksCollection.withReadPreference(primary()) >> chunksCollection
        1 * chunksCollection.listIndexes() >> listIndexesIterable
        1 * listIndexesIterable.into(_) >> [Document.parse('{"key": {"_id": 1}}'),
                                            Document.parse('{"key": {"files_id": 1, "n": 1 }}')]

        then:
        0 * chunksCollection.createIndex(_)
    }

    def 'should delete from files collection then chunks collection'() {
        given:
        def fileId = new ObjectId()
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)

        when:
        gridFSBucket.delete(fileId)

        then: 'Delete from the files collection first'
        1 * filesCollection.deleteOne(new Document('_id', new BsonObjectId(fileId))) >> DeleteResult.acknowledged(1)

        then:
        1 * chunksCollection.deleteMany(new Document('files_id', new BsonObjectId(fileId)))
    }

    def 'should throw an exception when deleting if no record in the files collection'() {
        given:
        def fileId = new ObjectId()
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)

        when:
        gridFSBucket.delete(fileId)

        then: 'Delete from the files collection first'
        1 * filesCollection.deleteOne(new Document('_id', new BsonObjectId(fileId))) >> DeleteResult.acknowledged(0)

        then: 'Should still delete any orphan chunks'
        1 * chunksCollection.deleteMany(new Document('files_id', new BsonObjectId(fileId)))

        then:
        thrown(MongoGridFSException)
    }

    def 'should rename a file'() {
        given:
        def fileId = new ObjectId()
        def filesCollection = Mock(MongoCollection)
        def newFilename = 'newFilename'
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, Stub(MongoCollection), true)

        when:
        gridFSBucket.rename(fileId, newFilename)

        then:
        1 * filesCollection.updateOne(new Document('_id', fileId),
                new Document('$set',
                        new Document('filename', newFilename))) >> new UpdateResult.UnacknowledgedUpdateResult()
    }

    def 'should throw an exception renaming non existent file'() {
        given:
        def fileId = new ObjectId()
        def filesCollection = Mock(MongoCollection) {
            1 * updateOne(_, _) >> new UpdateResult.AcknowledgedUpdateResult(0, 0, null)
        }
        def newFilename = 'newFilename'
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, Stub(MongoCollection), true)

        when:
        gridFSBucket.rename(fileId, newFilename)

        then:
        thrown(MongoGridFSException)
    }

    def 'should be able to drop the bucket'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)

        when:
        gridFSBucket.drop()

        then: 'drop the files collection first'
        1 * filesCollection.drop()

        then:
        1 * chunksCollection.drop()
    }
}
