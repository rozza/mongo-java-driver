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
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.async.AsyncBatchCursor
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.async.client.FindIterable
import com.mongodb.async.client.ListIndexesIterable
import com.mongodb.async.client.MongoCollection
import com.mongodb.async.client.MongoDatabase
import com.mongodb.async.client.MongoDatabaseImpl
import com.mongodb.async.client.TestOperationExecutor
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

import java.nio.ByteBuffer

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.async.client.gridfs.InputOutputStreamHelper.toAsyncInputStream
import static com.mongodb.async.client.gridfs.InputOutputStreamHelper.toAsyncOutputStream
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
        def defaultChunkSizeBytes = 255 * 1024
        def database = new MongoDatabaseImpl('test', fromProviders(new DocumentCodecProvider()), secondary(), WriteConcern.ACKNOWLEDGED,
                readConcern, new TestOperationExecutor([]))

        when:
        def gridFSBucket = new GridFSBucketImpl(database)

        then:
        gridFSBucket.getChunkSizeBytes() == defaultChunkSizeBytes
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
                255, null), ['md5', 'closeAndWritingLock'])
    }

    def 'should upload from stream'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)
        def contentBytes = 'content' as byte[]
        def inputStream = toAsyncInputStream(new ByteArrayInputStream(contentBytes))

        when:
        gridFSBucket.uploadFromStream('filename', inputStream, Stub(SingleResultCallback))

        then:
        1 * chunksCollection.insertOne(_, _) >> { it[1].onResult(null, null) }
        1 * filesCollection.insertOne(_, _)
    }

    def 'should clean up any chunks when upload from stream throws an IOException'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)
        def inputStream = Mock(AsyncInputStream) {
            2 * read(_, _) >> { it[0].put(new byte[255]); it[1].onResult(255, null) } >> {
                it[1].onResult(null, new IOException('stream failure'))
            }
        }
        def futureResult = new FutureResultCallback()

        when:
        gridFSBucket.uploadFromStream('filename', inputStream, futureResult)

        then:
        1 * chunksCollection.insertOne(_, _) >> { it[1].onResult(null, null) }
        1 * chunksCollection.deleteMany(_, _) >> { it[1].onResult(null, null) }

        then:
        0 * filesCollection.insertOne(_, _)

        when:
        futureResult.get()

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
        def inputStream = Mock(AsyncInputStream) {
            2 * read(_, _) >> { it[0].put(new byte[255]); it[1].onResult(255, null) } >> {
                it[1].onResult(null, alternativeException)
            }
        }
        def futureResult = new FutureResultCallback()

        when:
        gridFSBucket.uploadFromStream('filename', inputStream, futureResult)

        then:
        1 * chunksCollection.insertOne(_, _) >> { it[1].onResult(null, null) }

        then:
        0 * chunksCollection.deleteMany(_, _)

        then:
        0 * filesCollection.insertOne(_)

        when:
        futureResult.get()

        then:
        def exception = thrown(MongoGridFSException)
        exception == alternativeException
    }

    def 'should create the expected GridFSDownloadStream'() {
        given:
        def fileId = new BsonObjectId(new ObjectId())
        def findIterable = Mock(FindIterable)
        def gridFSFindIterable = new GridFSFindIterableImpl(findIterable)
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)

        when:
        def stream = gridFSBucket.openDownloadStream(fileId.getValue())

        then:
        1 * filesCollection.find() >> findIterable
        1 * findIterable.filter(_) >> findIterable

        then:
        expect stream, isTheSameAs(new GridFSDownloadStreamImpl(gridFSFindIterable, chunksCollection),
                ['closeAndReadingLock', 'resultsQueue'])
    }

    def 'should download to stream'() {
        given:
        def fileId = new ObjectId()
        def bsonFileId = new BsonObjectId(fileId)
        def fileInfo = new GridFSFile(bsonFileId, 'filename', 10, 255, new Date(), '1234', new Document())
        def batchCursor = Mock(AsyncBatchCursor)
        def filesFindIterable = Mock(FindIterable)
        def chunksFindIterable = Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def sizeOfStream = 10
        def tenBytes = new byte[sizeOfStream]
        def chunkDocument = new Document('files_id', fileInfo.getId()).append('n', 0).append('data', new Binary(tenBytes))
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)
        def outputStream = new ByteArrayOutputStream(1024)
        def asyncOutputStream = toAsyncOutputStream(outputStream)

        when:
        def futureResult = new FutureResultCallback()
        gridFSBucket.downloadToStream(fileId, asyncOutputStream, futureResult)
        asyncOutputStream.close(Stub(SingleResultCallback))
        def size = futureResult.get()

        then:
        1 * filesCollection.find() >> filesFindIterable
        1 * filesFindIterable.filter(new Document('_id', fileId)) >> filesFindIterable
        1 * filesFindIterable.map(_) >> filesFindIterable

        then:
        1 * filesFindIterable.first(_) >> { it[0].onResult(fileInfo, null) }
        1 * chunksCollection.find(_) >> chunksFindIterable
        1 * chunksFindIterable.sort(_) >> chunksFindIterable
        1 * chunksFindIterable.batchSize(_) >> chunksFindIterable
        1 * chunksFindIterable.batchCursor(_) >> { it[0].onResult(batchCursor, null) }
        1 * batchCursor.next(_) >> { it[0].onResult([chunkDocument], null) }

        then:
        size == sizeOfStream
        outputStream.toByteArray() == tenBytes
    }

    @Unroll
    def 'should download to stream using #description'() {
        given:
        def bsonFileId = fileId instanceof ObjectId ? new BsonObjectId(fileId) : fileId
        def fileInfo = new GridFSFile(bsonFileId, 'filename', 10L, 255, new Date(), '1234', new Document())
        def batchCursor = Mock(AsyncBatchCursor)
        def filesFindIterable = Mock(FindIterable)
        def chunksFindIterable = Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def sizeOfStream = 10
        def tenBytes = new byte[sizeOfStream]
        def chunkDocument = new Document('files_id', fileId).append('n', 0).append('data', new Binary(tenBytes))
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)
        def outputStream = new ByteArrayOutputStream(1024)
        def asyncOutputStream = toAsyncOutputStream(outputStream)

        when:
        def futureResult = new FutureResultCallback()
        gridFSBucket.downloadToStream(fileId, asyncOutputStream, futureResult)
        asyncOutputStream.close(Stub(SingleResultCallback))
        def size = futureResult.get()

        then:
        1 * filesCollection.find() >> filesFindIterable
        1 * filesFindIterable.filter(new Document('_id', fileId)) >> filesFindIterable
        1 * filesFindIterable.map(_) >> filesFindIterable

        then:
        1 * filesFindIterable.first(_) >> { it[0].onResult(fileInfo, null) }
        1 * chunksCollection.find(_) >> chunksFindIterable
        1 * chunksFindIterable.sort(_) >> chunksFindIterable
        1 * chunksFindIterable.batchSize(_) >> chunksFindIterable
        1 * chunksFindIterable.batchCursor(_) >> { it[0].onResult(batchCursor, null) }
        1 * batchCursor.next(_) >> { it[0].onResult([chunkDocument], null) }

        then:
        size == sizeOfStream
        outputStream.toByteArray() == tenBytes

        where:
        description       | fileId
        'using objectId'  | new ObjectId()
        'using bsonValue' | new BsonString('1')
    }

    def 'should download to stream by name'() {
        given:
        def filename = 'filename'
        def fileId = new ObjectId()
        def fileInfo = new GridFSFile(new BsonObjectId(fileId), filename, 10L, 255, new Date(), '1234', new Document())
        def batchCursor = Mock(AsyncBatchCursor)
        def filesFindIterable = Mock(FindIterable)
        def chunksFindIterable = Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def sizeOfStream = 10
        def tenBytes = new byte[sizeOfStream]
        def chunkDocument = new Document('files_id', fileId).append('n', 0).append('data', new Binary(tenBytes))
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)
        def outputStream = new ByteArrayOutputStream(1024)
        def asyncOutputStream = toAsyncOutputStream(outputStream)

        when:
        def futureResult = new FutureResultCallback()
        gridFSBucket.downloadToStreamByName(filename, asyncOutputStream, futureResult)
        asyncOutputStream.close(Stub(SingleResultCallback))
        def size = futureResult.get()

        then:
        1 * filesCollection.find(new Document('filename', filename)) >> filesFindIterable
        1 * filesFindIterable.sort(_) >> filesFindIterable
        1 * filesFindIterable.skip(_) >> filesFindIterable
        1 * filesFindIterable.map(_) >> filesFindIterable

        then:
        1 * filesFindIterable.first(_) >> { it[0].onResult(fileInfo, null) }
        1 * chunksCollection.find(_) >> chunksFindIterable
        1 * chunksFindIterable.sort(_) >> chunksFindIterable
        1 * chunksFindIterable.batchSize(_) >> chunksFindIterable
        1 * chunksFindIterable.batchCursor(_) >> { it[0].onResult(batchCursor, null) }
        1 * batchCursor.next(_) >> { it[0].onResult([chunkDocument], null) }

        then:
        size == sizeOfStream
        outputStream.toByteArray() == tenBytes
    }

    def 'should throw an exception if file not found'() {
        given:
        def fileId = new ObjectId()
        def findIterable = Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)

        when:
        def futureResult = new FutureResultCallback()
        def stream = gridFSBucket.openDownloadStream(fileId)
        stream.read(ByteBuffer.wrap(new byte[10]), futureResult)
        futureResult.get()

        then:
        1 * filesCollection.find() >> findIterable
        1 * findIterable.filter(new Document('_id', fileId)) >> findIterable
        1 * findIterable.map(_) >> findIterable
        1 * findIterable.first(_) >> { it[0].onResult(null, null) }
        thrown(MongoGridFSException)
    }

    @Unroll
    def 'should create the expected GridFSDownloadStream when opening by name with version: #version'() {
        given:
        def filename = 'filename'
        def fileId = new ObjectId()
        def bsonFileId = new BsonObjectId(fileId)
        def fileInfo = new GridFSFile(bsonFileId, filename, 10, 255, new Date(), '1234', new Document())
        def findIterable = Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)

        when:
        def futureResult = new FutureResultCallback()
        def stream = gridFSBucket.openDownloadStreamByName(filename, new GridFSDownloadByNameOptions().revision(version))
        stream.getGridFSFile(futureResult)
        futureResult.get()

        then:
        1 * filesCollection.find(new Document('filename', filename)) >> findIterable
        1 * findIterable.skip(skip) >> findIterable
        1 * findIterable.sort(new Document('uploadDate', sortOrder)) >> findIterable
        1 * findIterable.map(_) >> findIterable
        1 * findIterable.first(_) >> { it[0].onResult(fileInfo, null) }

        where:
        version | skip | sortOrder
        0       | 0    | 1
        1       | 1    | 1
        2       | 2    | 1
        3       | 3    | 1
        -1      | 0    | -1
        -2      | 1    | -1
        -3      | 2    | -1
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
        1 * collection.find(filter) >> findIterable
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
        def futureResult = new FutureResultCallback()
        def stream = gridFSBucket.openDownloadStreamByName('filename')
        stream.read(ByteBuffer.wrap(new byte[10]), futureResult)
        futureResult.get()

        then:
        1 * filesCollection.find(new Document('filename', 'filename')) >> findIterable
        1 * findIterable.skip(0) >> findIterable
        1 * findIterable.sort(new Document('uploadDate', -1)) >> findIterable
        1 * findIterable.map(_) >> findIterable
        1 * findIterable.first(_) >> { it[0].onResult(null, null) }

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
        1 * findIterable.first(_) >> { it[0].onResult(null, null) }

        // Files Index check
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        1 * filesCollection.listIndexes() >> listIndexesIterable
        1 * listIndexesIterable.into(_, _) >> { it[1].onResult([], null) }
        1 * filesCollection.createIndex({ index -> index == Document.parse('{"filename": 1, "uploadDate": 1 }') }, _) >> {
            it[1].onResult('filename_1', null)
        }

        // Chunks Index check
        1 * chunksCollection.withReadPreference(primary()) >> chunksCollection
        1 * chunksCollection.listIndexes() >> listIndexesIterable
        1 * listIndexesIterable.into(_, _) >> { it[1].onResult([], null) }
        1 * chunksCollection.createIndex({ index -> index == Document.parse('{"files_id": 1, "n": 1}') },
                { indexOptions -> indexOptions.isUnique() }, _) >> { it[2].onResult('files_id_1', null) }
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
        1 * findIterable.first(_) >> { it[0].onResult(null, null) }

        // Files Index check
        1 * filesCollection.withReadPreference(primary()) >> filesCollection
        1 * filesCollection.listIndexes() >> listIndexesIterable
        1 * listIndexesIterable.into(_, _) >> {
            it[1].onResult([Document.parse('{"key": {"_id": 1}}'),
                            Document.parse('{"key": {"filename": 1, "uploadDate": 1 }}')], null)
        }
        0 * filesCollection.createIndex(_, _)

        // Chunks Index check
        1 * chunksCollection.withReadPreference(primary()) >> chunksCollection
        1 * chunksCollection.listIndexes() >> listIndexesIterable
        1 * listIndexesIterable.into(_, _) >> {
            it[1].onResult([Document.parse('{"key": {"_id": 1}}'),
                            Document.parse('{"key": {"files_id": 1, "n": 1 }}')], null)
        }

        // Create chunks Index
        0 * chunksCollection.createIndex(_, _)
    }

    def 'should delete from files collection then chunks collection'() {
        given:
        def fileId = new ObjectId()
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)

        when:
        def futureResult = new FutureResultCallback()
        gridFSBucket.delete(fileId, futureResult)
        futureResult.get()

        then:
        1 * filesCollection.deleteOne(new Document('_id', new BsonObjectId(fileId)), _) >> {
            it[1].onResult(DeleteResult.acknowledged(1), null)
        }
        1 * chunksCollection.deleteMany(new Document('files_id', new BsonObjectId(fileId)), _) >> {
            it[1].onResult(DeleteResult.acknowledged(1), null)
        }
    }

    def 'should throw an exception when deleting if no record in the files collection'() {
        given:
        def fileId = new ObjectId()
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, chunksCollection, true)

        when:
        def futureResult = new FutureResultCallback()
        gridFSBucket.delete(fileId, futureResult)
        futureResult.get()

        then:
        1 * filesCollection.deleteOne(new Document('_id', new BsonObjectId(fileId)), _) >> {
            it[1].onResult(DeleteResult.acknowledged(0), null)
        }
        1 * chunksCollection.deleteMany(new Document('files_id', new BsonObjectId(fileId)), _) >> {
            it[1].onResult(DeleteResult.acknowledged(1), null)
        }

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
        def futureResult = new FutureResultCallback()
        gridFSBucket.rename(fileId, newFilename, futureResult)
        futureResult.get()

        then:
        1 * filesCollection.updateOne(new Document('_id', fileId),
                new Document('$set',
                        new Document('filename', newFilename)), _) >> {
            it[2].onResult(new UpdateResult.UnacknowledgedUpdateResult(), null)
        }
    }

    def 'should throw an exception renaming non existent file'() {
        given:
        def fileId = new ObjectId()
        def filesCollection = Mock(MongoCollection)
        def newFilename = 'newFilename'
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry), Stub(ReadPreference),
                Stub(WriteConcern), readConcern, filesCollection, Stub(MongoCollection), true)

        when:
        def futureResult = new FutureResultCallback()
        gridFSBucket.rename(fileId, newFilename, futureResult)
        futureResult.get()

        then:
        1 * filesCollection.updateOne(_, _, _) >> { it[2].onResult(new UpdateResult.AcknowledgedUpdateResult(0, 0, null), null) }

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
        def futureResult = new FutureResultCallback()
        gridFSBucket.drop(futureResult)
        futureResult.get()

        then:
        1 * filesCollection.drop(_) >> { it[0].onResult(null, null) }
        1 * chunksCollection.drop(_) >> { it[0].onResult(null, null) }
    }
}
