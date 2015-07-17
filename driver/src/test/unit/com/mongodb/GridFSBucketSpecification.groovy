/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License");
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
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.GridFSDownloadByNameOptions
import com.mongodb.client.result.DeleteResult
import com.mongodb.client.result.UpdateResult
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.configuration.CodecRegistry
import org.bson.types.ObjectId
import spock.lang.Specification
import spock.lang.Unroll

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondary
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

class GridFSBucketSpecification extends Specification {

    def 'should behave correctly when using withBucketName'() {
        given:
        def newBucketName = 'filez'
        def database = Stub(MongoDatabase)

        when:
        def gridFSBucket = new GridFSBucketImpl(database).withBucketName(newBucketName)

        then:
        gridFSBucket.getBucketName() == newBucketName
    }

    def 'should behave correctly when using withChunkSizeBytes'() {
        given:
        def newChunkSize = 200
        def database = Stub(MongoDatabase)

        when:
        def gridFSBucket = new GridFSBucketImpl(database).withChunkSizeBytes(newChunkSize)

        then:
        gridFSBucket.getChunkSizeBytes() == newChunkSize
    }

    def 'should behave correctly when using withCodecRegistry'() {
        given:
        def newCodecRegistry = Stub(CodecRegistry)
        def database = Stub(MongoDatabase)

        when:
        def gridFSBucket = new GridFSBucketImpl(database).withCodecRegistry(newCodecRegistry)

        then:
        gridFSBucket.getCodecRegistry() == newCodecRegistry
    }

    def 'should behave correctly when using withReadPreference'() {
        given:
        def newReadPreference = primary()
        def database = Stub(MongoDatabase)

        when:
        def gridFSBucket = new GridFSBucketImpl(database).withReadPreference(newReadPreference)

        then:
        gridFSBucket.getReadPreference() == newReadPreference
    }

    def 'should behave correctly when using withWriteConcern'() {
        given:
        def newWriteConcern = WriteConcern.MAJORITY
        def database = Stub(MongoDatabase)

        when:
        def gridFSBucket = new GridFSBucketImpl(database).withWriteConcern(newWriteConcern)

        then:
        gridFSBucket.getWriteConcern() == newWriteConcern
    }


    def 'should get defaults from MongoDatabase'() {
        given:
        def defaultBucketName = 'fs'
        def defaultChunkSize = 255
        def database = new MongoDatabaseImpl('test', fromProviders(new DocumentCodecProvider()), secondary(), WriteConcern.ACKNOWLEDGED,
                new TestOperationExecutor([]))

        when:
        def gridFSBucket = new GridFSBucketImpl(database)

        then:
        gridFSBucket.getBucketName() == defaultBucketName
        gridFSBucket.getChunkSizeBytes() == defaultChunkSize
        gridFSBucket.getCodecRegistry() == database.getCodecRegistry()
        gridFSBucket.getReadPreference() == database.getReadPreference()
        gridFSBucket.getWriteConcern() == database.getWriteConcern()
    }

    def 'should create the expected GridFSUploadStream'() {
        given:
        def filesCollection = Stub(MongoCollection)
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry),
                Stub(ReadPreference), Stub(WriteConcern), filesCollection, chunksCollection, true)

        when:
        def stream = gridFSBucket.openUploadStream('filename')

        then:
        expect stream, isTheSameAs(new GridFSUploadStreamImpl(filesCollection, chunksCollection, stream.getFileId(), 'filename',
                255, new BsonDocument()), ['md5'])
    }

    def 'should upload from stream'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry),
                Stub(ReadPreference), Stub(WriteConcern), filesCollection, chunksCollection, true)
        def contentBytes = 'content' as byte[]
        def inputStream = new ByteArrayInputStream(contentBytes)

        when:
        gridFSBucket.uploadFromStream('filename', inputStream)

        then:
        1 * chunksCollection.insertOne(_)

        then:
        1 * filesCollection.insertOne(_)
    }


    def 'should create the expected GridFSDownloadStream'() {
        given:
        def fileId = new ObjectId()
        def fileInfo = new BsonDocument('_id', new BsonObjectId(fileId))
                .append('chunkSize', new BsonInt32(255))
                .append('length', new BsonInt64(10))
        def filesCollection = Mock(MongoCollection) {
            1 * find(_) >> Mock(FindIterable) {
                1 * first() >> fileInfo
            }
        }
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry),
                Stub(ReadPreference), Stub(WriteConcern), filesCollection, chunksCollection, true)

        when:
        def stream = gridFSBucket.openDownloadStream(fileId)

        then:
        expect stream, isTheSameAs(new GridFSDownloadStreamImpl(fileInfo, chunksCollection))
    }

    def 'should download to stream'() {
        given:
        def fileId = new ObjectId()
        def filename = 'filename'
        def fileInfo = new BsonDocument('_id', new BsonObjectId(fileId))
                .append('filename', new BsonString(filename))
                .append('chunkSize', new BsonInt32(255))
                .append('length', new BsonInt64(10))
        def tenBytes = new byte[10]
        def chunkDocument = new BsonDocument('files_id', fileInfo.getObjectId('_id'))
                .append('n', new BsonInt32(0))
                .append('data', new BsonBinary(tenBytes))
        def findIterable = Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry),
                Stub(ReadPreference), Stub(WriteConcern), filesCollection, chunksCollection, true)
        def outputStream = new ByteArrayOutputStream(10)

        when:
        gridFSBucket.downloadToStream(fileId, outputStream)
        outputStream.close()

        then:
        1 * filesCollection.find(new BsonDocument('_id', new BsonObjectId(fileId))) >> findIterable
        1 * findIterable.first() >> fileInfo
        1 * chunksCollection.find(_) >> findIterable
        1 * findIterable.first() >> chunkDocument

        then:
        outputStream.toByteArray() == tenBytes

    }

    def 'should throw an exception if file not found'() {
        given:
        def fileId = new ObjectId()
        def filesCollection = Mock(MongoCollection) {
            1 * find(_) >> Mock(FindIterable)
        }
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry),
                Stub(ReadPreference), Stub(WriteConcern), filesCollection, chunksCollection, true)

        when:
        gridFSBucket.openDownloadStream(fileId)

        then:
        thrown(MongoGridFSException)
    }

    @Unroll
    def 'should create the expected GridFSDownloadStream when opening by name with version: #version'() {
        given:
        def filename = 'filename'
        def fileInfo = new BsonDocument('_id', new BsonObjectId(new ObjectId()))
                .append('filename', new BsonString(filename))
                .append('chunkSize', new BsonInt32(255))
                .append('length', new BsonInt64(10))
        def findIterable = Mock(FindIterable)
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry),
                Stub(ReadPreference), Stub(WriteConcern), filesCollection, chunksCollection, true)

        when:
        def stream = gridFSBucket.openDownloadStreamByName(filename, new GridFSDownloadByNameOptions().setRevision(version))

        then:
        1 * filesCollection.find(new BsonDocument('filename', new BsonString(filename))) >> findIterable
        1 * findIterable.skip(skip) >> findIterable
        1 * findIterable.sort(new BsonDocument('uploadDate', sortOrder)) >> findIterable
        1 * findIterable.first() >> fileInfo

        then:
        expect stream, isTheSameAs(new GridFSDownloadStreamImpl(fileInfo, chunksCollection))

        where:
        version | skip | sortOrder
          0     |  0   | new BsonInt32(1)
          1     |  1   | new BsonInt32(1)
          2     |  2   | new BsonInt32(1)
          3     |  3   | new BsonInt32(1)
          -1    |  0   | new BsonInt32(-1)
          -2    |  1   | new BsonInt32(-1)
          -3    |  2   | new BsonInt32(-1)
    }

    def 'should create the expected GridFSFindIterable'() {
        given:
        def database = Mock(MongoDatabase)
        def collection = Mock(MongoCollection)
        def findIterable = Mock(FindIterable)
        def filter = new BsonDocument('filename', new BsonString('filename'))
        def gridFSBucket = new GridFSBucketImpl(database, 'fs', 255, Stub(CodecRegistry),
                Stub(ReadPreference), Stub(WriteConcern), Stub(MongoCollection), Stub(MongoCollection), true)

        when:
        def result = gridFSBucket.find()

        then:
        1 * database.getCollection('fs.files', Document) >> collection
        1 * collection.withCodecRegistry(_) >> collection
        1 * collection.withReadPreference(_) >> collection
        1 * collection.withWriteConcern(_) >> collection
        1 * collection.find() >> findIterable
        expect result, isTheSameAs(new GridFSFindIterableImpl(findIterable))

        when:
        result = gridFSBucket.find(BsonDocument)

        then:
        1 * database.getCollection('fs.files', BsonDocument) >> collection
        1 * collection.withCodecRegistry(_) >> collection
        1 * collection.withReadPreference(_) >> collection
        1 * collection.withWriteConcern(_) >> collection
        1 * collection.find() >> findIterable
        expect result, isTheSameAs(new GridFSFindIterableImpl(findIterable))

        when:
        result = gridFSBucket.find(filter)

        then:
        1 * database.getCollection('fs.files', Document) >> collection
        1 * collection.withCodecRegistry(_) >> collection
        1 * collection.withReadPreference(_) >> collection
        1 * collection.withWriteConcern(_) >> collection
        1 * collection.find() >> findIterable
        1 * findIterable.filter(filter) >> findIterable
        expect result, isTheSameAs(new GridFSFindIterableImpl(findIterable))

        when:
        result = gridFSBucket.find(filter, BsonDocument)

        then:
        1 * database.getCollection('fs.files', BsonDocument) >> collection
        1 * collection.withCodecRegistry(_) >> collection
        1 * collection.withReadPreference(_) >> collection
        1 * collection.withWriteConcern(_) >> collection
        1 * collection.find() >> findIterable
        1 * findIterable.filter(filter) >> findIterable
        expect result, isTheSameAs(new GridFSFindIterableImpl(findIterable))
    }

    def 'should throw an exception if file not found when opening by name'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def findIterable = Mock(FindIterable)
        def chunksCollection = Stub(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry),
                Stub(ReadPreference), Stub(WriteConcern), filesCollection, chunksCollection, true)

        when:
        gridFSBucket.openDownloadStreamByName('filename')

        then:
        1 * filesCollection.find(_) >> findIterable
        1 * findIterable.skip(_) >> findIterable
        1 * findIterable.sort(_) >> findIterable
        1 * findIterable.first() >> null

        then:
        thrown(MongoGridFSException)
    }

    def 'should create indexes on write'() {
        given:
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def findIterable = Mock(FindIterable)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry),
                Stub(ReadPreference), Stub(WriteConcern), filesCollection, chunksCollection, false)

        when:
        gridFSBucket.openUploadStream('filename')

        then:
        1 * filesCollection.withReadPreference(_) >> filesCollection
        1 * filesCollection.find() >> findIterable
        1 * findIterable.projection(new BsonDocument('_id', new BsonInt32(1))) >> findIterable
        1 * findIterable.first() >> null

        then:
        1 * filesCollection.createIndex(_)

        then:
        1 * chunksCollection.createIndex(_, _)
    }

    def 'should delete from files collection then chunks collection'() {
        given:
        def fileId = new ObjectId()
        def filesCollection = Mock(MongoCollection)
        def chunksCollection = Mock(MongoCollection)
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry),
                Stub(ReadPreference), Stub(WriteConcern), filesCollection, chunksCollection, true)

        when:
        gridFSBucket.delete(fileId)

        then: 'Delete from the files collection first'
        1 * filesCollection.deleteOne(new BsonDocument('_id', new BsonObjectId(fileId))) >> DeleteResult.acknowledged(1)

        then:
        1 * chunksCollection.deleteMany(new BsonDocument('files_id', new BsonObjectId(fileId)))
    }

    def 'should rename a file'() {
        given:
        def fileId = new ObjectId()
        def filesCollection = Mock(MongoCollection)
        def newFilename = 'newFilename'
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry),
                Stub(ReadPreference), Stub(WriteConcern), filesCollection, Stub(MongoCollection), true)

        when:
        gridFSBucket.rename(fileId, newFilename)

        then:
        1 * filesCollection.updateOne(new BsonDocument('_id', new BsonObjectId(fileId)),
                new BsonDocument('$set',
                        new BsonDocument('filename', new BsonString(newFilename)))) >> new UpdateResult.UnacknowledgedUpdateResult()
    }

    def 'should throw an exception renaming non existent file'() {
        given:
        def fileId = new ObjectId()
        def filesCollection = Mock(MongoCollection) {
            1 * updateOne(_, _) >> new UpdateResult.AcknowledgedUpdateResult(0, 0, null)
        }
        def newFilename = 'newFilename'
        def gridFSBucket = new GridFSBucketImpl(Stub(MongoDatabase), 'fs', 255, Stub(CodecRegistry),
                Stub(ReadPreference), Stub(WriteConcern), filesCollection, Stub(MongoCollection), true)

        when:
        gridFSBucket.rename(fileId, newFilename)

        then:
        thrown(MongoGridFSException)
    }
}
