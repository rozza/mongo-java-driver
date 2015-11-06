/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.gridfs

import com.mongodb.FunctionalSpecification
import com.mongodb.MongoGridFSException
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.gridfs.model.GridFSDownloadByNameOptions
import com.mongodb.client.gridfs.model.GridFSUploadOptions
import org.bson.Document
import org.bson.types.ObjectId
import spock.lang.Unroll

import java.security.MessageDigest

import static com.mongodb.Fixture.getDefaultDatabaseName
import static com.mongodb.Fixture.getMongoClient

class GridFSBucketSmokeTestSpecification extends FunctionalSpecification {
    protected MongoDatabase mongoDatabase;
    protected MongoCollection<Document> filesCollection;
    protected MongoCollection<Document> chunksCollection;
    protected GridFSBucket gridFSBucket;
    def singleChunkString = 'GridFS'
    def multiChunkString = singleChunkString.padLeft(1024 * 255 * 5)

    def setup() {
        mongoDatabase = getMongoClient().getDatabase(getDefaultDatabaseName())
        filesCollection = mongoDatabase.getCollection('fs.files')
        chunksCollection = mongoDatabase.getCollection('fs.chunks')
        filesCollection.drop()
        chunksCollection.drop()
        gridFSBucket = new GridFSBucketImpl(mongoDatabase)
    }

    def cleanup() {
        if (filesCollection != null) {
            filesCollection.drop()
            chunksCollection.drop()
        }
    }

    @Unroll
    def 'should round trip a #description'() {
        given:
        def content = multiChunk ? multiChunkString : singleChunkString
        def contentBytes = content as byte[]
        def expectedLength = contentBytes.length as Long
        def expectedMD5 = MessageDigest.getInstance('MD5').digest(contentBytes).encodeHex().toString()
        ObjectId fileId
        byte[] gridFSContentBytes

        when:
        if (direct) {
            fileId = gridFSBucket.uploadFromStream('myFile', new ByteArrayInputStream(contentBytes));
        } else {
            def outputStream = gridFSBucket.openUploadStream('myFile')
            outputStream.write(contentBytes)
            outputStream.close()
            fileId = outputStream.getFileId()
        }

        then:
        filesCollection.count() == 1
        chunksCollection.count() == chunkCount

        when:
        def file = filesCollection.find().first()

        then:
        file.getObjectId('_id') == fileId
        file.getInteger('chunkSize') == gridFSBucket.getChunkSizeBytes()
        file.getLong('length') == expectedLength
        file.getString('md5') == expectedMD5
        !file.containsKey('metadata')

        when:
        if (direct) {
            gridFSContentBytes = gridFSBucket.openDownloadStream(fileId).getBytes()
        } else {
            def outputStream = new ByteArrayOutputStream(expectedLength as int)
            gridFSBucket.downloadToStream(fileId, outputStream)
            outputStream.close()
            gridFSContentBytes = outputStream.toByteArray()
        }

        then:
        gridFSContentBytes == contentBytes

        where:
        description              | multiChunk | chunkCount | direct
        'a small file directly'  | false      | 1          | true
        'a small file to stream' | false      | 1          | false
        'a large file directly'  | true       | 5          | true
        'a large file to stream' | true       | 5          | false
    }

    def 'should round trip with a batchSize of 1'() {
        given:
        def content = multiChunkString
        def contentBytes = content as byte[]
        def expectedLength = contentBytes.length as Long
        def expectedMD5 = MessageDigest.getInstance('MD5').digest(contentBytes).encodeHex().toString()
        ObjectId fileId
        byte[] gridFSContentBytes

        when:
        fileId = gridFSBucket.uploadFromStream('myFile', new ByteArrayInputStream(contentBytes));

        then:
        filesCollection.count() == 1
        chunksCollection.count() == 5

        when:
        def file = filesCollection.find().first()

        then:
        file.getObjectId('_id') == fileId
        file.getInteger('chunkSize') == gridFSBucket.getChunkSizeBytes()
        file.getLong('length') == expectedLength
        file.getString('md5') == expectedMD5
        !file.containsKey('metadata')

        when:
        gridFSContentBytes = gridFSBucket.openDownloadStream(fileId).batchSize(1).getBytes()

        then:
        gridFSContentBytes == contentBytes
    }

    def 'should use custom uploadOptions when uploading' () {
        given:
        def chunkSize = 20
        def metadata = new Document('archived', false)
        def options = new GridFSUploadOptions()
                .chunkSizeBytes(chunkSize)
                .metadata(metadata)
        def content = 'qwerty' * 1024
        def contentBytes = content as byte[]
        def expectedLength = contentBytes.length as Long
        def expectedNoChunks = Math.ceil((expectedLength as double) / chunkSize) as int
        def expectedMD5 = MessageDigest.getInstance('MD5').digest(contentBytes).encodeHex().toString()
        ObjectId fileId
        byte[] gridFSContentBytes

        when:
        if (direct) {
            fileId = gridFSBucket.uploadFromStream('myFile', new ByteArrayInputStream(contentBytes), options);
        } else {
            def outputStream = gridFSBucket.openUploadStream('myFile', options)
            outputStream.write(contentBytes)
            outputStream.close()
            fileId = outputStream.getFileId()
        }

        then:
        filesCollection.count() == 1
        chunksCollection.count() == expectedNoChunks

        when:
        def file = filesCollection.find().first()

        then:
        file.getObjectId('_id') == fileId
        file.get('metadata', Document) == metadata
        file.getInteger('chunkSize') == chunkSize
        file.getLong('length') == expectedLength
        file.getString('md5') == expectedMD5

        when:
        if (direct) {
            gridFSContentBytes = gridFSBucket.openDownloadStream(fileId).getBytes()
        } else {
            def outputStream = new ByteArrayOutputStream(expectedLength as int)
            gridFSBucket.downloadToStream(fileId, outputStream)
            outputStream.close()
            gridFSContentBytes = outputStream.toByteArray()
        }

        then:
        gridFSContentBytes == contentBytes

        where:
        direct << [true, false]
    }


    def 'should be able to open by name'() {
        given:
        def content = 'Hello GridFS'
        def contentBytes = content as byte[]
        def filename = 'myFile'
        gridFSBucket.uploadFromStream(filename, new ByteArrayInputStream(contentBytes))
        byte[] gridFSContentBytes

        when: 'Direct to a stream'
        gridFSContentBytes = gridFSBucket.openDownloadStreamByName(filename).getBytes()

        then:
        gridFSContentBytes == contentBytes

        when: 'To supplied stream'
        def outputStream = new ByteArrayOutputStream(contentBytes.length)
        gridFSBucket.downloadToStreamByName(filename, outputStream)
        outputStream.close()
        gridFSContentBytes = outputStream.toByteArray()

        then:
        gridFSContentBytes == contentBytes
    }

    @Unroll
    def 'should be able to open by name with selected version: #version'() {
        given:
        def contentBytes = (0..3).collect({ "Hello GridFS - ${it}" as byte[] }) as List
        def filename = 'myFile'
        byte[] gridFSContentBytes
        contentBytes.each{
            gridFSBucket.uploadFromStream(filename, new ByteArrayInputStream(it))
        }
        def expectedContentBytes = contentBytes[version]
        def options = new GridFSDownloadByNameOptions().revision(version)

        when: 'Direct to a stream'
        gridFSContentBytes = gridFSBucket.openDownloadStreamByName(filename, options).getBytes()

        then:
        gridFSContentBytes == expectedContentBytes

        when: 'To supplied stream'
        def outputStream = new ByteArrayOutputStream(expectedContentBytes.length)
        gridFSBucket.downloadToStreamByName(filename, outputStream, options)
        outputStream.close()
        gridFSContentBytes = outputStream.toByteArray()

        then:
        gridFSContentBytes == expectedContentBytes

        where:
        version << [0, 1, 2, 3, -1, -2, -3, -4]
    }

    def 'should throw an exception if cannot open by name'() {
        given:
        def filename = 'FileDoesNotExist'

        when: 'Direct to a stream'
        gridFSBucket.openDownloadStreamByName(filename)

        then:
        thrown(MongoGridFSException)

        when: 'To supplied stream'
        gridFSBucket.downloadToStreamByName(filename, new ByteArrayOutputStream(1024))

        then:
        thrown(MongoGridFSException)
    }

    def 'should throw an exception if cannot open by name with selected version'() {
        given:
        def filename = 'myFile'
        def options = new GridFSDownloadByNameOptions().revision(1)
        gridFSBucket.uploadFromStream(filename, new ByteArrayInputStream('Hello GridFS' as byte[]))

        when: 'Direct to a stream'
        gridFSBucket.openDownloadStreamByName(filename, options)

        then:
        thrown(MongoGridFSException)

        when: 'To supplied stream'
        gridFSBucket.downloadToStreamByName(filename, new ByteArrayOutputStream(1024), options)

        then:
        thrown(MongoGridFSException)
    }

    def 'should delete a file'() {
        given:
        def filename = 'myFile'

        when:
        def fileId = gridFSBucket.uploadFromStream(filename, new ByteArrayInputStream('Hello GridFS' as byte[]))

        then:
        filesCollection.count() == 1
        chunksCollection.count() == 1

        when:
        gridFSBucket.delete(fileId)

        then:
        filesCollection.count() == 0
        chunksCollection.count() == 0
    }

    def 'should thrown when deleting nonexistent file'() {
        when:
        gridFSBucket.delete(new ObjectId())

        then:
        thrown(MongoGridFSException)
    }

    def 'should delete a file data orphan chunks'() {
        def filename = 'myFile'
        def fileId = gridFSBucket.uploadFromStream(filename, new ByteArrayInputStream('Hello GridFS' as byte[]))

        when:
        filesCollection.drop()

        then:
        filesCollection.count() == 0
        chunksCollection.count() == 1

        when:
        gridFSBucket.delete(fileId)

        then:
        thrown(MongoGridFSException)

        then:
        filesCollection.count() == 0
        chunksCollection.count() == 0
    }

    def 'should rename a file'() {
        given:
        def filename = 'myFile'
        def newFileName = 'newFileName'

        when:
        def fileId = gridFSBucket.uploadFromStream(filename, new ByteArrayInputStream('Hello GridFS' as byte[]))

        then:
        filesCollection.count() == 1
        chunksCollection.count() == 1

        when:
        gridFSBucket.rename(fileId, 'newFileName')

        then:
        filesCollection.count() == 1
        chunksCollection.count() == 1

        when:
        gridFSBucket.openDownloadStreamByName(newFileName)

        then:
        notThrown(MongoGridFSException)
    }

    def 'should thrown an exception when rename a nonexistent file'() {
        when:
        gridFSBucket.rename(new ObjectId(), 'newFileName')

        then:
        thrown(MongoGridFSException)
    }

    def 'should only create indexes on first write'() {
        when:
        def contentBytes = 'Hello GridFS' as byte[]

        then:
        filesCollection.listIndexes().into([]).size() == 0
        chunksCollection.listIndexes().into([]).size() == 0

        when:
        if (direct) {
            gridFSBucket.uploadFromStream('myFile', new ByteArrayInputStream(contentBytes));
        } else {
            def outputStream = gridFSBucket.openUploadStream('myFile')
            outputStream.write(contentBytes)
            outputStream.close()
        }

        then:
        filesCollection.listIndexes().into([]).size() == 2
        chunksCollection.listIndexes().into([]).size() == 2

        where:
        direct << [true, false]
    }

    def 'should not create indexes if the files collection is not empty'() {
        when:
        filesCollection.insertOne(new Document('filename', 'bad file'))
        def contentBytes = 'Hello GridFS' as byte[]

        then:
        filesCollection.listIndexes().into([]).size() == 1
        chunksCollection.listIndexes().into([]).size() == 0

        when:
        if (direct) {
            gridFSBucket.uploadFromStream('myFile', new ByteArrayInputStream(contentBytes));
        } else {
            def outputStream = gridFSBucket.openUploadStream('myFile')
            outputStream.write(contentBytes)
            outputStream.close()
        }

        then:
        filesCollection.listIndexes().into([]).size() == 1
        chunksCollection.listIndexes().into([]).size() == 1

        where:
        direct << [true, false]
    }

    def 'should mark and reset'() {
        given:
        def content = 1 .. 1000 as byte[]
        def readByte = new byte[500]

        when:
        def fileId = gridFSBucket.uploadFromStream('myFile', new ByteArrayInputStream(content),
                new GridFSUploadOptions().chunkSizeBytes(500));

        then:
        filesCollection.count() == 1
        chunksCollection.count() == 2

        when:
        def gridFSDownloadStream = gridFSBucket.openDownloadStream(fileId)
        gridFSDownloadStream.read(readByte)

        then:
        readByte == 1 .. 500 as byte[]

        when:
        gridFSDownloadStream.mark()

        then:
        gridFSDownloadStream.read(readByte)

        then:
        readByte == 501 .. 1000 as byte[]

        when:
        gridFSDownloadStream.reset()

        then:
        gridFSDownloadStream.read(readByte)

        then:
        readByte == 501 .. 1000 as byte[]
    }

    def 'should drop the bucket'() {
        given:
        gridFSBucket.uploadFromStream('fileName', new ByteArrayInputStream('Hello GridFS' as byte[]))

        when:
        gridFSBucket.drop()

        then:
        def collectionNames = mongoDatabase.listCollectionNames().into([])
        !collectionNames.contains(filesCollection.getNamespace().collectionName)
        !collectionNames.contains(chunksCollection.getNamespace().collectionName)
    }
}
