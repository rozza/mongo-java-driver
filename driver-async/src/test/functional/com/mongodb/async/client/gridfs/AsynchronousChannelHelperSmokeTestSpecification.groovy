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
import spock.lang.IgnoreIf

import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption

import static GridFSTestHelper.run
import static GridFSTestHelper.TestAsynchronousByteChannel
import static com.mongodb.async.client.Fixture.getDefaultDatabaseName
import static com.mongodb.async.client.Fixture.getMongoClient
import static com.mongodb.async.client.gridfs.AsynchronousChannelHelper.toAsyncInputStream
import static com.mongodb.async.client.gridfs.AsynchronousChannelHelper.toAsyncOutputStream

class AsynchronousChannelHelperSmokeTestSpecification extends FunctionalSpecification {
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

    @IgnoreIf({ !jvm.isJava7Compatible() })
    @SuppressWarnings(['JavaIoPackageAccess', 'Println'])
    def 'should round trip a AsynchronousFileChannel'() {
        given:
        URI inputFileURI = getClass().getResource('/GridFSAsync/GridFSTestFile.txt').toURI()
        Path inputPath = Paths.get(inputFileURI)
        Path outputPath = Paths.get(inputPath.toString() + '.copy')

        when:
        def inputChannel = AsynchronousFileChannel.open(inputPath)
        def objectId = run(gridFSBucket.&uploadFromStream, 'myFile', toAsyncInputStream(inputChannel))

        then:
        run(filesCollection.&count) == 1
        run(chunksCollection.&count) == 1

        when:
        def outputChannel = AsynchronousFileChannel.open(outputPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        run(gridFSBucket.&downloadToStream, objectId, toAsyncOutputStream(outputChannel))

        then:
        ByteBuffer.wrap(Files.readAllBytes(inputPath)) == ByteBuffer.wrap(Files.readAllBytes(outputPath))

        cleanup:
        if (outputPath) {
            try {
                Files.delete(outputPath)
            } catch (NoSuchFileException e) {
                println(e.getMessage())
            }
        }
    }

    @IgnoreIf({ !jvm.isJava7Compatible() })
    def 'should round trip a AsynchronousByteChannel'() {
        when:
        def content = ByteBuffer.wrap('Hello GridFS Byte Channels'.getBytes())
        def asyncByteChannel = new TestAsynchronousByteChannel(content)
        def objectId = run(gridFSBucket.&uploadFromStream, 'myFile', toAsyncInputStream(asyncByteChannel))

        then:
        run(filesCollection.&count) == 1
        run(chunksCollection.&count) == 1

        when:
        run(gridFSBucket.&downloadToStream, objectId, toAsyncOutputStream(asyncByteChannel))

        then:
        content == asyncByteChannel.getReadBuffer()
        content == asyncByteChannel.getWriteBuffer()
    }

}
