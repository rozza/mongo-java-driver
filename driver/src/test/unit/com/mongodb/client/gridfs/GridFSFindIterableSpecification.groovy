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

import com.mongodb.Block
import com.mongodb.CursorType
import com.mongodb.FindIterableImpl
import com.mongodb.Function
import com.mongodb.MongoClient
import com.mongodb.MongoGridFSException
import com.mongodb.MongoNamespace
import com.mongodb.TestOperationExecutor
import com.mongodb.client.gridfs.model.GridFSFile
import com.mongodb.client.model.FindOptions
import com.mongodb.operation.BatchCursor
import com.mongodb.operation.FindOperation
import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonInt32
import org.bson.Document
import org.bson.codecs.DecoderContext
import org.bson.codecs.DocumentCodec
import spock.lang.Shared
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.secondary
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static spock.util.matcher.HamcrestSupport.expect

class GridFSFindIterableSpecification extends Specification {

    def codecRegistry = MongoClient.getDefaultCodecRegistry()
    def readPreference = secondary()
    def namespace = new MongoNamespace('test', 'fs.files')

    def 'should build the expected findOperation'() {
        given:
        def executor = new TestOperationExecutor([null, null]);
        def underlying = new FindIterableImpl(namespace, Document, Document, codecRegistry, readPreference, executor,
                new Document(), new FindOptions())
        def findIterable = new GridFSFindIterableImpl(underlying)

        when: 'default input should be as expected'
        findIterable.iterator()

        def operation = executor.getReadOperation() as FindOperation<Document>
        def readPreference = executor.getReadPreference()

        then:
        expect operation, isTheSameAs(new FindOperation<Document>(namespace, new DocumentCodec()).filter(new BsonDocument()).slaveOk(true))
        readPreference == secondary()

        when: 'overriding initial options'
        findIterable.filter(new Document('filter', 2))
                .sort(new Document('sort', 2))
                .maxTime(999, MILLISECONDS)
                .batchSize(99)
                .limit(99)
                .skip(9)
                .noCursorTimeout(true)
                .iterator()

        operation = executor.getReadOperation() as FindOperation<Document>

        then: 'should use the overrides'
        expect operation, isTheSameAs(new FindOperation<Document>(namespace, new DocumentCodec())
                .filter(new BsonDocument('filter', new BsonInt32(2)))
                .sort(new BsonDocument('sort', new BsonInt32(2)))
                .maxTime(999, MILLISECONDS)
                .batchSize(99)
                .limit(99)
                .skip(9)
                .noCursorTimeout(true)
                .slaveOk(true)
        )
    }

    def 'should handle mixed types'() {
        given:
        def executor = new TestOperationExecutor([null, null]);
        def findOptions = new FindOptions()
        def findIterable = new FindIterableImpl(namespace, Document, Document, codecRegistry, readPreference, executor,
                new Document('filter', 1), findOptions)

        when:
        findIterable.filter(new Document('filter', 1))
                .sort(new BsonDocument('sort', new BsonInt32(1)))
                .modifiers(new Document('modifier', 1))
                .iterator()

        def operation = executor.getReadOperation() as FindOperation<Document>

        then:
        expect operation, isTheSameAs(new FindOperation<Document>(namespace, new DocumentCodec())
                .filter(new BsonDocument('filter', new BsonInt32(1)))
                .sort(new BsonDocument('sort', new BsonInt32(1)))
                .modifiers(new BsonDocument('modifier', new BsonInt32(1)))
                .cursorType(CursorType.NonTailable)
                .slaveOk(true)
        )
    }

    def 'should follow the MongoIterable interface as expected'() {
        given:
        def expectedFilenames = ['File 1', 'File 2', 'File 3']
        def cursor = {
            Stub(BatchCursor) {
                def count = 0
                def results;
                def getResult = {
                    count++
                    results = count == 1 ? cannedResults : null
                    results
                }
                next() >> {
                    getResult()
                }
                hasNext() >> {
                    count == 0
                }

            }
        }
        def executor = new TestOperationExecutor([cursor(), cursor(), cursor(), cursor()]);
        def underlying = new FindIterableImpl(namespace, Document, Document, codecRegistry, readPreference, executor,
                new Document(), new FindOptions())
        def mongoIterable = new GridFSFindIterableImpl(underlying)

        when:
        def firstResult = mongoIterable.first()
        def expectedResult = cannedResults[0]

        then:
        firstResult.getId() == expectedResult.get('_id');
        firstResult.getFilename() == expectedResult.getString('filename').getValue()
        firstResult.getLength() == expectedResult.getInt64('length').getValue()
        firstResult.getChunkSize() == expectedResult.getInt32('chunkSize').getValue()
        firstResult.getMD5() == expectedResult.getString('md5').getValue()
        firstResult.getUploadDate() == new Date(expectedResult.getDateTime('uploadDate').getValue())

        if (expectedResult.get('_id').isObjectId()) {
            firstResult.getObjectId() == expectedResult.getObjectId('_id').getValue();
        } else {
            firstResult.getObjectId() == null
        }

        if (expectedResult.containsKey('metadata')) {
            def metadata = codecRegistry.get(Document)
                    .decode(new BsonDocumentReader(expectedResult.getDocument('metadata')), DecoderContext.builder().build())
            firstResult.getMetadata() == metadata
        } else {
            firstResult.getMetadata() == new Document()
        }
        if (expectedResult.containsKey('contentType')) {
            firstResult.getContentType() == expectedResult.getString('contentType').getValue()
        } else {
            firstResult.getContentType() == null
        }
        if (expectedResult.containsKey('aliases')) {
            firstResult.getAliases() == expectedResult.getArray('aliases').iterator()*.getString().getValue()
        } else {
            firstResult.getAliases() == null
        }

        when:
        def count = 0
        mongoIterable.forEach(new Block<GridFSFile>() {
            @Override
            void apply(GridFSFile document) {
                count++
            }
        })

        then:
        count == 3

        when:
        def target = []
        mongoIterable.into(target)

        then:
        target*.getFilename() == expectedFilenames

        when:
        target = []
        mongoIterable.map(new Function<GridFSFile, String>() {
            @Override
            String apply(GridFSFile file) {
                file.getFilename()
            }
        }).into(target)

        then:
        target == expectedFilenames

        where:
        cannedResults << [toSpecCannedResults, legacyCannedResults]
    }

    def 'should throw an exception when handling invalid aliases'() {
        given:
        def cannedResults = [BsonDocument.parse('''{ "_id" : { "$oid" : "000000000000000000000002" }, "filename" : "File 3",
                                    "length" : { "$numberLong" : "1" },  "chunkSize" : 255, "uploadDate" : { "$date" : 1438679434090 },
                                    "md5" : "d41d8cd98f00b204e9800998ecf8427e", "aliases" : ["File Three", 3] }''')]
        def cursor = {
            Stub(BatchCursor) {
                def count = 0
                def results;
                def getResult = {
                    count++
                    results = count == 1 ? cannedResults : null
                    results
                }
                next() >> {
                    getResult()
                }
                hasNext() >> {
                    count == 0
                }

            }
        }
        def executor = new TestOperationExecutor([cursor(), cursor(), cursor(), cursor()]);
        def underlying = new FindIterableImpl(namespace, Document, Document, codecRegistry, readPreference, executor,
                new Document(), new FindOptions())
        def mongoIterable = new GridFSFindIterableImpl(underlying)

        when:
        mongoIterable.first()

        then:
        thrown(MongoGridFSException)
    }

    @Shared
    def toSpecCannedResults = [
            BsonDocument.parse('''{ "_id" : { "$oid" : "000000000000000000000001" }, "filename" : "File 1",
                                    "length" : { "$numberLong" : "123" },  "chunkSize" : 255, "uploadDate" : { "$date" : 1438679434041 },
                                    "md5" : "d41d8cd98f00b204e9800998ecf8427e" }'''),
            BsonDocument.parse('''{ "_id" : { "$oid" : "000000000000000000000002" }, "filename" : "File 2",
                                    "length" : { "$numberLong" : "99999" },  "chunkSize" : 255, "uploadDate" : { "$date" : 1438679434050 },
                                    "md5" : "d41d8cd98f00b204e9800998ecf8427e" }'''),
            BsonDocument.parse('''{ "_id" : { "$oid" : "000000000000000000000003" }, "filename" : "File 3",
                                    "length" : { "$numberLong" : "1" },  "chunkSize" : 255, "uploadDate" : { "$date" : 1438679434090 },
                                    "md5" : "d41d8cd98f00b204e9800998ecf8427e" }''')
    ]

    @Shared
    def legacyCannedResults = [
            BsonDocument.parse('''{ "_id" : 1, "filename" : "File 1",
                                    "length" : { "$numberLong" : "123" },  "chunkSize" : 255, "uploadDate" : { "$date" : 1438679434041 },
                                    "md5" : "d41d8cd98f00b204e9800998ecf8427e" }'''),
            BsonDocument.parse('''{ "_id" : { "$oid" : "000000000000000000000001" }, "filename" : "File 2",
                                    "length" : { "$numberLong" : "99999" },  "chunkSize" : 255, "uploadDate" : { "$date" : 1438679434050 },
                                    "md5" : "d41d8cd98f00b204e9800998ecf8427e", "contentType" : "text/txt" }'''),
            BsonDocument.parse('''{ "_id" : { "$oid" : "000000000000000000000002" }, "filename" : "File 3",
                                    "length" : { "$numberLong" : "1" },  "chunkSize" : 255, "uploadDate" : { "$date" : 1438679434090 },
                                    "md5" : "d41d8cd98f00b204e9800998ecf8427e", "aliases" : ["File Three", "Third File"] }''')
    ]

    @Shared
    mixedResults = [toSpecCannedResults[0], legacyCannedResults[1], legacyCannedResults[2]]
}
