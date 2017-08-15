/*
 * Copyright 2017 MongoDB, Inc.
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

package com.mongodb.operation

import com.mongodb.MongoChangeStreamException
import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadConcern
import com.mongodb.WriteConcern
import com.mongodb.client.model.FullDocument
import com.mongodb.client.test.CollectionHelper
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DocumentCodec
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.concurrent.TimeUnit.MILLISECONDS

@IgnoreIf({ !serverVersionAtLeast([3, 5, 11]) || !isDiscoverableReplicaSet() })
class ChangeStreamOperationSpecification extends OperationFunctionalSpecification {

    def 'should have the correct defaults'() {
        when:
        ChangeStreamOperation operation = new ChangeStreamOperation<Document>(getNamespace(), FullDocument.DEFAULT, [], new DocumentCodec())

        then:
        operation.getBatchSize() == null
        operation.getCollation() == null
        operation.getFullDocument() == FullDocument.DEFAULT
        operation.getMaxAwaitTime(MILLISECONDS) == 0
        operation.getPipeline() == []
    }

    def 'should set optional values correctly'() {
        when:
        ChangeStreamOperation operation = new ChangeStreamOperation<Document>(getNamespace(), FullDocument.UPDATE_LOOKUP, [], new DocumentCodec())
                .batchSize(5)
                .collation(defaultCollation)
                .maxAwaitTime(15, MILLISECONDS)

        then:
        operation.getBatchSize() == 5
        operation.getCollation() == defaultCollation
        operation.getFullDocument() == FullDocument.UPDATE_LOOKUP
        operation.getMaxAwaitTime(MILLISECONDS) == 15
    }

    def 'should create the expected command'() {
        when:
        def pipeline = [BsonDocument.parse('{$match: {a: "A"}}')]
        def changeStream = BsonDocument.parse('{$changeStream: {fullDocument: "none"}}')

        def cursorResult = BsonDocument.parse('{ok: 1.0}')
                .append('cursor', new BsonDocument('id', new BsonInt64(0)).append('ns', new BsonString('db.coll'))
                .append('firstBatch', new BsonArrayWrapper([])))

        def operation = new ChangeStreamOperation<Document>(namespace, FullDocument.DEFAULT, pipeline, new DocumentCodec())
                .batchSize(5)
                .collation(defaultCollation)
                .maxAwaitTime(15, MILLISECONDS)
                .readConcern(ReadConcern.MAJORITY)

        def expectedCommand = new BsonDocument('aggregate', new BsonString(namespace.getCollectionName()))
                .append('collation', defaultCollation.asDocument())
                .append('cursor', new BsonDocument('batchSize', new BsonInt32(5)))
                .append('pipeline', new BsonArray([changeStream, *pipeline]))
                .append('readConcern', new BsonDocument('level', new BsonString('majority')))

        then:
        testOperation(operation, [3, 6, 0], expectedCommand, async, cursorResult)

        where:
        async << [true, false]
    }

    def 'should support return the expected results'() {
        given:
        def helper = getCollectionHelper(async)
        def expected = insertDocuments(helper, [1, 2])

        def pipeline = ['{$match: {operationType: "insert"}}', '{$sort: {"_id.ts": -1}}', '{$limit: 2}',
                        '{$sort: {"_id.ts": 1}}'].collect { BsonDocument.parse(it) }
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, pipeline, CODEC)

        when:
        def cursor = execute(operation, async)

        then:
        tryNext(cursor, async) == expected

        cleanup:
        cursor?.close()

        where:
        async << [true, false]
    }

    def 'should throw if the _id field is projected out'() {
        given:
        def helper = getCollectionHelper(async)
        insertDocuments(helper, [1, 2])
        def pipeline = [BsonDocument.parse('{$project: {"_id": 0}}')]
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, pipeline, CODEC)

        when:
        def cursor = execute(operation, async)
        tryNext(cursor, async)

        then:
        thrown(MongoChangeStreamException)

        where:
        async << [true, false]
    }

    def 'should act like a tailable cursor'() {
        given:
        def helper = getCollectionHelper(async)

        def pipeline = ['{$match: {operationType: "insert"}}'].collect { BsonDocument.parse(it) }
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, pipeline, CODEC)

        when:
        def cursor = execute(operation, async)

        then:
        tryNext(cursor, async) == null

        when:
        def expected = insertDocuments(helper, [1, 2])

        then:
        tryNext(cursor, async) == expected

        then:
        tryNext(cursor, async) == null

        when:
        expected = insertDocuments(helper, [3, 4])

        then:
        tryNext(cursor, async) == expected

        cleanup:
        cursor?.close()
        helper?.drop()

        where:
        async << [true, false]
    }

    def 'should be resumable'() {
        given:
        def helper = getCollectionHelper(async)

        def pipeline = ['{$match: {operationType: "insert"}}'].collect { BsonDocument.parse(it) }
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, pipeline, CODEC)

        when:
        def cursor = execute(operation, async)

        then:
        tryNext(cursor, async) == null

        when:
        helper.killCursor(helper.getNamespace(), cursor.getWrapped().getServerCursor())
        def expected = insertDocuments(helper, [1, 2])

        then:
        tryNext(cursor, async) == expected

        then:
        tryNext(cursor, async) == null

        when:
        expected = insertDocuments(helper, [3, 4])
        helper.killCursor(helper.getNamespace(), cursor.getWrapped().getServerCursor())

        then:
        tryNext(cursor, async) == expected

        cleanup:
        cursor?.close()
        helper?.drop()

        where:
        async << [true, false]
    }

    def 'should work with a resumeToken'() {
        given:
        def helper = getCollectionHelper(async)
        insertDocuments(helper, [1, 2])

        def pipeline = ['{$match: {operationType: "insert"}}'].collect { BsonDocument.parse(it) }
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, pipeline, CODEC)

        when:
        def cursor = execute(operation, async)
        def result = next(cursor, async)

        then:
        result.size() == 2

        when:
        cursor.close()
        operation.resumeAfter(result.head().getDocument('_id'))
        cursor = execute(operation, async)
        result = tryNext(cursor, async)

        then:
        result == [createExpectedChangeNotification(helper.getNamespace(), 2)]

        cleanup:
        cursor?.close()
        helper?.drop()

        where:
        async << [true, false]
    }

    def 'should support hasNext on the sync API'() {
        given:
        def helper = getCollectionHelper(false)
        insertDocuments(helper, [1, 2])
        def operation = new ChangeStreamOperation<BsonDocument>(helper.getNamespace(), FullDocument.DEFAULT, [], CODEC)

        when:
        def cursor = execute(operation, false)

        then:
        cursor.hasNext()

        cleanup:
        cursor?.close()
    }

    private final static CODEC = new BsonDocumentCodec()

    private CollectionHelper<Document> getCollectionHelper(final boolean async) {
        def namespace = new MongoNamespace(getDatabaseName(), "tailable_${async ? 'A' : 'S'}_${(int) (System.currentTimeMillis() / 100)}")
        getCollectionHelper(namespace)
    }

    private static List<BsonDocument> insertDocuments(final CollectionHelper<?> helper, final List<Integer> docs) {
        helper.insertDocuments(docs.collect { BsonDocument.parse("{_id: $it, a: $it}") }, WriteConcern.W2)
        docs.collect { createExpectedChangeNotification(helper.getNamespace(), it) }
    }

    private static BsonDocument createExpectedChangeNotification(MongoNamespace namespace, int idValue) {
        BsonDocument.parse("""{
            "_id": {
                "_id": $idValue,
                "ns": "$namespace",
            },
            "documentKey": {"_id": $idValue},
            "fullDocument": {"_id": $idValue, "a": $idValue},
            "ns": {"coll": "${namespace.getCollectionName()}", "db": "${namespace.getDatabaseName()}"},
            "operationType": "insert"
        }""")
    }
}
