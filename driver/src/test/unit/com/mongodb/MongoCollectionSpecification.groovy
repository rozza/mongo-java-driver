/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb

import com.mongodb.bulk.DeleteRequest
import com.mongodb.client.MongoCollectionOptions
import com.mongodb.client.model.AggregateOptions
import com.mongodb.client.model.BulkWriteOptions
import com.mongodb.client.model.CountOptions
import com.mongodb.client.model.CreateIndexOptions
import com.mongodb.client.model.DeleteManyModel
import com.mongodb.client.model.DeleteOneModel
import com.mongodb.client.model.DistinctOptions
import com.mongodb.client.model.FindOneAndDeleteOptions
import com.mongodb.client.model.FindOneAndReplaceOptions
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.FindOptions
import com.mongodb.client.model.InsertManyOptions
import com.mongodb.client.model.InsertOneModel
import com.mongodb.client.model.MapReduceOptions
import com.mongodb.client.model.RenameCollectionOptions
import com.mongodb.client.model.ReplaceOneModel
import com.mongodb.client.model.UpdateManyModel
import com.mongodb.client.model.UpdateOneModel
import com.mongodb.client.model.UpdateOptions
import com.mongodb.connection.AcknowledgedWriteConcernResult
import com.mongodb.operation.AggregateOperation
import com.mongodb.operation.AggregateToCollectionOperation
import com.mongodb.operation.BatchCursor
import com.mongodb.operation.CountOperation
import com.mongodb.operation.CreateIndexOperation
import com.mongodb.operation.DeleteOperation
import com.mongodb.operation.DistinctOperation
import com.mongodb.operation.DropCollectionOperation
import com.mongodb.operation.DropIndexOperation
import com.mongodb.operation.FindAndDeleteOperation
import com.mongodb.operation.FindAndReplaceOperation
import com.mongodb.operation.FindAndUpdateOperation
import com.mongodb.operation.FindOperation
import com.mongodb.operation.InsertOperation
import com.mongodb.operation.ListIndexesOperation
import com.mongodb.operation.MapReduceToCollectionOperation
import com.mongodb.operation.MapReduceWithInlineResultsOperation
import com.mongodb.operation.MixedBulkWriteOperation
import com.mongodb.operation.RenameCollectionOperation
import com.mongodb.operation.UpdateOperation
import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonJavaScript
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodec
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.RootCodecRegistry
import org.bson.types.ObjectId
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static com.mongodb.ReadPreference.secondary
import static com.mongodb.bulk.WriteRequest.Type.REPLACE
import static com.mongodb.bulk.WriteRequest.Type.UPDATE

class MongoCollectionSpecification extends Specification {

    def namespace = new MongoNamespace('db', 'coll')
    def collection;
    def options = MongoCollectionOptions.builder().writeConcern(WriteConcern.JOURNALED)
                                        .readPreference(secondary())
                                        .codecRegistry(new RootCodecRegistry([new ValueCodecProvider(),
                                                                              new DocumentCodecProvider(),
                                                                              new BsonValueCodecProvider()]))
                                        .build()

    def 'should get namespace'() {
        given:
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, new TestOperationExecutor([]))

        expect:
        collection.getNamespace() == namespace
    }

    def 'should get options'() {
        given:
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, new TestOperationExecutor([]))

        expect:
        collection.getOptions() == options
    }

    def 'insertOne should use InsertOperation properly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, false, null)])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        collection.insertOne(new Document('_id', 1))

        then:
        def operation = executor.getWriteOperation() as InsertOperation
        operation.insertRequests[0].document == new BsonDocument('_id', new BsonInt32(1))
    }

    def 'insert should add _id to document'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, false, null)])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)


        def document = new Document()
        when:
        collection.insertOne(document)

        then:
        document.containsKey('_id')
        document.get('_id') instanceof ObjectId
        executor.getWriteOperation() as InsertOperation
    }

    def 'insertMany should use InsertOperation properly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(2, false, null),
                                                  new AcknowledgedWriteConcernResult(2, false, null)])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        collection.insertMany([new Document('_id', 1), new Document('_id', 2)], new InsertManyOptions().ordered(false));

        then:
        def operation = executor.getWriteOperation() as InsertOperation
        operation.insertRequests[0].document == new BsonDocument('_id', new BsonInt32(1))
        operation.insertRequests[1].document == new BsonDocument('_id', new BsonInt32(2))
        !operation.ordered

        when:
        collection.insertMany([new Document('_id', 1), new Document('_id', 2)], new InsertManyOptions().ordered(false));

        then:
        def operation2 = executor.getWriteOperation() as InsertOperation
        !operation2.ordered
    }

    def 'insertMany should add _id to documents'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(2, false, null)])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        def documents = [new Document(), new Document()]
        when:
        collection.insertMany(documents);

        then:
        documents[0].containsKey('_id')
        documents[0].get('_id') instanceof ObjectId
        documents[1].containsKey('_id')
        documents[1].get('_id') instanceof ObjectId
        executor.getWriteOperation() as InsertOperation
    }

    def 'replace should use ReplaceOperation properly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, false, null)])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        def result = collection.replaceOne(new Document('_id', 1),
                                           new Document('color', 'blue'),
                                           new UpdateOptions().upsert(true))

        then:
        def operation = executor.getWriteOperation() as UpdateOperation

        def replaceRequest = operation.updateRequests[0]
        replaceRequest.type == REPLACE
        !replaceRequest.multi
        replaceRequest.upsert
        replaceRequest.filter == new BsonDocument('_id', new BsonInt32(1))
        replaceRequest.update == new BsonDocument('color', new BsonString('blue'))

        result.modifiedCount == 0
        result.matchedCount == 1
        !result.upsertedId
    }

    def 'updateOne should use UpdateOperation properly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, false, null)])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        def result = collection.updateOne(new Document('_id', 1),
                                          new Document('$set', new Document('color', 'blue')),
                                          new UpdateOptions().upsert(true))

        then:
        def operation = executor.getWriteOperation() as UpdateOperation

        def updateRequest = operation.updateRequests[0]
        updateRequest.type == UPDATE
        !updateRequest.multi
        updateRequest.upsert
        updateRequest.filter == new BsonDocument('_id', new BsonInt32(1))
        updateRequest.update == new BsonDocument('$set', new BsonDocument('color', new BsonString('blue')))

        result.modifiedCount == 0
        result.matchedCount == 1
        !result.upsertedId
    }

    def 'updateMany should use UpdateOperation properly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, false, null)])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        def result = collection.updateMany(new Document('_id', 1),
                                           new Document('$set', new Document('color', 'blue')),
                                           new UpdateOptions().upsert(true))

        then:
        def operation = executor.getWriteOperation() as UpdateOperation

        def updateRequest = operation.updateRequests[0]
        updateRequest.type == UPDATE
        updateRequest.multi
        updateRequest.upsert
        updateRequest.filter == new BsonDocument('_id', new BsonInt32(1))
        updateRequest.update == new BsonDocument('$set', new BsonDocument('color', new BsonString('blue')))

        result.modifiedCount == 0
        result.matchedCount == 1
        !result.upsertedId
    }

    def 'deleteOne should use RemoveOperation properly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, false, null)])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        def result = collection.deleteOne(new Document('_id', 1));

        then:
        def operation = executor.getWriteOperation() as DeleteOperation

        def removeRequest = operation.deleteRequests[0]
        !removeRequest.multi
        removeRequest.filter == new BsonDocument('_id', new BsonInt32(1))

        result.deletedCount == 1
    }

    def 'deleteMany should use RemoveOperation properly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, false, null)])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        def result = collection.deleteMany(new Document('_id', 1));

        then:
        def operation = executor.getWriteOperation() as DeleteOperation

        def deleteRequest = operation.deleteRequests[0]
        deleteRequest.multi
        deleteRequest.filter == new BsonDocument('_id', new BsonInt32(1))

        result.deletedCount == 1
    }

    def 'find iteration should use FindOperation properly'() {
        given:
        def cursor = Stub(BatchCursor)
        cursor.hasNext() >>> [true, false]
        def executor = new TestOperationExecutor([cursor, cursor])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)
        def fluentFind = collection.find(new Document('cold', true))
                                   .batchSize(4)
                                   .maxTime(1, TimeUnit.SECONDS)
                                   .skip(5)
                                   .limit(100)
                                   .modifiers(new Document('$hint', 'i1'))
                                   .projection(new Document('x', 1))
                                   .sort(new Document('y', 1))

        when:
        fluentFind.iterator()
        def operation = executor.getReadOperation() as FindOperation

        then:
        operation.filter == new BsonDocument('cold', BsonBoolean.TRUE)
        operation.batchSize == 4
        operation.getMaxTime(TimeUnit.SECONDS) == 1
        operation.skip == 5
        operation.limit == 100
        operation.modifiers == new BsonDocument('$hint', new BsonString('i1'))
        operation.projection == new BsonDocument('x', new BsonInt32(1))
        operation.sort == new BsonDocument('y', new BsonInt32(1))
        !operation.isTailableCursor()
        !operation.isOplogReplay()
        !operation.isNoCursorTimeout()
        !operation.isAwaitData()
        !operation.isPartial()
        operation.isSlaveOk()
        executor.readPreference == secondary()

        when: 'all the boolean properties are enabled'
        fluentFind.awaitData(true)
                  .noCursorTimeout(true)
                  .partial(true)
                  .tailable(true)
                  .oplogReplay(true)

        fluentFind.iterator()
        operation = executor.getReadOperation() as FindOperation

        then: 'cursor flags contains all flags'
        operation.isTailableCursor()
        operation.isOplogReplay()
        operation.isNoCursorTimeout()
        operation.isAwaitData()
        operation.isPartial()
    }

    def 'fluent find should use FindOperation properly'() {
        given:
        def cursor = Stub(BatchCursor)
        cursor.hasNext() >>> [true, false]
        def executor = new TestOperationExecutor([cursor, cursor])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        collection.find()
                  .filter(new Document('cold', true))
                  .batchSize(4)
                  .maxTime(1, TimeUnit.SECONDS)
                  .skip(5)
                  .limit(100)
                  .modifiers(new Document('$hint', 'i1'))
                  .projection(new Document('x', 1))
                  .sort(new Document('y', 1))
                  .iterator()
        def operation = executor.getReadOperation() as FindOperation

        then:
        operation.filter == new BsonDocument('cold', BsonBoolean.TRUE)
        operation.batchSize == 4
        operation.getMaxTime(TimeUnit.SECONDS) == 1
        operation.skip == 5
        operation.limit == 100
        operation.modifiers == new BsonDocument('$hint', new BsonString('i1'))
        operation.projection == new BsonDocument('x', new BsonInt32(1))
        operation.sort == new BsonDocument('y', new BsonInt32(1))
        !operation.isTailableCursor()
        !operation.isOplogReplay()
        !operation.isNoCursorTimeout()
        !operation.isAwaitData()
        !operation.isPartial()
        operation.isSlaveOk()
        executor.readPreference == secondary()

        when: 'all the boolean properties are enabled'
        options = new FindOptions().awaitData(true)
                                   .noCursorTimeout(true)
                                   .partial(true)
                                   .tailable(true)
                                   .oplogReplay(true)

        collection.find()
                  .awaitData(true)
                  .noCursorTimeout(true)
                  .partial(true)
                  .tailable(true)
                  .oplogReplay(true)
                  .iterator()
        operation = executor.getReadOperation() as FindOperation

        then: 'cursor flags contains all flags'
        operation.isTailableCursor()
        operation.isOplogReplay()
        operation.isNoCursorTimeout()
        operation.isAwaitData()
        operation.isPartial()
    }

    def 'find with first() should temporarily set limit to -1 and batchSize to 0'() {
        given:
        def document = new Document()
        def cursor = Stub(BatchCursor)
        cursor.hasNext() >>> [true, false]
        cursor.next() >> [document]

        def executor = new TestOperationExecutor([cursor, cursor])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)
        def fluentFind = collection.find(new Document('cold', true))
                                   .batchSize(4)
                                   .maxTime(1, TimeUnit.SECONDS)
                                   .skip(5)
                                   .limit(100)
                                   .modifiers(new Document('$hint', 'i1'))
                                   .projection(new Document('x', 1))
                                   .sort(new Document('y', 1))

        when:
        def first = fluentFind.first()
        def operation = executor.getReadOperation() as FindOperation
        def readPreference = executor.getReadPreference()

        then:
        first.is(document)
        operation.filter == new BsonDocument('cold', BsonBoolean.TRUE)
        operation.getMaxTime(TimeUnit.SECONDS) == 1
        operation.modifiers == new BsonDocument('$hint', new BsonString('i1'))
        operation.projection == new BsonDocument('x', new BsonInt32(1))
        operation.sort == new BsonDocument('y', new BsonInt32(1))
        operation.skip == 5
        !operation.isTailableCursor()
        !operation.isOplogReplay()
        !operation.isNoCursorTimeout()
        !operation.isAwaitData()
        !operation.isPartial()
        operation.batchSize == 0
        operation.limit == -1
        operation.isSlaveOk()
        readPreference == secondary()

        when:
        fluentFind.iterator()
        operation = executor.getReadOperation() as FindOperation

        then: 'cursor flags contains all flags'
        operation.filter == new BsonDocument('cold', BsonBoolean.TRUE)
        operation.getMaxTime(TimeUnit.SECONDS) == 1
        operation.modifiers == new BsonDocument('$hint', new BsonString('i1'))
        operation.projection == new BsonDocument('x', new BsonInt32(1))
        operation.sort == new BsonDocument('y', new BsonInt32(1))
        operation.skip == 5
        !operation.isTailableCursor()
        !operation.isOplogReplay()
        !operation.isNoCursorTimeout()
        !operation.isAwaitData()
        !operation.isPartial()
        operation.isSlaveOk()
        operation.batchSize == 4
        operation.limit == 100
    }

    def 'count should use CountOperation properly'() {
        given:
        def executor = new TestOperationExecutor([42L])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        collection.count(new Document('cold', true), new CountOptions()
                                           .maxTime(1, TimeUnit.SECONDS)
                                           .skip(5)
                                           .limit(100)
                                           .hint(new Document('x', 1)))

        then:
        def operation = executor.getReadOperation() as CountOperation
        operation.filter == new BsonDocument('cold', BsonBoolean.TRUE)
        operation.getMaxTime(TimeUnit.SECONDS) == 1
        operation.skip == 5
        operation.limit == 100
        operation.hint == new BsonDocument('x', new BsonInt32(1))
        executor.readPreference == secondary()
    }

    def 'count should use CountOperation properly with hint string'() {
        given:
        def executor = new TestOperationExecutor([42L])
        collection = new MongoCollectionImpl<Document>(namespace, Document, this.options, executor)

        when:
        collection.count(new Document(), new CountOptions().hintString('idx1'))

        then:
        def operation = executor.getReadOperation() as CountOperation
        operation.hint == new BsonString('idx1')
    }

    def 'distinct should use DistinctOperation properly'() {
        given:
        def executor = new TestOperationExecutor([new BsonArray(Arrays.asList(new BsonString('foo'), new BsonInt32(42)))])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        collection.distinct('fieldName1', new Document('cold', true), new DistinctOptions().maxTime(1, TimeUnit.SECONDS))

        then:
        def operation = executor.getReadOperation() as DistinctOperation
        operation.filter == new BsonDocument('cold', BsonBoolean.TRUE)
        operation.getMaxTime(TimeUnit.SECONDS) == 1
        executor.readPreference == secondary()
    }

    def 'bulk insert should use BulkWriteOperation properly'() {
        given:
        def executor = new TestOperationExecutor([new com.mongodb.connection.AcknowledgedBulkWriteResult(1, 0, 0, 0, [])])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)
        def document = new Document();

        when:
        collection.bulkWrite([new InsertOneModel<>(document),
                              new UpdateOneModel<>(new Document('_id', 1),
                                                   new Document('$set', new Document('color', 'blue')),
                                                   new UpdateOptions().upsert(true)),
                              new UpdateManyModel<>(new Document('_id', 1),
                                                    new Document('$set', new Document('color', 'blue')),
                                                    new UpdateOptions().upsert(true)),
                              new ReplaceOneModel<>(new Document('_id', 1),
                                                    new Document('color', 'blue'),
                                                    new UpdateOptions().upsert(true)),
                              new DeleteOneModel<>(new Document('_id', 1)),
                              new DeleteManyModel<>(new Document('_id', 1))],
                             new BulkWriteOptions().ordered(false))


        then:
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        def insertRequest = operation.writeRequests[0] as com.mongodb.bulk.InsertRequest
        insertRequest.document == new BsonDocument('_id', new BsonObjectId(document.getObjectId('_id')))

        def updateOneRequest = operation.writeRequests[1] as com.mongodb.bulk.UpdateRequest
        updateOneRequest.type == UPDATE
        !updateOneRequest.multi
        updateOneRequest.upsert
        updateOneRequest.filter == new BsonDocument('_id', new BsonInt32(1))
        updateOneRequest.update == new BsonDocument('$set', new BsonDocument('color', new BsonString('blue')))

        def updateManyRequest = operation.writeRequests[2] as com.mongodb.bulk.UpdateRequest
        updateManyRequest.type == UPDATE
        updateManyRequest.multi
        updateManyRequest.upsert
        updateManyRequest.filter == new BsonDocument('_id', new BsonInt32(1))
        updateManyRequest.update == new BsonDocument('$set', new BsonDocument('color', new BsonString('blue')))

        def replaceRequest = operation.writeRequests[3] as com.mongodb.bulk.UpdateRequest
        replaceRequest.type == REPLACE
        !replaceRequest.multi
        replaceRequest.upsert
        replaceRequest.filter == new BsonDocument('_id', new BsonInt32(1))
        replaceRequest.update == new BsonDocument('color', new BsonString('blue'))

        def deleteOneRequest = operation.writeRequests[4] as DeleteRequest
        !deleteOneRequest.multi
        deleteOneRequest.filter == new BsonDocument('_id', new BsonInt32(1))

        def deleteManyRequest = operation.writeRequests[5] as DeleteRequest
        deleteManyRequest.multi
        deleteManyRequest.filter == new BsonDocument('_id', new BsonInt32(1))

        document.containsKey('_id')
    }

    def 'bulk insert should generate _id'() {
        given:
        def executor = new TestOperationExecutor([new com.mongodb.connection.AcknowledgedBulkWriteResult(1, 0, 0, 0, [])])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)
        def document = new Document();

        when:
        collection.bulkWrite([new InsertOneModel<>(document)])


        then:
        document.containsKey('_id')
    }

    def 'aggregate should use AggregationOperation properly'() {
        given:
        def cursor = Stub(BatchCursor)
        def executor = new TestOperationExecutor([cursor])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        collection.aggregate([new Document('$match', new Document('job', 'plumber'))],
                             new AggregateOptions().allowDiskUse(true)
                                                   .batchSize(10)
                                                   .maxTime(1, TimeUnit.SECONDS)
                                                   .useCursor(true)).first()

        then:
        def operation = executor.getReadOperation() as AggregateOperation
        operation.pipeline == [new BsonDocument('$match', new BsonDocument('job', new BsonString('plumber')))]
        operation.batchSize == 10
        operation.getMaxTime(TimeUnit.SECONDS) == 1
        operation.useCursor
        executor.readPreference == secondary()
    }

    def 'aggregate should use AggregationToCollectionOperation properly'() {
        given:
        def cursor = Stub(BatchCursor)
        def executor = new TestOperationExecutor([null, cursor])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        def pipeline = [new Document('$match', new Document('job', 'plumber')), new Document('$out', 'outCollection')]
        def options = new AggregateOptions().allowDiskUse(true)
                                            .batchSize(10)
                                            .maxTime(1, TimeUnit.SECONDS)
                                            .useCursor(true)

        when:
        def first = collection.aggregate(pipeline, options).first()

        then:
        first == null
        def aggregateToCollectionOperation = executor.getWriteOperation() as AggregateToCollectionOperation
        aggregateToCollectionOperation != null
        aggregateToCollectionOperation.pipeline == [new BsonDocument('$match', new BsonDocument('job', new BsonString('plumber'))),
                                                    new BsonDocument('$out', new BsonString('outCollection'))]
        aggregateToCollectionOperation.getMaxTime(TimeUnit.SECONDS) == 1

        def findOperation = executor.getReadOperation() as FindOperation
        findOperation != null
        findOperation.namespace == new MongoNamespace(namespace.getDatabaseName(), 'outCollection')
        findOperation.decoder instanceof DocumentCodec

        executor.readPreference == secondary()
    }

    def 'mapReduce should use the MapReduceWithInlineResultsOperation properly'() {
        given:
        def cursor = Stub(BatchCursor)
        def executor = new TestOperationExecutor([cursor])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        def first = collection.mapReduce('map', 'reduce').first()

        then:
        first == null
        def operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation<Document>
        operation.getFilter() == null
        operation.getFinalizeFunction() == null
        operation.getLimit() == 0
        operation.getMapFunction() == new BsonJavaScript('map')
        operation.getReduceFunction() == new BsonJavaScript('reduce')
        operation.getScope() == null
        operation.getSort() == null
        operation.getMaxTime(TimeUnit.MILLISECONDS) == 0
        !operation.isJsMode()
        operation.isVerbose()
        executor.readPreference == secondary()
    }

    def 'mapReduce with options should use the MapReduceWithInlineResultsOperation properly'() {
        given:
        def cursor = Stub(BatchCursor)
        def executor = new TestOperationExecutor([cursor])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)
        def options = new MapReduceOptions()
                .finalizeFunction('finalize')
                .filter([filter: 1] as Document)
                .limit(10)
                .scope([scope: 1] as Document)
                .sort([sort: 1] as Document)
                .maxTime(10, TimeUnit.MILLISECONDS)
                .jsMode(true)
                .verbose(false)

        when:
        collection.mapReduce('map', 'reduce', options).first()

        then:
        def operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation<Document>
        operation.getMapFunction() == new BsonJavaScript('map')
        operation.getReduceFunction() == new BsonJavaScript('reduce')
        operation.getFinalizeFunction() == new BsonJavaScript('finalize')
        operation.getFilter() == new BsonDocument('filter', new BsonInt32(1))
        operation.getLimit() == 10
        operation.getScope() == new BsonDocument('scope', new BsonInt32(1))
        operation.getSort() == new BsonDocument('sort', new BsonInt32(1))
        operation.getMaxTime(TimeUnit.MILLISECONDS) == 10
        operation.isJsMode()
        !operation.isVerbose()
        executor.readPreference == secondary()
    }


    def 'mapReduce with options should use the MapReduceToCollectionOperation properly'() {
        given:
        def cursor = Stub(BatchCursor)
        def executor = new TestOperationExecutor([cursor, cursor])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)
        def options = new MapReduceOptions('out')
                .action(MapReduceOptions.Action.MERGE)
                .databaseName('db')
                .sharded(true)
                .nonAtomic(true)
                .finalizeFunction('finalize')
                .filter([filter: 1] as Document)
                .limit(10)
                .scope([scope: 1] as Document)
                .sort([sort: 1] as Document)
                .maxTime(10, TimeUnit.MILLISECONDS)
                .jsMode(true)
                .verbose(false)

        when:
        collection.mapReduce('map', 'reduce', options).first()

        then:
        def operation = executor.getWriteOperation() as MapReduceToCollectionOperation
        operation.getMapFunction() == new BsonJavaScript('map')
        operation.getReduceFunction() == new BsonJavaScript('reduce')
        operation.getFinalizeFunction() == new BsonJavaScript('finalize')
        operation.getFilter() == new BsonDocument('filter', new BsonInt32(1))
        operation.getLimit() == 10
        operation.getScope() == new BsonDocument('scope', new BsonInt32(1))
        operation.getSort() == new BsonDocument('sort', new BsonInt32(1))
        operation.getMaxTime(TimeUnit.MILLISECONDS) == 10
        operation.isJsMode()
        !operation.isVerbose()
        operation.isJsMode()
        operation.action == 'merge'
        operation.getDatabaseName() == 'db'
        operation.isSharded()
        operation.isNonAtomic()

        when:
        def findOperation = executor.getReadOperation() as FindOperation<Document>

        then:
        findOperation.getNamespace() == new MongoNamespace('db', 'out')
        executor.readPreference == secondary()
    }

    def 'findOneAndDelete should use FindAndDeleteOperation correctly'() {
        given:
        def returnedDocument = new Document('_id', 1).append('cold', true)
        def executor = new TestOperationExecutor([returnedDocument])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        def result = collection.findOneAndDelete(new Document('cold', true),
                                                 new FindOneAndDeleteOptions().projection(new Document('field', 1))
                                                                              .sort(new Document('sort', -1)))

        then:
        def operation = executor.getWriteOperation() as FindAndDeleteOperation
        operation.getFilter() == new BsonDocument('cold', new BsonBoolean(true))
        operation.getProjection() == new BsonDocument('field', new BsonInt32(1))
        operation.getSort() == new BsonDocument('sort', new BsonInt32(-1))

        result == returnedDocument
    }

    def 'findOneAndReplace should use FindOneAndReplaceOperation correctly'() {
        given:
        def returnedDocument = new Document('_id', 1).append('cold', true)
        def executor = new TestOperationExecutor([returnedDocument, returnedDocument])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        def result = collection.findOneAndReplace(new Document('cold', true), new Document('hot', false),
                                                  new FindOneAndReplaceOptions().projection(new Document('field', 1))
                                                                                .sort(new Document('sort', -1)))

        then:
        def operation = executor.getWriteOperation() as FindAndReplaceOperation
        operation.getFilter() == new BsonDocument('cold', new BsonBoolean(true))
        operation.getReplacement() == new BsonDocument('hot', new BsonBoolean(false))
        operation.getProjection() == new BsonDocument('field', new BsonInt32(1))
        operation.getSort() == new BsonDocument('sort', new BsonInt32(-1))
        operation.isReturnOriginal()
        !operation.isUpsert()

        result == returnedDocument

        when:
        collection.findOneAndReplace(new Document('cold', true), new Document('hot', false),
                                     new FindOneAndReplaceOptions().projection(new Document('field', 1))
                                                                   .sort(new Document('sort', -1))
                                                                   .upsert(true)
                                                                   .returnOriginal(false))

        then:
        def operation2 = executor.getWriteOperation() as FindAndReplaceOperation
        operation2.isUpsert()
        !operation2.isReturnOriginal()
    }

    def 'findOneAndUpdate should use FindOneAndUpdateOperation correctly'() {
        given:
        def returnedDocument = new Document('_id', 1).append('cold', true)
        def executor = new TestOperationExecutor([returnedDocument, returnedDocument])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        def result = collection.findOneAndUpdate(new Document('cold', true), new Document('hot', false),
                                                 new FindOneAndUpdateOptions().projection(new Document('field', 1))
                                                                              .sort(new Document('sort', -1)))

        then:
        def operation = executor.getWriteOperation() as FindAndUpdateOperation
        operation.getFilter() == new BsonDocument('cold', new BsonBoolean(true))
        operation.getUpdate() == new BsonDocument('hot', new BsonBoolean(false))
        operation.getProjection() == new BsonDocument('field', new BsonInt32(1))
        operation.getSort() == new BsonDocument('sort', new BsonInt32(-1))
        operation.isReturnOriginal()
        !operation.isUpsert()

        result == returnedDocument

        when:
        collection.findOneAndUpdate(new Document('cold', true), new Document('hot', false),
                                    new FindOneAndUpdateOptions().projection(new Document('field', 1))
                                                                 .sort(new Document('sort', -1))
                                                                 .upsert(true)
                                                                 .returnOriginal(false))

        then:
        def operation2 = executor.getWriteOperation() as FindAndUpdateOperation
        operation2.isUpsert()
        !operation2.isReturnOriginal()
    }

    def 'dropCollection should use DropCollectionOperation properly'() {
        given:
        def executor = new TestOperationExecutor([null])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        collection.dropCollection()

        then:
        executor.getWriteOperation() instanceof DropCollectionOperation
    }

    @SuppressWarnings(['FactoryMethodName'])
    def 'createIndex should use CreateIndexOperation properly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        collection.createIndex(new Document('a', 1))

        then:
        def operation = executor.getWriteOperation() as CreateIndexOperation
        operation.getKey() == new BsonDocument('a', new BsonInt32(1))
        !operation.isBackground()
        !operation.isUnique()
        !operation.isSparse()
        operation.getName() == null
        operation.getExpireAfterSeconds() == null
        operation.getVersion() == null
        operation.getWeights() == null
        operation.getDefaultLanguage() == null
        operation.getLanguageOverride() == null
        operation.getTextIndexVersion() == null
        operation.getTwoDSphereIndexVersion() == null
        operation.getBits() == null
        operation.getMin() == null
        operation.getMax() == null
        operation.getBucketSize() == null

        when:
        collection.createIndex(new Document('a', 1), new CreateIndexOptions()
                .background(true)
                .unique(true)
                .sparse(true)
                .name('aIndex')
                .expireAfterSeconds(100)
                .version(1)
                .weights(new Document('a', 1000))
                .defaultLanguage('es')
                .languageOverride('language')
                .textIndexVersion(1)
                .twoDSphereIndexVersion(1)
                .bits(1)
                .min(-180.0)
                .max(180.0)
                .bucketSize(200.0)
        )

        then:
        def operation2 = executor.getWriteOperation() as CreateIndexOperation
        operation2.getKey() == new BsonDocument('a', new BsonInt32(1))
        operation2.isBackground()
        operation2.isUnique()
        operation2.isSparse()
        operation2.getName() == 'aIndex'
        operation2.getExpireAfterSeconds() == 100
        operation2.getVersion() == 1
        operation2.getWeights() == new BsonDocument('a', new BsonInt32(1000))
        operation2.getDefaultLanguage() == 'es'
        operation2.getLanguageOverride() == 'language'
        operation2.getTextIndexVersion() == 1
        operation2.getTwoDSphereIndexVersion() == 1
        operation2.getBits() == 1
        operation2.getMin() == -180.0
        operation2.getMax() == 180.0
        operation2.getBucketSize() == 200.0
    }

    def 'getIndexes should use GetIndexesOperation properly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        collection.getIndexes()

        then:
        executor.getReadOperation() instanceof ListIndexesOperation<Document>

        when:
        collection.getIndexes(BsonDocument)

        then:
        executor.getReadOperation() instanceof ListIndexesOperation<BsonDocument>
    }

    def 'dropIndex should use DropIndexOperation properly'() {
        given:
        def executor = new TestOperationExecutor([null])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        collection.dropIndex('index_name')

        then:
        def operation = executor.getWriteOperation() as DropIndexOperation
        operation.indexName == 'index_name'
    }

    def 'dropIndexes should use DropIndexOperation properly'() {
        given:
        def executor = new TestOperationExecutor([null])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)

        when:
        collection.dropIndexes()

        then:
        def operation = executor.getWriteOperation() as DropIndexOperation
        operation.indexName == '*'
    }


    def 'should use RenameCollectionOperation properly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        collection = new MongoCollectionImpl<Document>(namespace, Document, options, executor)
        def newNamespace = new MongoNamespace(namespace.getDatabaseName(), 'anotherCollection')

        when:
        collection.renameCollection(newNamespace)

        then:
        def operation = executor.getWriteOperation() as RenameCollectionOperation
        operation.originalNamespace == namespace
        operation.newNamespace == newNamespace
        !operation.isDropTarget()

        when:
        collection.renameCollection(newNamespace, new RenameCollectionOptions().dropTarget(true))

        then:
        def operation2 = executor.getWriteOperation() as RenameCollectionOperation
        operation2.originalNamespace == namespace
        operation2.newNamespace == newNamespace
        operation2.isDropTarget()
    }

}
