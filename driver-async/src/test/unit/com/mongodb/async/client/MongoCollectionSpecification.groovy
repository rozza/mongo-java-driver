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

package com.mongodb.async.client

import com.mongodb.MongoException
import com.mongodb.MongoNamespace
import com.mongodb.WriteConcern
import com.mongodb.async.MongoAsyncCursor
import com.mongodb.async.SingleResultFuture
import com.mongodb.bulk.DeleteRequest
import com.mongodb.bulk.InsertRequest
import com.mongodb.bulk.UpdateRequest
import com.mongodb.bulk.WriteRequest
import com.mongodb.client.OperationOptions
import com.mongodb.client.model.BulkWriteOptions
import com.mongodb.client.model.CountOptions
import com.mongodb.client.model.CreateIndexOptions
import com.mongodb.client.model.DistinctOptions
import com.mongodb.client.model.FindOneAndDeleteOptions
import com.mongodb.client.model.FindOneAndReplaceOptions
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.InsertManyOptions
import com.mongodb.client.model.InsertOneModel
import com.mongodb.client.model.MapReduceOptions
import com.mongodb.client.model.UpdateOptions
import com.mongodb.connection.AcknowledgedWriteConcernResult
import com.mongodb.operation.AggregateOperation
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
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonJavaScript
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodec
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.CodecConfigurationException
import org.bson.codecs.configuration.RootCodecRegistry
import org.hamcrest.BaseMatcher
import org.hamcrest.Description
import spock.lang.Specification

import static com.mongodb.ReadPreference.secondary
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static spock.util.matcher.HamcrestSupport.that

class MongoCollectionSpecification extends Specification {

    def namespace = new MongoNamespace('databaseName', 'collectionName')
    def options = OperationOptions.builder()
                                  .writeConcern(WriteConcern.ACKNOWLEDGED)
                                  .readPreference(secondary())
                                  .codecRegistry(MongoClientImpl.getDefaultCodecRegistry()).build()

    def 'should return the correct name from getName'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, options, new TestOperationExecutor([null]))

        expect:
        collection.getNamespace() == namespace
    }

    def 'should return the correct options'() {
        given:
        def collection = new MongoCollectionImpl(namespace, Document, options, new TestOperationExecutor([null]))

        expect:
        collection.getOptions() == options
    }

    def 'should use CountOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([1, 2, 3])
        def filter = new BsonDocument()
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new CountOperation(namespace).filter(filter)

        when:
        collection.count().get()
        def operation = executor.getReadOperation() as CountOperation

        then:
        that operation, isTheSameAs(expectedOperation)

        when:
        filter = new BsonDocument('a', new BsonInt32(1))
        collection.count(filter).get()
        operation = executor.getReadOperation() as CountOperation

        then:
        that operation, isTheSameAs(expectedOperation.filter(filter))

        when:
        def hint = new BsonDocument('hint', new BsonInt32(1))
        collection.count(filter, new CountOptions().hint(hint).skip(10).limit(100).maxTime(100, MILLISECONDS))
        operation = executor.getReadOperation() as CountOperation

        then:
        that operation, isTheSameAs(expectedOperation.filter(filter).hint(hint).skip(10).limit(100).maxTime(100, MILLISECONDS))
    }

    def 'should use DistinctOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([new BsonArray([new BsonString('distinct')]), new BsonArray([new BsonString('distinct')])])
        def filter = new BsonDocument()
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

        when:
        collection.distinct('test', filter).get()
        def operation = executor.getReadOperation() as DistinctOperation

        then:
        that operation, isTheSameAs(new DistinctOperation(namespace, 'test').filter(new BsonDocument()))

        when:
        filter = new BsonDocument('a', new BsonInt32(1))
        collection.distinct('test', filter, new DistinctOptions().maxTime(100, MILLISECONDS)).get()
        operation = executor.getReadOperation() as DistinctOperation

        then:
        that operation, isTheSameAs(new DistinctOperation(namespace, 'test').filter(filter).maxTime(100, MILLISECONDS))
    }

    def 'should handle exceptions in distinct correctly'() {
        given:
        def options = OperationOptions.builder().codecRegistry(new RootCodecRegistry(asList(new ValueCodecProvider(),
                                                                                            new BsonValueCodecProvider()))).build()
        def executor = new TestOperationExecutor([new MongoException('failure'),
                                                  new BsonArray([new BsonString('no document codec')])])
        def filter = new BsonDocument()
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

        when: 'A failed operation'
        collection.distinct('test', filter).get()

        then:
        thrown(MongoException)

        when: 'An unexpected result'
        collection.distinct('test', filter).get()

        then:
        thrown(MongoException)

        when: 'A missing codec should throw immediately'
        collection.distinct('test', new Document())

        then:
        thrown(CodecConfigurationException)
    }

    def 'should use FindOperation correctly'() {
        given:
        def asyncCursor = Stub(MongoAsyncCursor) {
            forEach(_) >> { new SingleResultFuture<Void>(null) }
        }
        def executor = new TestOperationExecutor([asyncCursor, asyncCursor, asyncCursor, asyncCursor])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def documentOperation = new FindOperation(namespace, new DocumentCodec()).filter(new BsonDocument()).slaveOk(true)
        def bsonOperation = new FindOperation(namespace, new BsonDocumentCodec()).filter(new BsonDocument()).slaveOk(true)

        when:
        collection.find().into([]).get()
        def operation = executor.getReadOperation() as FindOperation

        then:
        that operation, isTheSameAs(documentOperation)

        when:
        collection.find(BsonDocument).into([]).get()
        operation = executor.getReadOperation() as FindOperation

        then:
        that operation, isTheSameAs(bsonOperation)

        when:
        collection.find(new Document('filter', 1)).into([]).get()
        operation = executor.getReadOperation() as FindOperation

        then:
        that operation, isTheSameAs(documentOperation.filter(new BsonDocument('filter', new BsonInt32(1))))

        when:
        collection.find(new Document('filter', 1), BsonDocument).into([]).get()
        operation = executor.getReadOperation() as FindOperation

        then:
        that operation, isTheSameAs(bsonOperation.filter(new BsonDocument('filter', new BsonInt32(1))))
    }

    def 'should use AggregateOperation correctly'() {
        given:
        def asyncCursor = Stub(MongoAsyncCursor) {
            forEach(_) >> { new SingleResultFuture<Void>(null) }
        }
        def executor = new TestOperationExecutor([asyncCursor, asyncCursor])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

        when:
        collection.aggregate([new Document('$match', 1)]).into([]).get()
        def operation = executor.getReadOperation() as AggregateOperation

        then:
        that operation, isTheSameAs(new AggregateOperation(namespace, [new BsonDocument('$match', new BsonInt32(1))], new DocumentCodec()))

        when:
        collection.aggregate([new Document('$match', 1)], BsonDocument).into([]).get()
        operation = executor.getReadOperation() as AggregateOperation

        then:
        that operation, isTheSameAs(new AggregateOperation(namespace, [new BsonDocument('$match', new BsonInt32(1))],
                                                           new BsonDocumentCodec()))
    }

    def 'should handle exceptions in aggregate correctly'() {
        given:
        def options = OperationOptions.builder().codecRegistry(new RootCodecRegistry(asList(new ValueCodecProvider(),
                                                                                            new BsonValueCodecProvider()))).build()
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

        when: 'The operation fails with an exception'
        collection.aggregate([new BsonDocument('$match', new BsonInt32(1))], BsonDocument).into([]).get()

        then: 'the future should handle the exception'
        thrown(MongoException)

        when: 'a codec is missing its acceptable to immediately throw'
        collection.aggregate([new Document('$match', 1)])

        then:
        thrown(CodecConfigurationException)
    }

    def 'should use MapReduceWithInlineResultsOperation correctly'() {
        given:
        def asyncCursor = Stub(MongoAsyncCursor) {
            forEach(_) >> { new SingleResultFuture<Void>(null) }
        }
        def executor = new TestOperationExecutor([asyncCursor, asyncCursor, asyncCursor, asyncCursor])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def documentOperation = new MapReduceWithInlineResultsOperation(namespace, new BsonJavaScript('map'), new BsonJavaScript('reduce'),
                                                                        new DocumentCodec()).verbose(true)
        def bsonOperation = new MapReduceWithInlineResultsOperation(namespace, new BsonJavaScript('map'), new BsonJavaScript('reduce'),
                                                                    new BsonDocumentCodec()).verbose(true)

        when:
        collection.mapReduce('map', 'reduce').into([]).get()
        def operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation

        then:
        that operation, isTheSameAs(documentOperation)

        when:
        def mapReduceOptions = new MapReduceOptions().finalizeFunction('final')
        collection.mapReduce('map', 'reduce', mapReduceOptions).into([]).get()
        operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation

        then:
        that operation, isTheSameAs(documentOperation.finalizeFunction(new BsonJavaScript('final')))

        when:
        collection.mapReduce('map', 'reduce', BsonDocument).into([]).get()
        operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation

        then:
        that operation, isTheSameAs(bsonOperation)

        when:
        collection.mapReduce('map', 'reduce', mapReduceOptions, BsonDocument).into([]).get()
        operation = executor.getReadOperation() as MapReduceWithInlineResultsOperation

        then:
        that operation, isTheSameAs(bsonOperation.finalizeFunction(new BsonJavaScript('final')))

    }

    def 'should use MapReduceToCollectionOperation correctly'() {
        given:
        def asyncCursor = Stub(MongoAsyncCursor) {
            forEach(_) >> { new SingleResultFuture<Void>(null) }
        }
        def executor = new TestOperationExecutor([asyncCursor, asyncCursor, asyncCursor, asyncCursor])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new MapReduceToCollectionOperation(namespace, new BsonJavaScript('map'), new BsonJavaScript('reduce'),
                                                                   'collectionName').filter(new BsonDocument('filter', new BsonInt32(1)))
                                                                                    .finalizeFunction(new BsonJavaScript('final'))
                                                                                    .verbose(true)
        def mapReduceOptions = new MapReduceOptions('collectionName').filter(new Document('filter', 1)).finalizeFunction('final')

        when:
        collection.mapReduce('map', 'reduce', mapReduceOptions).into([]).get()
        def operation = executor.getWriteOperation() as MapReduceToCollectionOperation

        then:
        that operation, isTheSameAs(expectedOperation)

        when: 'The following read operation'
        operation = executor.getReadOperation() as FindOperation

        then:
        that operation, isTheSameAs(new FindOperation(new MongoNamespace(namespace.databaseName, 'collectionName'), new DocumentCodec())
                                            .filter(new BsonDocument()))

        when:
        collection.mapReduce('map', 'reduce', mapReduceOptions, BsonDocument).into([]).get()
        operation = executor.getWriteOperation() as MapReduceToCollectionOperation

        then:
        that operation, isTheSameAs(expectedOperation)

        when: 'The following read operation'
        operation = executor.getReadOperation() as FindOperation

        then:
        that operation, isTheSameAs(new FindOperation(new MongoNamespace(namespace.databaseName, 'collectionName'), new BsonDocumentCodec())
                                            .filter(new BsonDocument()))
    }

    def 'should handle exceptions in mapReduce correctly'() {
        given:
        def options = OperationOptions.builder().codecRegistry(new RootCodecRegistry(asList(new ValueCodecProvider(),
                                                                                            new BsonValueCodecProvider()))).build()
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

        when: 'The operation fails with an exception'
        collection.mapReduce('map', 'reduce', BsonDocument).into([]).get()

        then: 'the future should handle the exception'
        thrown(MongoException)

        when: 'a codec is missing its acceptable to immediately throw'
        collection.mapReduce('map', 'reduce').into([])

        then:
        thrown(CodecConfigurationException)
    }

    def 'should use MixedBulkWriteOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = { boolean ordered ->
            new MixedBulkWriteOperation(namespace, [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))],
                                        ordered, WriteConcern.ACKNOWLEDGED)
        }

        when:
        collection.bulkWrite([new InsertOneModel(new Document('_id', 1))]).get()
        def operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        that operation, isTheSameAs(expectedOperation(false))

        when:
        collection.bulkWrite([new InsertOneModel(new Document('_id', 1))], new BulkWriteOptions().ordered(true)).get()
        operation = executor.getWriteOperation() as MixedBulkWriteOperation

        then:
        that operation, isTheSameAs(expectedOperation(true))
    }

    def 'should handle exceptions in bulkWrite correctly'() {
        given:
        def options = OperationOptions.builder().codecRegistry(new RootCodecRegistry(asList(new ValueCodecProvider(),
                                                                                            new BsonValueCodecProvider()))).build()
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)

        when: 'a codec is missing its acceptable to immediately throw'
        collection.bulkWrite([new InsertOneModel(new Document('_id', 1))])

        then:
        thrown(CodecConfigurationException)
    }

    def 'should use InsertOneOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new InsertOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                                    [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))])

        when:
        collection.insertOne(new Document('_id', 1)).get()
        def operation = executor.getWriteOperation() as InsertOperation

        then:
        that operation, isTheSameAs(expectedOperation)
    }

    def 'should use InsertManyOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = { ordered ->
            new InsertOperation(namespace, ordered, WriteConcern.ACKNOWLEDGED,
                                [new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                                 new InsertRequest(new BsonDocument('_id', new BsonInt32(2)))])
        }

        when:
        collection.insertMany([new Document('_id', 1), new Document('_id', 2)]).get()
        def operation = executor.getWriteOperation() as InsertOperation

        then:
        that operation, isTheSameAs(expectedOperation(false))

        when:
        collection.insertMany([new Document('_id', 1), new Document('_id', 2)], new InsertManyOptions().ordered(true)).get()
        operation = executor.getWriteOperation() as InsertOperation

        then:
        that operation, isTheSameAs(expectedOperation(true))
    }

    def 'should use DeleteOneOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new DeleteOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                                    [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1))).multi(false)])

        when:
        collection.deleteOne(new Document('_id', 1)).get()
        def operation = executor.getWriteOperation() as DeleteOperation

        then:
        that operation, isTheSameAs(expectedOperation)
    }

    def 'should use DeleteManyOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new DeleteOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                                    [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1)))])


        when:
        collection.deleteMany(new Document('_id', 1)).get()
        def operation = executor.getWriteOperation() as DeleteOperation

        then:
        that operation, isTheSameAs(expectedOperation)
    }

    def 'should use UpdateOperation correctly for replaceOne'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new UpdateOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                                    [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)),
                                                                       new BsonDocument('a', new BsonInt32(10)),
                                                                       WriteRequest.Type.REPLACE)])

        when:
        collection.replaceOne(new Document('a', 1), new Document('a', 10)).get()
        def operation = executor.getWriteOperation() as UpdateOperation

        then:
        that operation, isTheSameAs(expectedOperation)
    }

    def 'should use UpdateOperation correctly for updateOne'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null),
                                                  new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = { boolean upsert ->
            new UpdateOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)),
                                                   new BsonDocument('a', new BsonInt32(10)),
                                                   WriteRequest.Type.UPDATE).multi(false).upsert(upsert)])
        }

        when:
        collection.updateOne(new Document('a', 1), new Document('a', 10)).get()
        def operation = executor.getWriteOperation() as UpdateOperation

        then:
        that operation, isTheSameAs(expectedOperation(false))

        when:
        collection.updateOne(new Document('a', 1), new Document('a', 10), new UpdateOptions().upsert(true)).get()
        operation = executor.getWriteOperation() as UpdateOperation

        then:
        that operation, isTheSameAs(expectedOperation(true))
    }

    def 'should use UpdateOperation correctly for updateMany'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null),
                                                  new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = { boolean upsert ->
            new UpdateOperation(namespace, true, WriteConcern.ACKNOWLEDGED,
                                [new UpdateRequest(new BsonDocument('a', new BsonInt32(1)),
                                                   new BsonDocument('a', new BsonInt32(10)),
                                                   WriteRequest.Type.UPDATE)
                                         .multi(true).upsert(upsert)])
        }

        when:
        collection.updateMany(new Document('a', 1), new Document('a', 10)).get()
        def operation = executor.getWriteOperation() as UpdateOperation

        then:
        that operation, isTheSameAs(expectedOperation(false))

        when:
        collection.updateMany(new Document('a', 1), new Document('a', 10), new UpdateOptions().upsert(true)).get()
        operation = executor.getWriteOperation() as UpdateOperation

        then:
        that operation, isTheSameAs(expectedOperation(true))
    }

    def 'should use FindOneAndDeleteOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null),
                                                  new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new FindAndDeleteOperation(namespace, new DocumentCodec()).filter(new BsonDocument('a', new BsonInt32(1)))

        when:
        collection.findOneAndDelete(new Document('a', 1)).get()
        def operation = executor.getWriteOperation() as FindAndDeleteOperation

        then:
        that operation, isTheSameAs(expectedOperation)

        when:
        collection.findOneAndDelete(new Document('a', 1), new FindOneAndDeleteOptions().projection(new Document('projection', 1))).get()
        operation = executor.getWriteOperation() as FindAndDeleteOperation

        then:
        that operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1))))
    }

    def 'should use FindOneAndReplaceOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null),
                                                  new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new FindAndReplaceOperation(namespace, new DocumentCodec(), new BsonDocument('a', new BsonInt32(10)))
                .filter(new BsonDocument('a', new BsonInt32(1)))

        when:
        collection.findOneAndReplace(new Document('a', 1), new Document('a', 10)).get()
        def operation = executor.getWriteOperation() as FindAndReplaceOperation

        then:
        that operation, isTheSameAs(expectedOperation)

        when:
        collection.findOneAndReplace(new Document('a', 1), new Document('a', 10),
                                     new FindOneAndReplaceOptions().projection(new Document('projection', 1))).get()
        operation = executor.getWriteOperation() as FindAndReplaceOperation

        then:
        that operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1))))
    }

    def 'should use FindAndUpdateOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([new AcknowledgedWriteConcernResult(1, true, null),
                                                  new AcknowledgedWriteConcernResult(1, true, null)])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new FindAndUpdateOperation(namespace, new DocumentCodec(), new BsonDocument('a', new BsonInt32(10)))
                .filter(new BsonDocument('a', new BsonInt32(1)))

        when:
        collection.findOneAndUpdate(new Document('a', 1), new Document('a', 10)).get()
        def operation = executor.getWriteOperation() as FindAndUpdateOperation

        then:
        that operation, isTheSameAs(expectedOperation)

        when:
        collection.findOneAndUpdate(new Document('a', 1), new Document('a', 10),
                                    new FindOneAndUpdateOptions().projection(new Document('projection', 1))).get()
        operation = executor.getWriteOperation() as FindAndUpdateOperation

        then:
        that operation, isTheSameAs(expectedOperation.projection(new BsonDocument('projection', new BsonInt32(1))))
    }

    def 'should use DropCollectionOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new DropCollectionOperation(namespace)

        when:
        collection.dropCollection().get()
        def operation = executor.getWriteOperation() as DropCollectionOperation

        then:
        that operation, isTheSameAs(expectedOperation)
    }

    def 'should use CreateIndexOperations correctly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new CreateIndexOperation(namespace, new BsonDocument('key', new BsonInt32(1)))
        when:
        collection.createIndex(new Document('key', 1)).get()
        def operation = executor.getWriteOperation() as CreateIndexOperation

        then:
        that operation, isTheSameAs(expectedOperation)

        when:
        collection.createIndex(new Document('key', 1), new CreateIndexOptions().background(true)).get()
        operation = executor.getWriteOperation() as CreateIndexOperation

        then:
        that operation, isTheSameAs(expectedOperation.background(true))
    }

    def 'should use ListIndexesOperations correctly'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new ListIndexesOperation(namespace, new DocumentCodec())

        when:
        collection.getIndexes().get()
        def operation = executor.getReadOperation() as ListIndexesOperation

        then:
        that operation, isTheSameAs(expectedOperation)
    }

    def 'should use DropIndexOperation correctly for dropIndex'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new DropIndexOperation(namespace, 'indexName')

        when:
        collection.dropIndex('indexName').get()
        def operation = executor.getWriteOperation() as DropIndexOperation

        then:
        that operation, isTheSameAs(expectedOperation)
    }

    def 'should use DropIndexOperation correctly for dropIndexes'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def expectedOperation = new DropIndexOperation(namespace, '*')

        when:
        collection.dropIndex('*').get()
        def operation = executor.getWriteOperation() as DropIndexOperation

        then:
        that operation, isTheSameAs(expectedOperation)
    }

    def 'should use RenameCollectionOperation correctly'() {
        given:
        def executor = new TestOperationExecutor([null])
        def collection = new MongoCollectionImpl(namespace, Document, options, executor)
        def newNamespace = new MongoNamespace(namespace.getDatabaseName(), 'newName')
        def expectedOperation = new RenameCollectionOperation(namespace, newNamespace)

        when:
        collection.renameCollection(newNamespace).get()
        def operation = executor.getWriteOperation() as RenameCollectionOperation

        then:
        that operation, isTheSameAs(expectedOperation)
    }

    static isTheSameAs(final Object e) {
        [
                matches         : { a -> compare(e, a) },
                describeTo      : { Description description -> description.appendText("Operation has the same attributes ${e.class.name}")
                },
                describeMismatch: { a, description -> describer(e, a, description) }
        ] as BaseMatcher
    }

    static compare(expected, actual) {
        if (expected == actual) {
            return true
        }
        if (expected == null || actual == null) {
            return false
        }
        if (actual.class.name != expected.class.name) {
            return false
        }
        actual.class.declaredFields.findAll { !it.synthetic }*.name.collect { it ->
            if (it == 'decoder') {
                return actual.decoder.class == expected.decoder.class
            } else if (actual."$it" != expected."$it") {
                def (a1, e1) = [actual."$it", expected."$it"]
                if (List.isCase(a1) && List.isCase(e1) && (a1.size() == e1.size())) {
                    def i = -1
                    return a1.collect { a -> i++; compare(a, e1[i]) }.every { it }
                }
                return false
            }
            true
        }.every { it }
    }

    static describer(expected, actual, description) {
        if (expected == actual) {
            return true
        }
        if (expected == null || actual == null) {
            description.appendText("different values: $expected != $actual, ")
            return false
        }
        if (actual.class.name != expected.class.name) {
            description.appendText("different classes: ${expected.class.name} != ${actual.class.name}, ")
            return false
        }
        actual.class.declaredFields.findAll { !it.synthetic }*.name
                     .collect { it ->
            if (it == 'decoder' && actual.decoder.class != expected.decoder.class) {
                description.appendText("different decoder classes $it : ${expected.decoder.class.name} != ${actual.decoder.class.name}, ")
                return false
            } else if (actual."$it" != expected."$it") {
                def (a1, e1) = [actual."$it", expected."$it"]
                if (List.isCase(a1) && List.isCase(e1) && (a1.size() == e1.size())) {
                    def i = -1
                    a1.each { a ->
                        i++; if (!compare(a, e1[i])) {
                            describer(a, e1[i], description)
                        }
                    }.every { it }
                }
                description.appendText("different values in $it : $e1 != $a1\n")
                return false
            }
            true
        }
    }

}
