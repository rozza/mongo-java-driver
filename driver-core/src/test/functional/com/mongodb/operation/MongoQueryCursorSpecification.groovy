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

package com.mongodb.operation

import category.Slow
import com.mongodb.MongoCursorNotFoundException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ServerCursor
import com.mongodb.binding.ConnectionSource
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.codecs.DocumentCodec
import com.mongodb.protocol.GetMoreProtocol
import com.mongodb.protocol.KillCursorProtocol
import com.mongodb.protocol.QueryProtocol
import com.mongodb.protocol.QueryResult
import org.bson.BsonDocument
import org.bson.BsonTimestamp
import org.junit.experimental.categories.Category
import org.mongodb.Document
import spock.lang.IgnoreIf

import java.util.concurrent.CountDownLatch

import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.SECONDS
import static org.junit.Assert.assertEquals
import static org.junit.Assert.fail

class MongoQueryCursorSpecification extends OperationFunctionalSpecification {
    ConnectionSource connectionSource
    MongoQueryCursor<Document> cursor

    def setup() {
        for ( int i = 0; i < 10; i++) {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', i))
        }
        connectionSource = getBinding().getReadConnectionSource()
    }

    def cleanup() {
        if (cursor != null) {
            cursor.close()
        }
    }

    def 'server cursor should not be null'() {
        given:
        def firstBatch = executeQuery(2)

        when:
        cursor = new MongoQueryCursor<Document>(getNamespace(),
                                                firstBatch, 0, 0, new DocumentCodec(),
                                                connectionSource)

        then:
        cursor.getServerCursor() != null
    }

    def 'test server address'() {
        given:
        def firstBatch = executeQuery()

        when:
        cursor = new MongoQueryCursor<Document>(getNamespace(),
                                                firstBatch, 0, 0, new DocumentCodec(),
                                                connectionSource)
        then:
        cursor.getServerAddress() != null
    }

    def 'should get Exceptions for operations on the cursor after closing'() {
        given:
        def firstBatch = executeQuery()

        cursor = new MongoQueryCursor<Document>(getNamespace(),
                                                firstBatch, 0, 0, new DocumentCodec(),
                                                connectionSource)

        when:
        cursor.close()
        cursor.close()

        and:
        cursor.next()

        then:
        thrown(IllegalStateException)

        when:
        cursor.hasNext()

        then:
        thrown(IllegalStateException)

        when:
        cursor.getServerCursor()

        then:
        thrown(IllegalStateException)
    }

    def 'should throw an Exception when going off the end'() {
        given:
        def firstBatch = executeQuery(1)

        cursor = new MongoQueryCursor<Document>(getNamespace(),
                                                firstBatch, 2, 0, new DocumentCodec(),
                                                connectionSource)
        when:
        cursor.next()
        cursor.next()
        cursor.next()

        then:
        thrown(NoSuchElementException)
    }

    def 'test normal exhaustion'() {
        given:
        def firstBatch = executeQuery()

        cursor = new MongoQueryCursor<Document>(getNamespace(),
                                                firstBatch, 0, 0, new DocumentCodec(),
                                                connectionSource)

        when:
        int i = 0
        while (cursor.hasNext()) {
            cursor.next()
            i++
        }

        then:
        i == 10
    }

    def 'test limit exhaustion'() {
        given:
        def firstBatch = executeQuery(5)

        cursor = new MongoQueryCursor<Document>(getNamespace(),
                                                firstBatch, 5, 0, new DocumentCodec(),
                                                connectionSource)

        when:
        int i = 0
        while (cursor.hasNext()) {
            cursor.next()
            i++
        }

        then:
        i == 5
    }

    def 'test remove'() {
        given:
        def firstBatch = executeQuery()

        cursor = new MongoQueryCursor<Document>(getNamespace(),
                                                firstBatch, 0, 0, new DocumentCodec(),
                                                connectionSource)

        when:
        cursor.remove()

        then:
        thrown(UnsupportedOperationException)
    }

    def 'test sizes and num get mores'() {
        given:
        def firstBatch = executeQuery(2)

        when:
        cursor = new MongoQueryCursor<Document>(getNamespace(),
                                                firstBatch, 0, 2, new DocumentCodec(),
                                                connectionSource)

        then:
        cursor.getNumGetMores() == 0
        cursor.getSizes().size() == 1
        cursor.getSizes().get(0) == 2

        when:
        cursor.next()
        cursor.next()
        cursor.next()

        then:
        cursor.getNumGetMores() == 1
        cursor.getSizes().size() == 2
        cursor.getSizes().get(1) == 2

        when:
        cursor.next()
        cursor.next()

        then:
        cursor.getNumGetMores() == 2
        cursor.getSizes().size() == 3
        cursor.getSizes().get(2) == 2
    }

    @Category(Slow)
    def 'test tailable'() {
        collectionHelper.create(collectionName, new CreateCollectionOptions().capped(true).sizeInBytes(1000))
        collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 1).append('ts', new BsonTimestamp(5, 0)))
        def firstBatch = executeQueryProtocol(getQueryProtocol(new BsonDocument('ts', new BsonDocument('$gte', new BsonTimestamp(5, 0))), 2)
                                                      .tailableCursor(true).awaitData(true))

        when:
        cursor = new MongoQueryCursor<Document>(getNamespace(),
                                                firstBatch, 0, 2, new DocumentCodec(),
                                                connectionSource)

        then:
        cursor.hasNext()
        cursor.next().get('_id') == 1

        when:
        def latch = new CountDownLatch(1)
        new Thread(new Runnable() {
            @Override
            void run() {
                try {
                    Thread.sleep(500)
                    MongoQueryCursorSpecification.this.collectionHelper
                                                 .insertDocuments(new DocumentCodec(),
                                                                  new Document('_id', 2).append('ts', new BsonTimestamp(6, 0)))
                } catch (ignored) {
                }
                latch.countDown()
            }
        }).start()

        // Note: this test is racy.
        // The sleep above does not guarantee that we're testing what we're trying to, which is the loop in the hasNext() method.
        then:
        cursor.hasNext()
        cursor.next().get('_id') == 2

        cleanup:
        latch.await(5, SECONDS)
    }

    @Category(Slow)
    def 'test tailable interrupt'() throws InterruptedException {
        collectionHelper.create(collectionName, new CreateCollectionOptions().capped(true).sizeInBytes(1000))
        collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', 1))

        def firstBatch = executeQueryProtocol(getQueryProtocol(new BsonDocument('ts', new BsonDocument('$gte', new BsonTimestamp(5, 0))), 2)
                                                      .tailableCursor(true).awaitData(true))

        when:
        cursor = new MongoQueryCursor<Document>(getNamespace(),
                                                firstBatch, 0, 2, new DocumentCodec(),
                                                connectionSource)

        CountDownLatch latch = new CountDownLatch(1)
        //TODO: there might be a more Groovy-y way to do this, may be no need to hack into an array?
        List<Boolean> success = []
        Thread t = new Thread(new Runnable() {
            @Override
            void run() {
                try {
                    cursor.next()
                    cursor.next()
                } catch (ignored) {
                    success.add(true)
                } finally {
                    latch.countDown()
                }
            }
        })
        t.start()
        Thread.sleep(1000)  // Note: this is racy, as where the interrupted exception is actually thrown from depends on timing.
        t.interrupt()
        latch.await()

        then:
        !success.isEmpty()
    }

    // 2.2 does not properly detect cursor not found, so ignoring
    @IgnoreIf({ isSharded() && !serverVersionAtLeast([2, 4, 0]) })
    @Category(Slow)
    def 'should kill cursor if limit is reached on initial query'() throws InterruptedException {
        given:
        def firstBatch = executeQuery(5)

        cursor = new MongoQueryCursor<Document>(getNamespace(),
                                                firstBatch, 5, 0, new DocumentCodec(),
                                                connectionSource)

        ServerCursor serverCursor = cursor.getServerCursor()
        Thread.sleep(1000) //Note: waiting for some time for killCursor operation to be performed on a server.

        when:
        makeAdditionalGetMoreCall(serverCursor)

        then:
        thrown(MongoCursorNotFoundException)
    }

    @IgnoreIf({ isSharded() && !serverVersionAtLeast([2, 4, 0]) })
    // 2.2 does not properly detect cursor not found, so ignoring
    @Category(Slow)
    def 'should kill cursor if limit is reached on get more'() throws InterruptedException {
        given:
        def firstBatch = executeQuery(3)

        cursor = new MongoQueryCursor<Document>(getNamespace(),
                                                firstBatch, 5, 3, new DocumentCodec(),
                                                connectionSource)
        ServerCursor serverCursor = cursor.getServerCursor()

        cursor.next()
        cursor.next()
        cursor.next()
        cursor.next()

        Thread.sleep(1000) //Note: waiting for some time for killCursor operation to be performed on a server.
        when:
        makeAdditionalGetMoreCall(serverCursor)

        then:
        thrown(MongoCursorNotFoundException)
    }

    def 'test limit with get more'() {
        given:
        def firstBatch = executeQuery(2)

        when:
        cursor = new MongoQueryCursor<Document>(getNamespace(), firstBatch, 5, 2, new DocumentCodec(), connectionSource)

        then:
        cursor.next() != null
        cursor.next() != null
        cursor.next() != null
        cursor.next() != null
        cursor.next() != null
        !cursor.hasNext()
    }

    @Category(Slow)
    def 'test limit with large documents'() {
        given:
        char[] array = 'x' * 16000
        String bigString = new String(array)

        for (int i = 11; i < 1000; i++) {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', i).append('s', bigString))
        }

        def firstBatch = executeQuery()

        when:
        cursor = new MongoQueryCursor<Document>(getNamespace(), firstBatch, 300, 0, new DocumentCodec(), connectionSource)

        then:
        for (int i = 0; i < 300; i++) {
           cursor.hasNext()
           cursor.next() != null
        }
        !cursor.hasNext()
    }

    def 'test normal loop with get more'() {
        given:
        def firstBatch = executeQuery(2)

        when:
        cursor = new MongoQueryCursor<Document>(getNamespace(),
                                                firstBatch, 0, 2, new DocumentCodec(),
                                                connectionSource)

        then:
        int i = 0
        while (cursor.hasNext()) {
            Document cur = cursor.next()
            i++
            cur.get('_id') == i
        }
        i == 10
        !cursor.hasNext()
    }

    def 'test next without has next with get more'() {
        given:
        def firstBatch = executeQuery(2)

        when:
        cursor = new MongoQueryCursor<Document>(getNamespace(),
                                                firstBatch, 0, 2, new DocumentCodec(),
                                                connectionSource)

        then:
        for (int i = 0; i < 10; i++) {
            cursor.next()
        }
        !cursor.hasNext()
        !cursor.hasNext()

        when:
        cursor.next()

        then:
        thrown(NoSuchElementException)
    }

    // 2.2 does not properly detect cursor not found, so ignoring
    @IgnoreIf({ isSharded() && !serverVersionAtLeast([2, 4, 0]) })
    def 'should throw cursor not found exception'() {
        given:
        def firstBatch = executeQuery(2)

        when:
        cursor = new MongoQueryCursor<Document>(getNamespace(),
                                                firstBatch, 0, 2, new DocumentCodec(),
                                                connectionSource)

        def connection = connectionSource.getConnection()
        new KillCursorProtocol(asList(cursor.getServerCursor())).execute(connection)
        connection.release()
        cursor.next()
        cursor.next()
        then:
        try {
            cursor.next()
        } catch (MongoCursorNotFoundException e) {
            assertEquals(cursor.getServerCursor().getId(), e.getCursorId())
            assertEquals(cursor.getServerCursor().getAddress(), e.getServerAddress())
        } catch (ignored) {
            fail()
        }
    }

    private QueryResult<Document> executeQuery() {
        executeQuery(0)
    }

    private QueryResult<Document> executeQuery(int numToReturn) {
        executeQueryProtocol(getQueryProtocol(new BsonDocument(), numToReturn))
    }

    private QueryResult<Document> executeQueryProtocol(QueryProtocol<Document> protocol) {
        def connection = connectionSource.getConnection()
        try {
            protocol.execute(connection)
        } finally {
            connection.release();
        }
    }

    private QueryProtocol<Document> getQueryProtocol(final BsonDocument query, final int numberToReturn) {
        new QueryProtocol<Document>(getNamespace(), 0, numberToReturn, query, null, new DocumentCodec());
    }

    private void makeAdditionalGetMoreCall(ServerCursor serverCursor) {
        def connection = connectionSource.getConnection()
        try {
            new GetMoreProtocol<Document>(getNamespace(), new GetMore(serverCursor, 1, 1, 1), new DocumentCodec()).execute(connection)
        } finally {
            connection.release()
        }
    }
}
