/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.reactivestreams.client.internal

import com.mongodb.MongoException
import com.mongodb.internal.async.AsyncBatchCursor
import com.mongodb.internal.async.client.MongoIterable
import com.mongodb.reactivestreams.client.TestSubscriber
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import spock.lang.Specification

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class MongoIterableSubscriptionSpecification extends Specification {

    def 'should do nothing until data is requested'() {
        given:
        def mongoIterable = Mock(MongoIterable)
        def subscriber = new TestSubscriber()

        when:
        Publishers.publish(mongoIterable).subscribe(subscriber)

        then:
        0 * mongoIterable.batchCursor(_)

        when:
        subscriber.requestMore(1)

        then:
        1 * mongoIterable.batchCursor(_)
    }

    def 'should call batchCursor.next when requested data is more than queued data'() {
        given:
        def mongoIterable = getMongoIterable()
        def subscriber = new TestSubscriber()

        when:
        Publishers.publish(mongoIterable).subscribe(subscriber)

        then:
        0 * mongoIterable.batchCursor(_)

        when:
        subscriber.requestMore(2)

        then:
        1 * mongoIterable.batchSize(2)
        subscriber.assertReceivedOnNext([1, 2])

        when:
        subscriber.requestMore(3)

        then:
        subscriber.assertNoErrors()
        subscriber.assertReceivedOnNext([1, 2, 3, 4])
        subscriber.assertTerminalEvent()
    }

    def 'should call onComplete after cursor has completed and all onNext values requested'() {
        given:
        def mongoIterable = getMongoIterable()
        def executor = Executors.newFixedThreadPool(5)
        def subscriber = new TestSubscriber()
        Publishers.publish(mongoIterable).subscribe(subscriber)

        when:
        100.times { executor.submit { subscriber.requestMore(1) } }
        subscriber.requestMore(10)

        then:
        subscriber.assertNoErrors()
        subscriber.assertReceivedOnNext([1, 2, 3, 4])
        subscriber.assertTerminalEvent()

        cleanup:
        executor?.shutdown()
        executor?.awaitTermination(10, TimeUnit.SECONDS)
    }

    def 'should call onError if batchCursor returns an throwable in the callback'() {
        given:
        def subscriber = new TestSubscriber()
        def mongoIterable = Mock(MongoIterable) {
            1 * batchCursor(_) >> {
                it[0].onResult(null, new MongoException('failed'))
            }
        }
        Publishers.publish(mongoIterable).subscribe(subscriber)

        when:
        subscriber.requestMore(1)

        then:
        subscriber.assertErrored()
        subscriber.assertTerminalEvent()
    }

    def 'should call onError if batchCursor returns a null for the cursor in the callback'() {
        given:
        def subscriber = new TestSubscriber()
        def mongoIterable = Mock(MongoIterable) {
            1 * batchCursor(_) >> {
                it[0].onResult(null, null)
            }
        }
        Publishers.publish(mongoIterable).subscribe(subscriber)

        when:
        subscriber.requestMore(1)

        then:
        subscriber.assertErrored()
        subscriber.assertTerminalEvent()
    }

    def 'should call onError if batchCursor.next returns an throwable in the callback'() {
        given:
        def subscriber = new TestSubscriber()
        def mongoIterable = Mock(MongoIterable) {
            1 * batchCursor(_) >> {
                it[0].onResult(Mock(AsyncBatchCursor) {
                    next(_) >> { it[0].onResult(null, new MongoException('failed')) }
                }, null)
            }
        }
        Publishers.publish(mongoIterable).subscribe(subscriber)

        when:
        subscriber.requestMore(1)

        then:
        subscriber.assertErrored()
        subscriber.assertTerminalEvent()
    }

    def 'should set batchSize to 2 if request is passed 1'() {
        given:
        def subscriber = new TestSubscriber()
        def mockIterable = getMongoIterable()
        Publishers.publish(mockIterable).subscribe(subscriber)

        when:
        subscriber.requestMore(1)

        then:
        1 * mockIterable.batchSize(2)
    }

    def 'should set batchSize to Integer.MAX_VALUE if request is passed a bigger value'() {
        given:
        def subscriber = new TestSubscriber()
        def mockIterable = getMongoIterable()
        Publishers.publish(mockIterable).subscribe(subscriber)

        when:
        subscriber.requestMore(Long.MAX_VALUE)

        then:
        1 * mockIterable.batchSize(Integer.MAX_VALUE)
        subscriber.assertTerminalEvent()
    }

    def 'should set batchSize on the cursor to 2 if request is passed 1'() {
        given:
        def subscriber = new TestSubscriber()
        def cursor = getCursor()
        def mockIterable = getMongoIterable(cursor)
        Publishers.publish(mockIterable).subscribe(subscriber)

        when:
        subscriber.requestMore(2)
        subscriber.requestMore(1)
        subscriber.requestMore(100)

        then:
        1 * mockIterable.batchSize(2)
        2 * cursor.setBatchSize(2)
        subscriber.assertTerminalEvent()
    }

    def 'should set batchSize to Integer.MAX_VALUE  on the cursor if request is passed a bigger value'() {
        given:
        def subscriber = new TestSubscriber()
        def cursor = getCursor()
        def mockIterable = getMongoIterable(cursor)
        Publishers.publish(mockIterable).subscribe(subscriber)

        when:
        subscriber.requestMore(2)
        subscriber.requestMore(Long.MAX_VALUE)

        then:
        1 * mockIterable.batchSize(2)
        2 * cursor.setBatchSize(Integer.MAX_VALUE)
        subscriber.assertTerminalEvent()
    }

    def 'should use the set batchSize when configured on the mongoIterable'() {
        given:
        def subscriber = new TestSubscriber()
        def cursor = Mock(AsyncBatchCursor) {
            def cursorResults = [(1..3), (1..3), (1..3)]
            next(_) >> {
                it[0].onResult(cursorResults.isEmpty() ? null : cursorResults.remove(0), null)
            }
        }
        def mockIterable = getMongoIterable(cursor)
        _ * mockIterable.getBatchSize() >> { 3 }
        Publishers.publish(mockIterable).subscribe(subscriber)

        when:
        subscriber.getSubscription()
        subscriber.requestMore(4)

        then:
        1 * mockIterable.batchSize(3)
        2 * cursor.setBatchSize(3)

        when:
        subscriber.requestMore(Long.MAX_VALUE)

        then:
        2 * cursor.setBatchSize(3)
        subscriber.assertTerminalEvent()
    }

    def 'should use negative batchSize values when configured on the mongoIterable'() {
        given:
        def subscriber = new TestSubscriber()
        def cursor = Mock(AsyncBatchCursor) {
            def cursorResults = [(1..3)]
            next(_) >> {
                it[0].onResult(cursorResults.isEmpty() ? null : cursorResults.remove(0), null)
            }
        }
        def mockIterable = getMongoIterable(cursor)
        _ * mockIterable.getBatchSize() >> { -3 }
        Publishers.publish(mockIterable).subscribe(subscriber)

        when:
        subscriber.getSubscription()
        subscriber.requestMore(4)

        then:
        1 * mockIterable.batchSize(-3)
        2 * cursor.setBatchSize(-3)
        subscriber.assertTerminalEvent()
    }

    def 'should throw an error if request is less than 1'() {
        given:
        def subscriber = new TestSubscriber()
        Publishers.publish(Stub(MongoIterable)).subscribe(subscriber)

        when:
        subscriber.requestMore(0)

        then:
        subscriber.assertErrored()
    }

    def 'should not be unsubscribed unless unsubscribed is called'() {
        given:
        def mongoIterable = getMongoIterable()
        def subscriber = new TestSubscriber()
        Publishers.publish(mongoIterable).subscribe(subscriber)

        when:
        subscriber.requestMore(1)
        subscriber.requestMore(5)

        then: // check that the subscriber is finished
        subscriber.assertNoErrors()
        subscriber.assertReceivedOnNext([1, 2, 3, 4])
        subscriber.assertTerminalEvent()
    }

    def 'should close the batchCursor when unsubscribe is called'() {
        given:
        def cursor = getCursor()
        def subscriber = new TestSubscriber()
        Publishers.publish(getMongoIterable(cursor)).subscribe(subscriber)

        when:
        subscriber.requestMore(1)
        subscriber.getSubscription().cancel()

        then:
        1 * cursor.close()
        subscriber.assertNoErrors()
        subscriber.assertReceivedOnNext([1])
    }

    def 'should not call onNext after cancel is called'() {
        given:
        def cursor = getCursor()
        def subscriber = new TestSubscriber()
        Publishers.publish(getMongoIterable(cursor)).subscribe(subscriber)

        when:
        subscriber.requestMore(1)
        subscriber.getSubscription().cancel()

        then:
        subscriber.assertReceivedOnNext([1])

        when:
        subscriber.requestMore(10)

        then:
        0 * cursor.next(_)
        subscriber.assertNoErrors()
        subscriber.assertReceivedOnNext([1])
    }

    def 'should not call onComplete after unsubscribe is called'() {
        given:
        def cursor = getCursor()
        def subscriber = new TestSubscriber()
        Publishers.publish(getMongoIterable(cursor)).subscribe(subscriber)

        when:
        subscriber.requestMore(1)

        then:
        subscriber.assertNoTerminalEvent()
        subscriber.assertReceivedOnNext([1])
    }

    def 'should not call onError after unsubscribe is called'() {
        given:
        def subscriber = new TestSubscriber(new Subscriber() {
            @Override
            void onSubscribe(final Subscription subscription) {
            }

            @Override
            void onNext(final Object result) {
                if (result == 2) {
                    throw new MongoException('Failure')
                }
            }

            @Override
            void onError(final Throwable e) {
            }

            @Override
            void onComplete() {
            }
        })
        Publishers.publish(getMongoIterable()).subscribe(subscriber)

        when:
        subscriber.requestMore(1)
        subscriber.getSubscription().cancel()

        then:
        subscriber.assertNoTerminalEvent()
        subscriber.assertReceivedOnNext([1])

        when:
        subscriber.requestMore(5)

        then:
        subscriber.assertNoTerminalEvent()
        subscriber.assertReceivedOnNext([1])
    }

    def 'should call onError if onNext causes an Error'() {
        given:
        def subscriber = new TestSubscriber(new Subscriber() {
            @Override
            void onSubscribe(final Subscription subscription) {
            }

            @Override
            void onNext(final Object result) {
                throw new MongoException('Failure')
            }

            @Override
            void onError(final Throwable e) {
            }

            @Override
            void onComplete() {
            }
        })
        Publishers.publish(getMongoIterable()).subscribe(subscriber)

        when:
        subscriber.requestMore(1)

        then:
        notThrown(MongoException)
        subscriber.assertTerminalEvent()
        subscriber.assertErrored()
    }

    def 'should throw the exception if calling onComplete raises one'() {
        given:
        def subscriber = new TestSubscriber(new Subscriber() {
            @Override
            void onSubscribe(final Subscription subscription) {
            }

            @Override
            void onNext(final Object result) {
            }

            @Override
            void onError(final Throwable e) {
            }

            @Override
            void onComplete() {
                throw new MongoException('exception calling onComplete')
            }
        })
        Publishers.publish(getMongoIterable()).subscribe(subscriber)

        when:
        subscriber.requestMore(100)

        then:
        def ex = thrown(MongoException)
        ex.message == 'exception calling onComplete'
        subscriber.assertTerminalEvent()
        subscriber.assertNoErrors()
    }

    def 'should throw the exception if calling onError raises one'() {
        given:
        def subscriber = new TestSubscriber(new Subscriber() {
            @Override
            void onSubscribe(final Subscription subscription) {
            }

            @Override
            void onNext(final Object result) {
                throw new MongoException('fail')
            }

            @Override
            void onError(final Throwable e) {
                throw new MongoException('exception calling onError')
            }

            @Override
            void onComplete() {
            }
        })
        Publishers.publish(getMongoIterable()).subscribe(subscriber)

        when:
        subscriber.requestMore(1)

        then:
        def ex = thrown(MongoException)
        ex.message == 'exception calling onError'
        subscriber.assertTerminalEvent()
        subscriber.assertErrored()
    }

    def 'should call onError if MongoIterable errors'() {
        given:
        def subscriber = new TestSubscriber()
        Publishers.publish(getMongoIterable(getFailingCursor(failImmediately))).subscribe(subscriber)

        when:
        subscriber.requestMore(3)

        then:
        subscriber.assertTerminalEvent()
        subscriber.assertErrored()

        where:
        failImmediately << [true, false]
    }

    def getMongoIterable() {
        getMongoIterable(getCursor())
    }

    def getMongoIterable(AsyncBatchCursor cursor) {
        Mock(MongoIterable) {
            1 * batchCursor(_) >> {
                it[0].onResult(cursor, null)
            }
        }
    }

    def getCursor() {
        Mock(AsyncBatchCursor) {
            def cursorResults = [[1, 2], [3, 4]]
            next(_) >> {
                it[0].onResult(cursorResults.isEmpty() ? null : cursorResults.remove(0), null)
            }
        }
    }

    def getFailingCursor(boolean failImmediately) {
        Mock(AsyncBatchCursor) {
            def cursorResults = [[1, 2]]
            def hasSetBatchSize = failImmediately
            setBatchSize(_) >> {
                if (hasSetBatchSize) {
                    throw new MongoException('Failure')
                } else {
                    hasSetBatchSize = true
                }
            }
            next(_) >> {
                it[0].onResult(cursorResults.isEmpty() ? null : cursorResults.remove(0), null)
            }
        }
    }
}
