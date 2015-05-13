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

package com.mongodb.async.client

import com.mongodb.MongoException
import com.mongodb.async.AsyncBatchCursor
import spock.lang.Specification

import static com.mongodb.async.client.SubscriptionHelpers.subscribeToMongoIterable

class MongoIterableSubscriptionSpecification extends Specification {

    def 'should do nothing until data is requested'() {
        given:
        def iterable = Mock(MongoIterable)
        def observer = new TestObserver()

        when:
        subscribeToMongoIterable(iterable, observer)

        then:
        0 * iterable.batchCursor(_)

        when:
        observer.requestMore(1)

        then:
        1 * iterable.batchCursor(_)
    }

    def 'should call batchCursor.next when requested data is more than queued data'() {
        given:
        def mongoIterable = getMongoIterable()
        def observer = new TestObserver()

        when:
        subscribeToMongoIterable(mongoIterable, observer)

        then:
        0 * mongoIterable.batchCursor(_)

        when:
        observer.requestMore(2)

        then:
        1 * mongoIterable.batchSize(2)
        observer.assertReceivedOnNext([1, 2])

        when:
        observer.requestMore(3)

        then:
        observer.assertNoErrors()
        observer.assertReceivedOnNext([1, 2, 3, 4])
        observer.assertTerminalEvent()
    }

    def 'should call onComplete after cursor has completed and all onNext values requested'() {
        given:
        def mongoIterable = getMongoIterable()
        def observer = new TestObserver()
        subscribeToMongoIterable(mongoIterable, observer)

        when:
        observer.requestMore(10)

        then:
        observer.assertNoErrors()
        observer.assertReceivedOnNext([1, 2, 3, 4])
        observer.assertTerminalEvent()
    }

    def 'should throw an error if request is less than 1'() {
        given:
        def observer = new TestObserver()
        subscribeToMongoIterable(Stub(MongoIterable), observer)

        when:
        observer.requestMore(0)

        then:
        thrown IllegalArgumentException
    }

    def 'should not be unsubscribed unless unsubscribed is called'() {
        given:
        def mongoIterable = getMongoIterable()
        def observer = new TestObserver()
        subscribeToMongoIterable(mongoIterable, observer)

        when:
        observer.requestMore(1)

        then:
        observer.assertSubscribed()

        when:
        observer.requestMore(5)

        then: // check that the observer is finished
        observer.assertSubscribed()
        observer.assertNoErrors()
        observer.assertReceivedOnNext([1, 2, 3, 4])
        observer.assertTerminalEvent()

        when: // unsubscribe
        observer.getSubscription().unsubscribe()

        then: // check the subscriber is unsubscribed
        observer.assertUnsubscribed()
    }

    def 'should close the batchCursor when unsubscribe is called'() {
        given:
        def cursor = getCursor()
        def observer = new TestObserver()
        subscribeToMongoIterable(getMongoIterable(cursor), observer)

        when:
        observer.requestMore(1)

        then:
        observer.assertSubscribed()

        when:
        observer.getSubscription().unsubscribe()

        then:
        1 * cursor.close()
        observer.assertNoErrors()
        observer.assertReceivedOnNext([1])
        observer.assertUnsubscribed()
    }

    def 'should not call onNext after unsubscribe is called'() {
        given:
        def cursor = getCursor()
        def observer = new TestObserver()
        subscribeToMongoIterable(getMongoIterable(cursor), observer)

        when:
        observer.requestMore(1)
        observer.getSubscription().unsubscribe()

        then:
        observer.assertUnsubscribed()
        observer.assertReceivedOnNext([1])

        when:
        observer.requestMore(10)

        then:
        0 * cursor.next(_)
        observer.assertNoErrors()
        observer.assertReceivedOnNext([1])
        observer.assertUnsubscribed()
    }

    def 'should not call onComplete after unsubscribe is called'() {
        given:
        def cursor = getCursor()
        def observer = new TestObserver()
        subscribeToMongoIterable(getMongoIterable(cursor), observer)

        when:
        observer.requestMore(1)
        observer.getSubscription().unsubscribe()

        then:
        observer.assertUnsubscribed()
        observer.assertNoTerminalEvent()
        observer.assertReceivedOnNext([1])
    }

    def 'should not call onError after unsubscribe is called'() {
        given:
        def observer = new TestObserver(new Observer() {
            @Override
            void onSubscribe(final Subscription s) {
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
        subscribeToMongoIterable(getMongoIterable(), observer)

        when:
        observer.requestMore(1)
        observer.getSubscription().unsubscribe()

        then:
        observer.assertUnsubscribed()
        observer.assertNoTerminalEvent()
        observer.assertReceivedOnNext([1])

        when:
        observer.requestMore(5)

        then:
        observer.assertNoTerminalEvent()
        observer.assertReceivedOnNext([1])
    }

    def 'should call onError if onNext causes an Error'() {
        given:
        def observer = new TestObserver(new Observer() {
            @Override
            void onSubscribe(final Subscription s) {
            }

            @Override
            void onNext(final Object t) {
                throw new MongoException('Failure')
            }

            @Override
            void onError(final Throwable e) {
            }

            @Override
            void onComplete() {
            }
        })
        subscribeToMongoIterable(getMongoIterable(), observer)

        when:
        observer.requestMore(1)

        then:
        observer.assertTerminalEvent()
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
}
