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

import com.mongodb.Block
import com.mongodb.MongoException
import com.mongodb.async.SingleResultCallback
import spock.lang.Specification

import static com.mongodb.async.client.SubscriptionHelpers.subscribeToListBlock
import static com.mongodb.async.client.SubscriptionHelpers.subscribeToSingleItemBlock

class SingleResultCallbackSubscriptionSpecification extends Specification {

    def 'should do nothing until data is requested'() {
        given:
        def block = Mock(Block)
        def observer = new TestObserver()

        when:
        subscribeToListBlock(block, observer)

        then:
        0 * block.apply(_)

        when:
        observer.requestMore(1)

        then:
        1 * block.apply(_)
    }


    def 'should call onComplete after all data has been consumed'() {
        given:
        def block = getBlock()
        def observer = new TestObserver()
        subscribeToListBlock(block, observer)

        when:
        observer.requestMore(10)

        then:
        observer.assertNoErrors()
        observer.assertReceivedOnNext([1, 2, 3, 4])
        observer.assertTerminalEvent()
    }

    def 'should throw an error if request is less than 1'() {
        given:
        def block = getBlock()
        def observer = new TestObserver()
        subscribeToListBlock(block, observer)

        when:
        observer.requestMore(0)

        then:
        thrown IllegalArgumentException
    }

    def 'should not be unsubscribed unless unsubscribed is called'() {
        given:
        def block = getBlock()
        def observer = new TestObserver()
        subscribeToListBlock(block, observer)

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

    def 'should not call onNext after unsubscribe is called'() {
        given:
        def block = getBlock()
        def observer = new TestObserver()
        subscribeToListBlock(block, observer)

        when:
        observer.requestMore(1)
        observer.getSubscription().unsubscribe()

        then:
        observer.assertUnsubscribed()
        observer.assertReceivedOnNext([1])

        when:
        observer.requestMore(1)

        then:
        observer.assertNoErrors()
        observer.assertReceivedOnNext([1])
        observer.assertUnsubscribed()
    }

    def 'should not call onComplete after unsubscribe is called'() {
        given:
        def block = getBlock()
        def observer = new TestObserver()
        subscribeToListBlock(block, observer)

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
        def block = getBlock()
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
        subscribeToListBlock(block, observer)

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
        def block = getBlock()
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
        subscribeToListBlock(block, observer)

        when:
        observer.requestMore(1)

        then:
        observer.assertTerminalEvent()
    }

    def 'should do nothing until data is requested from single item block'() {
        given:
        def block = Mock(Block)
        def observer = new TestObserver()

        when:
        subscribeToSingleItemBlock(block, observer)

        then:
        0 * block.apply(_)

        when:
        observer.requestMore(1)

        then:
        1 * block.apply(_)
    }


    def 'should work with single item blocks and call onComplete after all data has been consumed'() {
        given:
        def block = getSingleBlock()
        def observer = new TestObserver()
        subscribeToSingleItemBlock(block, observer)

        when:
        observer.requestMore(10)

        then:
        observer.assertNoErrors()
        observer.assertReceivedOnNext([1])
        observer.assertTerminalEvent()
    }

    def 'should not flatten lists in single item blocks'() {
        given:
        def expected = [1, 2, 3, 4]
        def block = getSingleBlock(expected)
        def observer = new TestObserver()
        subscribeToSingleItemBlock(block, observer)

        when:
        observer.requestMore(10)

        then:
        observer.assertNoErrors()
        observer.assertReceivedOnNext([expected])
        observer.assertTerminalEvent()
    }

    def getSingleBlock() {
        getSingleBlock(1)
    }

    def getSingleBlock(results) {
        new Block<SingleResultCallback<Integer>>() {

            @Override
            void apply(final SingleResultCallback<Integer> callback) {
                callback.onResult(results, null);
            }
        }
    }

    def getBlock() {
        new Block<SingleResultCallback<List<Integer>>>() {

            @Override
            void apply(final SingleResultCallback<List<Integer>> callback) {
                callback.onResult([1, 2, 3, 4], null);
            }
        }
    }
}
