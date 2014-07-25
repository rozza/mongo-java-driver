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

package org.mongodb.async.rxjava

import org.mongodb.Document
import org.mongodb.WriteResult
import rx.functions.Action1

import java.util.concurrent.ConcurrentLinkedQueue

class DatabaseRequestSpecification extends FunctionalSpecification {

    def 'should not throw an exception when request done is called without calling requestStart'() {

        when:
        database.requestDone()

        then:
        notThrown Exception
    }

    def 'should release connection on last call to request done when nested'() {
        when:
        database.requestStart()
        try {
            database.executeCommand(new Document('ping', 1)).toBlockingObservable().first()
            database.requestStart()
            try {
                database.executeCommand(new Document('ping', 1)).toBlockingObservable().first()
            } finally {
                database.requestDone()
            }
        } finally {
            database.requestDone()
        }

        then:
        notThrown Exception
    }


    def 'should block only on the last operation'() {

        given:
        Queue<Integer> steps = new ConcurrentLinkedQueue<Integer>()

        when:

        database.requestStart()
        1000.times {
            collection.insert(new Document('_id', it)).subscribe(new Action1<WriteResult>() {
                @Override
                void call(final WriteResult result) {
                    steps.add(it)
                }
            })
        }

        rx.Observable<Long> countFuture = collection.find(new Document()).count()
        countFuture.subscribe(new Action1<Long>() {
            @Override
            void call(final Long result) {
                steps.add(1000)
            }
        })
        database.requestDone()

        then:
        countFuture.toBlockingObservable().first() == 1000
        steps.collect() == 0..1000
    }
}
