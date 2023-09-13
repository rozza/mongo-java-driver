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

package com.mongodb.internal.operation

import com.mongodb.MongoException
import com.mongodb.async.FutureResultCallback
import org.bson.Document
import spock.lang.Specification

class AsyncSingleBatchCursorSpecification extends Specification {

    def 'should work as expected'() {
        given:
        def cursor = new AsyncSingleBatchCursor<Document>(firstBatch, 0)

        when:
        def batch = nextBatch(cursor)

        then:
        batch == firstBatch

        then:
        nextBatch(cursor) == null

        when:
        nextBatch(cursor)

        then:
        thrown(MongoException)
    }

    def 'should not support setting batchsize'() {
        given:
        def cursor = new AsyncSingleBatchCursor<Document>(firstBatch, 0)

        when:
        cursor.setBatchSize(1)

        then:
        cursor.getBatchSize() == 0
    }


    List<Document> nextBatch(AsyncSingleBatchCursor cursor) {
        def futureResultCallback = new FutureResultCallback()
        cursor.next(futureResultCallback)
        futureResultCallback.get()
    }

    def firstBatch = [new Document('a', 1)]
}
