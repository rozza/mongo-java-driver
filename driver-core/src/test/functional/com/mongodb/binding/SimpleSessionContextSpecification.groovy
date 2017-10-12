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

package com.mongodb.binding

import org.bson.BsonDocument
import org.bson.BsonTimestamp
import spock.lang.Specification

class SimpleSessionContextSpecification extends Specification {


    def 'should set clusterTime'() {
        given:
        def newClusterTime = BsonDocument.parse('{time: 1}')

        when:
        def sessionContext = new SimpleSessionContext()

        then:
        sessionContext.getClusterTime() == null

        when:
        sessionContext.advanceClusterTime(newClusterTime)

        then:
        sessionContext.getClusterTime() == newClusterTime
    }

    def 'should have session'() {
        when:
        def sessionContext = new SimpleSessionContext()

        then:
        sessionContext.hasSession()
        sessionContext.getSessionId() != null
    }

    def 'should increment the transactionNumber'() {
        given:
        def sessionContext = new SimpleSessionContext()

        when:
        def txnNumber1 = sessionContext.advanceTransactionNumber()
        def txnNumber2 = sessionContext.advanceTransactionNumber()

        then:
        txnNumber2 > txnNumber1
    }

    def 'should support setting operationalTime'() {
        given:
        def sessionContext = new SimpleSessionContext()

        when:
        sessionContext.advanceOperationTime(new BsonTimestamp())

        then:
        notThrown(Throwable)
    }
}
