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

package org.mongodb.async
import org.mongodb.Document
import org.mongodb.MongoException
import org.mongodb.MongoFuture
import org.mongodb.WriteResult
import org.mongodb.connection.SingleResultCallback

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
            database.executeCommand(new Document('ping', 1)).get()
            database.requestStart()
            try {
                database.executeCommand(new Document('ping', 1)).get()
            } finally {
                database.requestDone()
            }
        } finally {
            database.requestDone()
        }

        then:
        notThrown Exception
    }

    def 'should use the same connection'() {
        given:
        List<Document> expectedDocs = []

        when:
        database.requestStart()
        1000.times {
            Document doc = new Document('_id', it)
            expectedDocs.add(doc)
            collection.insert(doc).register(new SingleResultCallback<WriteResult>() {
                @Override
                void onResult(final WriteResult result, final MongoException e) {
                }
            })
        }
        MongoFuture<Long> countFuture = collection.find(new Document()).count()
        countFuture.register(new SingleResultCallback<Long>() {
            @Override
            void onResult(final Long result, final MongoException e) {
            }
        })
        database.requestDone()

        then:
        countFuture.get() == 1000
        collection.find(new Document()).into([]).get() == expectedDocs
    }
}
