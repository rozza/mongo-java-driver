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

package com.mongodb.async.rx.client

import org.mongodb.Document

import static Fixture.getAsList
import static Fixture.getMongoClient

class ClientAdministrationSpecification extends FunctionalSpecification {

    def 'should return the database name in getDatabaseNames'() {
        when:
        collection.insert(['_id': 1] as Document)
        def client = getMongoClient()

        then:
        getAsList(client.getDatabaseNames()).contains(databaseName)
    }

}
