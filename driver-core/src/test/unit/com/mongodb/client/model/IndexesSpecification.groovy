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

package com.mongodb.client.model


import spock.lang.Specification

import static com.mongodb.client.model.BsonTestHelper.toBson
import static com.mongodb.client.model.Indexes.ascending
import static com.mongodb.client.model.Indexes.compoundIndex
import static com.mongodb.client.model.Indexes.descending
import static com.mongodb.client.model.Indexes.geo2d
import static com.mongodb.client.model.Indexes.geo2dsphere
import static com.mongodb.client.model.Indexes.geoHaystack
import static com.mongodb.client.model.Indexes.hashed
import static com.mongodb.client.model.Indexes.text
import static org.bson.BsonDocument.parse

class IndexesSpecification extends Specification {

    def 'ascending'() {
        expect:
        toBson(ascending('x'), direct) == parse('{x : 1}')
        toBson(ascending('x', 'y'), direct) == parse('{x : 1, y : 1}')
        toBson(ascending(['x', 'y']), direct) == parse('{x : 1, y : 1}')

        where:
        direct << [true, false]
    }

    def 'descending'() {
        expect:
        toBson(descending('x'), direct) == parse('{x : -1}')
        toBson(descending('x', 'y'), direct) == parse('{x : -1, y : -1}')
        toBson(descending(['x', 'y']), direct) == parse('{x : -1, y : -1}')

        where:
        direct << [true, false]
    }

    def 'geo2dsphere'() {
        expect:
        toBson(geo2dsphere('x'), direct) == parse('{x : "2dsphere"}')
        toBson(geo2dsphere('x', 'y'), direct) == parse('{x : "2dsphere", y : "2dsphere"}')
        toBson(geo2dsphere(['x', 'y']), direct) == parse('{x : "2dsphere", y : "2dsphere"}')

        where:
        direct << [true, false]
    }

    def 'geo2d'() {
        expect:
        toBson(geo2d('x'), direct) == parse('{x : "2d"}')

        where:
        direct << [true, false]
    }

    def 'geoHaystack'() {
        expect:
        toBson(geoHaystack('x', descending('b')), direct) == parse('{x : "geoHaystack", b: -1}')

        where:
        direct << [true, false]
    }

    def 'text'() {
        expect:
        toBson(text('x'), direct) == parse('{x : "text"}')
        toBson(text(), direct) == parse('{ "$**" : "text"}')

        where:
        direct << [true, false]
    }

    def 'hashed'() {
        expect:
        toBson(hashed('x'), direct) == parse('{x : "hashed"}')

        where:
        direct << [true, false]
    }

    def 'compoundIndex'() {
        expect:
        toBson(compoundIndex([ascending('x'), descending('y')]), direct) == parse('{x : 1, y : -1}')
        toBson(compoundIndex(ascending('x'), descending('y')), direct) == parse('{x : 1, y : -1}')
        toBson(compoundIndex(ascending('x'), descending('y'), descending('x')), direct) == parse('{y : -1, x : -1}')
        toBson(compoundIndex(ascending('x', 'y'), descending('a', 'b')), direct) == parse('{x : 1, y : 1, a : -1, b : -1}')

        where:
        direct << [true, false]
    }
}
