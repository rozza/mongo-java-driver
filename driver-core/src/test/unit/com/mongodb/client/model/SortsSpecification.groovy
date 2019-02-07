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


import org.bson.codecs.BsonCodecProvider
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.ValueCodecProvider
import spock.lang.Specification

import static com.mongodb.client.model.BsonTestHelper.toBson
import static com.mongodb.client.model.Sorts.ascending
import static com.mongodb.client.model.Sorts.descending
import static com.mongodb.client.model.Sorts.metaTextScore
import static com.mongodb.client.model.Sorts.orderBy
import static org.bson.BsonDocument.parse
import static org.bson.codecs.configuration.CodecRegistries.fromProviders

class SortsSpecification extends Specification {
    def registry = fromProviders([new BsonValueCodecProvider(), new ValueCodecProvider(), new BsonCodecProvider()])

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

    def 'metaTextScore'() {
        expect:
        toBson(metaTextScore('x'), direct) == parse('{x : {$meta : "textScore"}}')

        where:
        direct << [true, false]
    }

    def 'orderBy'() {
        expect:
        toBson(orderBy([ascending('x'), descending('y')]), direct) == parse('{x : 1, y : -1}')
        toBson(orderBy(ascending('x'), descending('y')), direct) == parse('{x : 1, y : -1}')
        toBson(orderBy(ascending('x'), descending('y'), descending('x')), direct) == parse('{y : -1, x : -1}')
        toBson(orderBy(ascending('x', 'y'), descending('a', 'b')), direct) == parse('{x : 1, y : 1, a : -1, b : -1}')

        where:
        direct << [true, false]
    }

    def 'should create string representation for simple sorts'() {
        expect:
        ascending('x', 'y').toString() == '{"x": 1, "y": 1}'
        descending('x', 'y').toString() == '{"x": -1, "y": -1}'
        metaTextScore('x').toString() == '{"x": {"$meta": "textScore"}}'
    }

    def 'should create string representation for compound sorts'() {
        expect:
        orderBy(ascending('x', 'y'), descending('a', 'b')).toString() ==
                'Compound Sort{sorts=[{"x": 1, "y": 1}, {"a": -1, "b": -1}]}'
    }
}
