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
import static com.mongodb.client.model.Filters.and
import static com.mongodb.client.model.Filters.eq
import static com.mongodb.client.model.Projections.computed
import static com.mongodb.client.model.Projections.elemMatch
import static com.mongodb.client.model.Projections.exclude
import static com.mongodb.client.model.Projections.excludeId
import static com.mongodb.client.model.Projections.fields
import static com.mongodb.client.model.Projections.include
import static com.mongodb.client.model.Projections.metaTextScore
import static com.mongodb.client.model.Projections.slice
import static org.bson.BsonDocument.parse

class ProjectionsSpecification extends Specification {

    def 'include'() {
        expect:
        toBson(include('x'), direct) == parse('{x : 1}')
        toBson(include('x', 'y'), direct) == parse('{x : 1, y : 1}')
        toBson(include(['x', 'y']), direct) == parse('{x : 1, y : 1}')
        toBson(include(['x', 'y', 'x']), direct) == parse('{y : 1, x : 1}')

        where:
        direct << [true, false]
    }

    def 'exclude'() {
        expect:
        toBson(exclude('x'), direct) == parse('{x : 0}')
        toBson(exclude('x', 'y'), direct) == parse('{x : 0, y : 0}')
        toBson(exclude(['x', 'y']), direct) == parse('{x : 0, y : 0}')

        where:
        direct << [true, false]
    }

    def 'excludeId'() {
        expect:
        toBson(excludeId(), direct) == parse('{_id : 0}')

        where:
        direct << [true, false]
    }

    def 'firstElem'() {
        expect:
        toBson(elemMatch('x'), direct) == parse('{"x.$" : 1}')

        where:
        direct << [true, false]
    }

    def 'elemMatch'() {
        expect:
        toBson(elemMatch('x', and(eq('y', 1), eq('z', 2))), direct) == parse('{x : {$elemMatch : {y : 1, z : 2}}}')

        where:
        direct << [true, false]
    }

    def 'slice'() {
        expect:
        toBson(slice('x', 5), direct) == parse('{x : {$slice : 5}}')
        toBson(slice('x', 5, 10), direct) == parse('{x : {$slice : [5, 10]}}')

        where:
        direct << [true, false]
    }

    def 'metaTextScore'() {
        expect:
        toBson(metaTextScore('x'), direct) == parse('{x : {$meta : "textScore"}}')

        where:
        direct << [true, false]
    }

    def 'computed'() {
        expect:
        toBson(computed('c', '$y'), direct) == parse('{c : "$y"}')

        where:
        direct << [true, false]
    }

    def 'combine fields'() {
        expect:
        toBson(fields(include('x', 'y'), exclude('_id')), direct) == parse('{x : 1, y : 1, _id : 0}')
        toBson(fields([include('x', 'y'), exclude('_id')]), direct) == parse('{x : 1, y : 1, _id : 0}')
        toBson(fields(include('x', 'y'), exclude('x')), direct) == parse('{y : 1, x : 0}')

        where:
        direct << [true, false]
    }

    def 'should create string representation for include and exclude'() {
        expect:
        include(['x', 'y', 'x']).toString() == '{"y": 1, "x": 1}'
        exclude(['x', 'y', 'x']).toString() == '{"y": 0, "x": 0}'
        excludeId().toString() == '{"_id": 0}'
    }

    def 'should create string representation for computed'() {
        expect:
        computed('c', '$y').toString() == 'Expression{name=\'c\', expression=$y}'
    }

    def 'should create string representation for elemMatch with filter'() {
        expect:
        elemMatch('x', and(eq('y', 1), eq('z', 2))).toString() ==
                'ElemMatch Projection{fieldName=\'x\', ' +
                'filter=And Filter{filters=[Filter{fieldName=\'y\', value=1}, Filter{fieldName=\'z\', value=2}]}}'
    }

    def 'should create string representation for fields'() {
        expect:
        fields(include('x', 'y'), exclude('_id')).toString() == 'Projections{projections=[{"x": 1, "y": 1}, {"_id": 0}]}'
    }
}
