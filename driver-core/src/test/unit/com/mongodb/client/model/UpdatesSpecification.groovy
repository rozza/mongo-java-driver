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
import static com.mongodb.client.model.Updates.addEachToSet
import static com.mongodb.client.model.Updates.addToSet
import static com.mongodb.client.model.Updates.bitwiseAnd
import static com.mongodb.client.model.Updates.bitwiseOr
import static com.mongodb.client.model.Updates.bitwiseXor
import static com.mongodb.client.model.Updates.combine
import static com.mongodb.client.model.Updates.currentDate
import static com.mongodb.client.model.Updates.currentTimestamp
import static com.mongodb.client.model.Updates.inc
import static com.mongodb.client.model.Updates.max
import static com.mongodb.client.model.Updates.min
import static com.mongodb.client.model.Updates.mul
import static com.mongodb.client.model.Updates.popFirst
import static com.mongodb.client.model.Updates.popLast
import static com.mongodb.client.model.Updates.pull
import static com.mongodb.client.model.Updates.pullAll
import static com.mongodb.client.model.Updates.pullByFilter
import static com.mongodb.client.model.Updates.push
import static com.mongodb.client.model.Updates.pushEach
import static com.mongodb.client.model.Updates.rename
import static com.mongodb.client.model.Updates.set
import static com.mongodb.client.model.Updates.setOnInsert
import static com.mongodb.client.model.Updates.unset
import static org.bson.BsonDocument.parse

class UpdatesSpecification extends Specification {

    def 'should render $set'() {
        expect:
        toBson(set('x', 1), direct) == parse('{$set : { x : 1} }')
        toBson(set('x', null), direct) == parse('{$set : { x : null } }')

        where:
        direct << [true, false]
    }

    def 'should render $setOnInsert'() {
        expect:
        toBson(setOnInsert('x', 1), direct) == parse('{$setOnInsert : { x : 1} }')
        toBson(setOnInsert('x', null), direct) == parse('{$setOnInsert : { x : null } }')
        toBson(setOnInsert(parse('{ a : 1, b: "two"}')), direct) == parse('{$setOnInsert : {a: 1, b: "two"} }')

        when:
        toBson(setOnInsert(null), direct)

        then:
        thrown IllegalArgumentException

        where:
        direct << [true, false]
    }

    def 'should render $unset'() {
        expect:
        toBson(unset('x'), direct) == parse('{$unset : { x : ""} }')

        where:
        direct << [true, false]
    }

    def 'should render $rename'() {
        expect:
        toBson(rename('x', 'y'), direct) == parse('{$rename : { x : "y"} }')

        where:
        direct << [true, false]
    }

    def 'should render $inc'() {
        expect:
        toBson(inc('x', 1), direct) == parse('{$inc : { x : 1} }')
        toBson(inc('x', 5L), direct) == parse('{$inc : { x : {$numberLong : "5"}} }')
        toBson(inc('x', 3.4d), direct) == parse('{$inc : { x : 3.4} }')

        where:
        direct << [true, false]
    }

    def 'should render $mul'() {
        expect:
        toBson(mul('x', 1), direct) == parse('{$mul : { x : 1} }')
        toBson(mul('x', 5L), direct) == parse('{$mul : { x : {$numberLong : "5"}} }')
        toBson(mul('x', 3.4d), direct) == parse('{$mul : { x : 3.4} }')

        where:
        direct << [true, false]
    }

    def 'should render $min'() {
        expect:
        toBson(min('x', 42), direct) == parse('{$min : { x : 42} }')

        where:
        direct << [true, false]
    }

    def 'should render $max'() {
        expect:
        toBson(max('x', 42), direct) == parse('{$max : { x : 42} }')

        where:
        direct << [true, false]
    }

    def 'should render $currentDate'() {
        expect:
        toBson(currentDate('x'), direct) == parse('{$currentDate : { x : true} }')
        toBson(currentTimestamp('x'), direct) == parse('{$currentDate : { x : {$type : "timestamp"} } }')

        where:
        direct << [true, false]
    }

    def 'should render $addToSet'() {
        expect:
        toBson(addToSet('x', 1), direct) == parse('{$addToSet : { x : 1} }')
        toBson(addEachToSet('x', [1, 2, 3]), direct) == parse('{$addToSet : { x : { $each : [1, 2, 3] } } }')

        where:
        direct << [true, false]
    }

    def 'should render $push'() {
        expect:
        toBson(push('x', 1), direct) == parse('{$push : { x : 1} }')
        toBson(pushEach('x', [1, 2, 3], new PushOptions()), direct) == parse('{$push : { x : { $each : [1, 2, 3] } } }')
        toBson(pushEach('x', [parse('{score : 89}'), parse('{score : 65}')],
                        new PushOptions().position(0).slice(3).sortDocument(parse('{score : -1}'))), direct) ==
        parse('{$push : { x : { $each : [{score : 89}, {score : 65}], $position : 0, $slice : 3, $sort : { score : -1 } } } }')

        toBson(pushEach('x', [89, 65],
                        new PushOptions().position(0).slice(3).sort(-1)), direct) ==
        parse('{$push : { x : { $each : [89, 65], $position : 0, $slice : 3, $sort : -1 } } }')

        where:
        direct << [true, false]
    }

    def 'should render "$pull'() {
        expect:
        toBson(pull('x', 1), direct) == parse('{$pull : { x : 1} }')
        toBson(pullByFilter(Filters.gte('x', 5)), direct) == parse('{$pull : { x : { $gte : 5 }} }')

        where:
        direct << [true, false]
    }

    def 'should render "$pullAll'() {
        expect:
        toBson(pullAll('x', []), direct) == parse('{$pullAll : { x : []} }')
        toBson(pullAll('x', [1, 2, 3]), direct) == parse('{$pullAll : { x : [1, 2, 3]} }')

        where:
        direct << [true, false]
    }

    def 'should render $pop'() {
        expect:
        toBson(popFirst('x'), direct) == parse('{$pop : { x : -1} }')
        toBson(popLast('x'), direct) == parse('{$pop : { x : 1} }')

        where:
        direct << [true, false]
    }

    def 'should render $bit'() {
        expect:
        toBson(bitwiseAnd('x', 5), direct) == parse('{$bit : { x : {and : 5} } }')
        toBson(bitwiseAnd('x', 5L), direct) == parse('{$bit : { x : {and : {$numberLong : "5"} } } }')
        toBson(bitwiseOr('x', 5), direct) == parse('{$bit : { x : {or : 5} } }')
        toBson(bitwiseOr('x', 5L), direct) == parse('{$bit : { x : {or : {$numberLong : "5"} } } }')
        toBson(bitwiseXor('x', 5), direct) == parse('{$bit : { x : {xor : 5} } }')
        toBson(bitwiseXor('x', 5L), direct) == parse('{$bit : { x : {xor : {$numberLong : "5"} } } }')

        where:
        direct << [true, false]
    }

    def 'should combine updates'() {
        expect:
        toBson(combine(set('x', 1)), direct) == parse('{$set : { x : 1} }')
        toBson(combine(set('x', 1), set('y', 2)), direct) == parse('{$set : { x : 1, y : 2} }')
        toBson(combine(set('x', 1), set('x', 2)), direct) == parse('{$set : { x : 2} }')
        toBson(combine(set('x', 1), inc('z', 3), set('y', 2), inc('a', 4)), direct) == parse('''{
                                                                                          $set : { x : 1, y : 2},
                                                                                          $inc : { z : 3, a : 4}}
                                                                                        }''')

        toBson(combine(combine(set('x', 1))), direct) == parse('{$set : { x : 1} }')
        toBson(combine(combine(set('x', 1), set('y', 2))), direct) == parse('{$set : { x : 1, y : 2} }')
        toBson(combine(combine(set('x', 1), set('x', 2))), direct) == parse('{$set : { x : 2} }')

        toBson(combine(combine(set('x', 1), inc('z', 3), set('y', 2), inc('a', 4))), direct) == parse('''{
                                                                                                   $set : { x : 1, y : 2},
                                                                                                   $inc : { z : 3, a : 4}
                                                                                                  }''')

        where:
        direct << [true, false]
    }

    def 'should create string representation for simple updates'() {
        expect:
        set('x', 1).toString() == 'Update{fieldName=\'x\', operator=\'$set\', value=1}'

        where:
        direct << [true, false]
    }

    def 'should create string representation for with each update'() {
        expect:
        addEachToSet('x', [1, 2, 3]).toString() == 'Each Update{fieldName=\'x\', operator=\'$addToSet\', values=[1, 2, 3]}'
    }

    def 'should create string representation for push each update'() {
        expect:
        pushEach('x', [89, 65], new PushOptions().position(0).slice(3).sort(-1)).toString() ==
                'Each Update{fieldName=\'x\', operator=\'$push\', values=[89, 65], ' +
                'options=Push Options{position=0, slice=3, sort=-1}}'
        pushEach('x', [89, 65], new PushOptions().position(0).slice(3).sortDocument(parse('{x : 1}'))).toString() ==
                'Each Update{fieldName=\'x\', operator=\'$push\', values=[89, 65], ' +
                'options=Push Options{position=0, slice=3, sortDocument={"x": 1}}}'
    }

    def 'should create string representation for pull all update'() {
        expect:
        pullAll('x', [1, 2, 3]).toString() == 'Update{fieldName=\'x\', operator=\'$pullAll\', value=[1, 2, 3]}'
    }

    def 'should create string representation for combined update'() {
        expect:
        combine(set('x', 1), inc('z', 3)).toString() ==
                'Updates{updates=[' +
                'Update{fieldName=\'x\', operator=\'$set\', value=1}, ' +
                'Update{fieldName=\'z\', operator=\'$inc\', value=3}]}'
    }
}
