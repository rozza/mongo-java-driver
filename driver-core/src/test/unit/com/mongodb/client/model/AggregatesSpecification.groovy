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

import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.conversions.Bson
import spock.lang.Specification

import static BucketGranularity.R5
import static com.mongodb.client.model.Accumulators.addToSet
import static com.mongodb.client.model.Accumulators.avg
import static com.mongodb.client.model.Accumulators.first
import static com.mongodb.client.model.Accumulators.last
import static com.mongodb.client.model.Accumulators.max
import static com.mongodb.client.model.Accumulators.min
import static com.mongodb.client.model.Accumulators.push
import static com.mongodb.client.model.Accumulators.stdDevPop
import static com.mongodb.client.model.Accumulators.stdDevSamp
import static com.mongodb.client.model.Accumulators.sum
import static com.mongodb.client.model.AggregateOutStageOptions.Mode.INSERT_DOCUMENTS
import static com.mongodb.client.model.AggregateOutStageOptions.Mode.REPLACE_COLLECTION
import static com.mongodb.client.model.AggregateOutStageOptions.Mode.REPLACE_DOCUMENTS
import static com.mongodb.client.model.Aggregates.addFields
import static com.mongodb.client.model.Aggregates.bucket
import static com.mongodb.client.model.Aggregates.bucketAuto
import static com.mongodb.client.model.Aggregates.count
import static com.mongodb.client.model.Aggregates.facet
import static com.mongodb.client.model.Aggregates.graphLookup
import static com.mongodb.client.model.Aggregates.group
import static com.mongodb.client.model.Aggregates.limit
import static com.mongodb.client.model.Aggregates.lookup
import static com.mongodb.client.model.Aggregates.match
import static com.mongodb.client.model.Aggregates.out
import static com.mongodb.client.model.Aggregates.project
import static com.mongodb.client.model.Aggregates.replaceRoot
import static com.mongodb.client.model.Aggregates.sample
import static com.mongodb.client.model.Aggregates.skip
import static com.mongodb.client.model.Aggregates.sort
import static com.mongodb.client.model.Aggregates.sortByCount
import static com.mongodb.client.model.Aggregates.unwind
import static com.mongodb.client.model.BsonTestHelper.toBson
import static com.mongodb.client.model.Filters.eq
import static com.mongodb.client.model.Filters.expr
import static com.mongodb.client.model.Projections.computed
import static com.mongodb.client.model.Projections.fields
import static com.mongodb.client.model.Projections.include
import static com.mongodb.client.model.Sorts.ascending
import static com.mongodb.client.model.Sorts.descending
import static java.util.Arrays.asList
import static org.bson.BsonDocument.parse

class AggregatesSpecification extends Specification {
    def 'should render $addFields'() {
        expect:
        toBson(addFields(new Field('newField', null)), direct) == parse('{$addFields: {newField: null}}')
        toBson(addFields(new Field('newField', 'hello')), direct) == parse('{$addFields: {newField: "hello"}}')
        toBson(addFields(new Field('this', '$$CURRENT')), direct) == parse('{$addFields: {this: "$$CURRENT"}}')
        toBson(addFields(new Field('myNewField', new Document('c', 3)
                .append('d', 4))), direct) == parse('{$addFields: {myNewField: {c: 3, d: 4}}}')
        toBson(addFields(new Field('alt3', new Document('$lt', asList('$a', 3)))), direct) == parse(
                '{$addFields: {alt3: {$lt: ["$a", 3]}}}')
        toBson(addFields(new Field('b', 3), new Field('c', 5)), direct) == parse('{$addFields: {b: 3, c: 5}}')
        toBson(addFields(asList(new Field('b', 3), new Field('c', 5))), direct) == parse('{$addFields: {b: 3, c: 5}}')

        where:
        direct << [true, false]
    }

    def 'should render $bucket'() {
        expect:
        toBson(bucket('$screenSize', [0, 24, 32, 50, 100000]), direct) == parse('''{
            $bucket: {
              groupBy: "$screenSize",
              boundaries: [0, 24, 32, 50, 100000]
            }
          }''')
        toBson(bucket('$screenSize', [0, 24, 32, 50, 100000],
                      new BucketOptions()
                              .defaultBucket('other')), direct) == parse('''{
            $bucket: {
              groupBy: "$screenSize",
              boundaries: [0, 24, 32, 50, 100000],
              default: "other"
            }
          }''')
        toBson(bucket('$screenSize', [0, 24, 32, 50, 100000],
                      new BucketOptions()
                              .defaultBucket('other')
                              .output(sum('count', 1), push('matches', '$screenSize'))), direct) == parse('''{
            $bucket: {
                groupBy: "$screenSize",
                boundaries: [0, 24, 32, 50, 100000],
                default: "other",
                output: {
                    count: {$sum: 1},
                    matches: {$push: "$screenSize"}
                }
            }
        }''')

        where:
        direct << [true, false]
    }

    def 'should render $bucketAuto'() {
        expect:
        toBson(bucketAuto('$price', 4), direct) == parse('''{
            $bucketAuto: {
              groupBy: "$price",
              buckets: 4
            }
          }''')
        toBson(bucketAuto('$price', 4, new BucketAutoOptions()
                .output(sum('count', 1),
                        avg('avgPrice', '$price'))), direct) == parse('''{
                                              $bucketAuto: {
                                                groupBy: "$price",
                                                buckets: 4,
                                                output: {
                                                  count: {$sum: 1},
                                                  avgPrice: {$avg: "$price"},
                                                }
                                              }
                                            }''')
        toBson(bucketAuto('$price', 4, new BucketAutoOptions()
                .granularity(R5)
                .output(sum('count', 1),
                        avg('avgPrice', '$price'))), direct) == parse('''{
                                              $bucketAuto: {
                                                groupBy: "$price",
                                                buckets: 4,
                                                output: {
                                                  count: {$sum: 1},
                                                  avgPrice: {$avg: "$price"},
                                                },
                                                granularity: "R5"
                                              }
                                            }''')

        where:
        direct << [true, false]
    }

    def 'should render $count'() {
        expect:
        toBson(count(), direct) == parse('{$count: "count"}')
        toBson(count('count'), direct) == parse('{$count: "count"}')
        toBson(count('total'), direct) == parse('{$count: "total"}')

        where:
        direct << [true, false]
    }

    def 'should render $match'() {
        expect:
        toBson(match(eq('author', 'dave')), direct) == parse('{ $match : { author : "dave" } }')

        where:
        direct << [true, false]
    }

    def 'should render $project'() {
        expect:
        toBson(project(fields(include('title', 'author'), computed('lastName', '$author.last'))), direct) ==
        parse('{ $project : { title : 1 , author : 1, lastName : "$author.last" } }')

        where:
        direct << [true, false]
    }

    def 'should render $replaceRoot'() {
        expect:
        toBson(replaceRoot('$a1'), direct) == parse('{$replaceRoot: {newRoot: "$a1"}}')
        toBson(replaceRoot('$a1.b'), direct) == parse('{$replaceRoot: {newRoot: "$a1.b"}}')
        toBson(replaceRoot('$a1'), direct) == parse('{$replaceRoot: {newRoot: "$a1"}}')

        where:
        direct << [true, false]
    }

    def 'should render $sort'() {
        expect:
        toBson(sort(ascending('title', 'author')), direct) == parse('{ $sort : { title : 1 , author : 1 } }')

        where:
        direct << [true, false]
    }

    def 'should render $sortByCount'() {
        expect:
        toBson(sortByCount('someField'), direct) == parse('{$sortByCount: "someField"}')
        toBson(sortByCount(new Document('$floor', '$x')), direct) == parse('{$sortByCount: {$floor: "$x"}}')

        where:
        direct << [true, false]
    }

    def 'should render $limit'() {
        expect:
        toBson(limit(5), direct) == parse('{ $limit : 5 }')

        where:
        direct << [true, false]
    }

    def 'should render $lookup'() {
        expect:
        toBson(lookup('from', 'localField', 'foreignField', 'as'), direct) == parse('''{ $lookup : { from: "from", localField: "localField",
            foreignField: "foreignField", as: "as" } }''')

        List<Bson> pipeline = asList(match(expr(new Document('$eq', asList('x', '1')))))
        toBson(lookup('from', asList(new Variable('var1', 'expression1')), pipeline, 'as'), direct) ==
                parse('''{ $lookup : { from: "from",
                                            let: { var1: "expression1" },
                                            pipeline : [{ $match : { $expr: { $eq : [ "x" , "1" ]}}}],
                                            as: "as" }}''')

        // without variables
        toBson(lookup('from', pipeline, 'as'), direct) ==
                parse('''{ $lookup : { from: "from",
                                            pipeline : [{ $match : { $expr: { $eq : [ "x" , "1" ]}}}],
                                            as: "as" }}''')

        where:
        direct << [true, false]
    }

    def 'should render $facet'() {
        expect:
        toBson(facet(
                new Facet('Screen Sizes',
                               unwind('$attributes'),
                               match(eq('attributes.name', 'screen size')),
                               group(null, sum('count', 1 ))),
                new Facet('Manufacturer',
                          match(eq('attributes.name', 'manufacturer')),
                          group('$attributes.value', sum('count', 1)),
                          sort(descending('count')),
                          limit(5))), direct) ==
        parse('''{$facet: {
          "Screen Sizes": [
             {$unwind: "$attributes"},
             {$match: {"attributes.name": "screen size"}},
             {$group: {
                 _id: null,
                 count: {$sum: 1}
             }}
           ],

           "Manufacturer": [
             {$match: {"attributes.name": "manufacturer"}},
             {$group: {_id: "$attributes.value", count: {$sum: 1}}},
             {$sort: {count: -1}}
             {$limit: 5}
           ]
        }} ''')

        where:
        direct << [true, false]
    }

    def 'should render $graphLookup'() {
        expect:
        //without options
        toBson(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork'), direct) ==
        parse('''{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
            as: "socialNetwork" } }''')

        //with maxDepth
        toBson(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions().maxDepth(1)), direct) ==
        parse('''{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
            as: "socialNetwork", maxDepth: 1 } }''')

        // with depthField
        toBson(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork',
                new GraphLookupOptions().depthField('master')), direct) ==
                parse('''{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
            as: "socialNetwork", depthField: "master" } }''')

        // with restrictSearchWithMatch
        toBson(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .restrictSearchWithMatch(eq('hobbies', 'golf'))), direct) ==
                parse('''{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
            as: "socialNetwork", restrictSearchWithMatch : { "hobbies" : "golf" } } }''')

        // with maxDepth and depthField
        toBson(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('master')), direct) ==
        parse('''{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
            as: "socialNetwork", maxDepth: 1, depthField: "master" } }''')

        // with all options
        toBson(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('master').restrictSearchWithMatch(eq('hobbies', 'golf'))), direct) ==
                parse('''{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
            as: "socialNetwork", maxDepth: 1, depthField: "master", restrictSearchWithMatch : { "hobbies" : "golf" } } }''')

        where:
        direct << [true, false]
    }

    def 'should render $skip'() {
        expect:
        toBson(skip(5), direct) == parse('{ $skip : 5 }')

        where:
        direct << [true, false]
    }

    def 'should render $unwind'() {
        expect:
        toBson(unwind('$sizes'), direct) == parse('{ $unwind : "$sizes" }')
        toBson(unwind('$sizes', new UnwindOptions().preserveNullAndEmptyArrays(null)), direct) == parse('{ $unwind : { path : "$sizes" } }')
        toBson(unwind('$sizes', new UnwindOptions().preserveNullAndEmptyArrays(false)), direct) == parse('''
            { $unwind : { path : "$sizes", preserveNullAndEmptyArrays : false } }''')
        toBson(unwind('$sizes', new UnwindOptions().preserveNullAndEmptyArrays(true)), direct) == parse('''
            { $unwind : { path : "$sizes", preserveNullAndEmptyArrays : true } }''')
        toBson(unwind('$sizes', new UnwindOptions().includeArrayIndex(null)), direct) == parse('{ $unwind : { path : "$sizes" } }')
        toBson(unwind('$sizes', new UnwindOptions().includeArrayIndex('$a')), direct) == parse('''
            { $unwind : { path : "$sizes", includeArrayIndex : "$a" } }''')
        toBson(unwind('$sizes', new UnwindOptions().preserveNullAndEmptyArrays(true).includeArrayIndex('$a')), direct) == parse('''
            { $unwind : { path : "$sizes", preserveNullAndEmptyArrays : true, includeArrayIndex : "$a" } }''')

        where:
        direct << [true, false]
    }

    def 'should render $out'() {
        expect:
        toBson(out('authors'), direct) == parse('{ $out : "authors" }')
        toBson(out('authors', new AggregateOutStageOptions().mode(REPLACE_COLLECTION)), direct) == parse('{ $out : "authors" }')
        toBson(out('authors', new AggregateOutStageOptions().mode(REPLACE_DOCUMENTS)), direct) ==
                parse('{ $out : {mode : "replaceDocuments", to : "authors" } }')
        toBson(out('authors', new AggregateOutStageOptions().mode(INSERT_DOCUMENTS)), direct) ==
                parse('{ $out : {mode : "insertDocuments", to : "authors" } }')
        toBson(out('authors', new AggregateOutStageOptions().databaseName('db1')), direct) ==
                parse('{ $out : {mode : "replaceCollection", to : "authors", db : "db1" } }')
        toBson(out('authors', new AggregateOutStageOptions().uniqueKey(new BsonDocument('x', new BsonInt32(1)))), direct) ==
                parse('{ $out : {mode : "replaceCollection", to : "authors", uniqueKey : {x : 1 } } }')

        where:
        direct << [true, false]
    }

    def 'should render $group'() {
        expect:
        toBson(group('$customerId'), direct) == parse('{ $group : { _id : "$customerId" } }')
        toBson(group(null), direct) == parse('{ $group : { _id : null } }')

        toBson(group(parse('{ month: { $month: "$date" }, day: { $dayOfMonth: "$date" }, year: { $year: "$date" } }')), direct) ==
        parse('{ $group : { _id : { month: { $month: "$date" }, day: { $dayOfMonth: "$date" }, year: { $year: "$date" } } } }')


        def groupDocument = parse('''{
                            $group : {
                                      _id : null,
                                      sum: { $sum: { $multiply: [ "$price", "$quantity" ] } },
                                      avg: { $avg: "$quantity" },
                                      min: { $min: "$quantity" },
                                      max: { $max: "$quantity" },
                                      first: { $first: "$quantity" },
                                      last: { $last: "$quantity" },
                                      all: { $push: "$quantity" },
                                      unique: { $addToSet: "$quantity" },
                                      stdDevPop: { $stdDevPop: "$quantity" },
                                      stdDevSamp: { $stdDevSamp: "$quantity" }
                                     }
                                  }''')
        toBson(group(null,
                     sum('sum', parse('{ $multiply: [ "$price", "$quantity" ] }')),
                     avg('avg', '$quantity'),
                     min('min', '$quantity'),
                     max('max', '$quantity'),
                     first('first', '$quantity'),
                     last('last', '$quantity'),
                     push('all', '$quantity'),
                     addToSet('unique', '$quantity'),
                     stdDevPop('stdDevPop', '$quantity'),
                     stdDevSamp('stdDevSamp', '$quantity')
        ), direct) == groupDocument

        where:
        direct << [true, false]
    }

    def 'should render $sample'() {
        expect:
        toBson(sample(5), direct) == parse('{ $sample : { size: 5} }')

        where:
        direct << [true, false]
    }

    def 'should create string representation for simple stages'() {
        expect:
        match(new BsonDocument('x', new BsonInt32(1))).toString() == 'Stage{name=\'$match\', value={"x": 1}}'
    }

    def 'should create string representation for group stage'() {
        expect:
        group('_id', avg('avg', '$quantity')).toString() ==
                'Stage{name=\'$group\', id=_id, ' +
                'fieldAccumulators=[' +
                'BsonField{name=\'avg\', value=Expression{name=\'$avg\', expression=$quantity}}]}'
        group(null, avg('avg', '$quantity')).toString() ==
                'Stage{name=\'$group\', id=null, ' +
                'fieldAccumulators=[' +
                'BsonField{name=\'avg\', value=Expression{name=\'$avg\', expression=$quantity}}]}'
    }
}
