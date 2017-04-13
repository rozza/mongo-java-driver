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

package org.bson.codecs

import spock.lang.Specification

import static org.bson.codecs.configuration.CodecRegistries.fromProviders

class BigDecimalCodecProviderSpecification extends Specification {
    def 'should provide codec for BigDecimals'() {
        given:
        def provider = new BigDecimalCodecProvider()
        def registry = fromProviders(provider)

        expect:
        provider.get(BigDecimal, registry) instanceof BigDecimalCodec
    }

    def 'should not provide codec for non-BigDecimals'() {
        given:
        def provider = new BigDecimalCodecProvider()
        def registry = fromProviders(provider)

        expect:
        provider.get(Integer, registry) == null
    }

    def 'identical instances should be equal and have same hash code'() {
        given:
        def first = new BigDecimalCodecProvider()
        def second = new BigDecimalCodecProvider()

        expect:
        first.equals(first)
        first.equals(second)
        first.hashCode() == first.hashCode()
        first.hashCode() == second.hashCode()
    }
}
