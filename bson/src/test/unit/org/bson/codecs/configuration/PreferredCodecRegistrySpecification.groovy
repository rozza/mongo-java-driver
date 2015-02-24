/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.configuration

import org.bson.codecs.MaxKeyCodec
import org.bson.codecs.MinKeyCodec
import org.bson.types.MaxKey
import org.bson.types.MinKey
import spock.lang.Specification

import static org.bson.codecs.configuration.CodecRegistryHelper.fromCodec

class PreferredCodecRegistrySpecification extends Specification {

    def 'should throw if supplied codec is null'() {
        given:
        def registry = fromCodec(new MinKeyCodec())

        when:
        new PreferredCodecRegistry(null, null)

        then:
        thrown(IllegalArgumentException)

        when:
        new PreferredCodecRegistry(registry, null)

        then:
        thrown(IllegalArgumentException)

        when:
        new PreferredCodecRegistry(null, registry)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should return null if codec not found'() {
        when:
        def registry = new PreferredCodecRegistry(fromCodec(new MinKeyCodec()), fromCodec(new MinKeyCodec()))

        then:
        registry.get(MaxKey) == null

    }

    def 'should prefer the preferred codec registry'() {
        when:
        def codec1 = new MinKeyCodec()
        def codec2 = new MinKeyCodec()
        def codec3 = new MaxKeyCodec()
        def registry = new PreferredCodecRegistry(fromCodec(codec1), fromCodec(codec2))

        then:
        registry.get(MinKey) == codec1

        when:
        registry =  new PreferredCodecRegistry(fromCodec(codec3), fromCodec(codec2))

        then:
        registry.get(MinKey) == codec2
    }

}
