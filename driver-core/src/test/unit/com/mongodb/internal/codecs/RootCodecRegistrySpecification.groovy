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

package com.mongodb.internal.codecs

import org.bson.codecs.MinKeyCodec
import org.bson.codecs.configuration.CodecConfigurationException
import org.bson.types.MaxKey
import org.bson.types.MinKey
import spock.lang.Specification

import static com.mongodb.internal.codecs.RootCodecRegistry.createRootRegistry
import static org.bson.codecs.configuration.CodecRegistryHelper.fromCodec

class RootCodecRegistrySpecification extends Specification {

    def 'should throw if supplied codec registry is null'() {
        when:
        createRootRegistry(null)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should throw exception if codec not found'() {
        when:
        createRootRegistry(fromCodec(new MinKeyCodec())).get(MaxKey)

        then:
        thrown(CodecConfigurationException)
    }

    def 'should find the codec if available from the underlying registry'() {
        when:
        def codec = new MinKeyCodec()
        def registry = createRootRegistry(fromCodec(codec))

        then:
        registry.get(MinKey) == codec
    }


    def 'should not nest RootCodecRegistries'() {
        when:
        def registry = createRootRegistry(fromCodec(new MinKeyCodec()))

        then:
        registry.is(createRootRegistry(registry))
    }

}
