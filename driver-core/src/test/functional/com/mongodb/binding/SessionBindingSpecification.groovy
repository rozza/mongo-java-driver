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

package com.mongodb.binding

import spock.lang.Specification

class SessionBindingSpecification extends Specification {

    def 'should wrap the passed in binding'() {
        given:
        def wrapped = Mock(ReadWriteBinding)
        def binding = new SessionBinding(wrapped)

        when:
        binding.getCount()

        then:
        1 * wrapped.getCount()

        when:
        binding.getReadPreference()

        then:
        1 * wrapped.getReadPreference()

        when:
        binding.retain()

        then:
        1 * wrapped.retain()

        when:
        binding.release()

        then:
        1 * wrapped.release()

        when:
        binding.getReadConnectionSource()

        then:
        1 * wrapped.getReadConnectionSource()

        when:
        binding.getWriteConnectionSource()

        then:
        1 * wrapped.getWriteConnectionSource()

        when:
        def context = binding.getSessionContext()

        then:
        0 * wrapped.getSessionContext()
        context instanceof SimpleSessionContext
    }

}
