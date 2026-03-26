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

package com.mongodb.client.internal;

import com.mongodb.Function;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static com.mongodb.CustomMatchers.isTheSameAs;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MappingIterableTest {

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("should follow the MongoIterable interface as expected")
    void shouldFollowTheMongoIterableInterfaceAsExpected() {
        MongoIterable<Object> iterable = mock(MongoIterable.class);
        Function<Object, Object> mapper = doc -> doc;
        MappingIterable<Object, Object> mappingIterable = new MappingIterable<>(iterable, mapper);

        // first
        mappingIterable.first();
        verify(iterable).first();

        // forEach
        mappingIterable.forEach(doc -> { });
        verify(iterable).forEach(any());

        // into
        mappingIterable.into(new ArrayList<>());
        // into calls forEach internally

        // batchSize
        mappingIterable.batchSize(5);
        verify(iterable).batchSize(5);

        // iterator
        when(iterable.iterator()).thenReturn(mock(MongoCursor.class));
        mappingIterable.iterator();
        verify(iterable).iterator();

        // map
        Function<Object, Object> newMapper = x -> x;
        assertThat(mappingIterable.map(newMapper), isTheSameAs(new MappingIterable<>(mappingIterable, newMapper)));
    }
}
