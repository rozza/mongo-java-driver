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

package com.mongodb.reactivestreams.client.internal.reactor;

import com.mongodb.ReadPreference;
import com.mongodb.internal.operation.ListCollectionsOperation;
import com.mongodb.reactivestreams.client.ListCollectionsPublisher;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import static com.mongodb.reactivestreams.client.MongoClients.getDefaultCodecRegistry;
import static com.mongodb.reactivestreams.client.internal.reactor.PublisherCreator.createListCollectionsPublisher;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ListCollectionsPublisherImplTest extends TestHelper {

    private final static String DATABASE_NAME = "databaseName";

    @DisplayName("Should build the expected ListCollectionsOperation")
    @Test
    void shouldBuildTheExpectedOperation() {
        configureBatchCursor();

        TestOperationExecutor executor = new TestOperationExecutor(asList(batchCursor, batchCursor));
        ListCollectionsPublisher<String> publisher = createListCollectionsPublisher(null, DATABASE_NAME, String.class, getDefaultCodecRegistry(),
                ReadPreference.primary(), executor, true, true);

        ListCollectionsOperation<String> expectedOperation = new ListCollectionsOperation<>(DATABASE_NAME,
                getDefaultCodecRegistry().get(String.class)).nameOnly(true).retryReads(true);

        // default input should be as expected
        Flux.from(publisher).blockFirst();

        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());

        // Should apply settings
        publisher
                .filter(new Document("filter", 1))
                .maxTime(10, SECONDS)
                .batchSize(100);

        expectedOperation
                .filter(new BsonDocument("filter", new BsonInt32(1)))
                .maxTime(10, SECONDS)
                .batchSize(100);

        configureBatchCursor();
        Flux.from(publisher).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
    }

}
