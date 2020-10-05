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

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.operation.DistinctOperation;
import com.mongodb.reactivestreams.client.DistinctPublisher;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import static com.mongodb.reactivestreams.client.MongoClients.getDefaultCodecRegistry;
import static com.mongodb.reactivestreams.client.internal.reactor.PublisherCreator.createDistinctPublisher;
import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DistinctPublisherImplTest extends TestHelper {

    private final static MongoNamespace NAMESPACE = new MongoNamespace("db", "coll");
    private final static Collation COLLATION = Collation.builder().locale("en").build();

    @DisplayName("Should build the expected DistinctOperation")
    @Test
    void shouldBuildTheExpectedOperation() {
        configureBatchCursor();

        String fieldName = "fieldName";
        TestOperationExecutor executor = new TestOperationExecutor(asList(batchCursor, batchCursor));
        DistinctPublisher<Document> publisher = createDistinctPublisher(null, NAMESPACE, Document.class, Document.class,
                getDefaultCodecRegistry(), ReadPreference.primary(), ReadConcern.DEFAULT, executor, fieldName, new Document(), true);

        DistinctOperation<Document> expectedOperation = new DistinctOperation<>(NAMESPACE, fieldName,
                getDefaultCodecRegistry().get(Document.class)).retryReads(true).filter(new BsonDocument());

        // default input should be as expected
        Flux.from(publisher).blockFirst();

        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());

        // Should apply settings
        BsonDocument filter = BsonDocument.parse("{a: 1}");
        publisher
                .batchSize(100)
                .collation(COLLATION)
                .filter(filter);

        expectedOperation
                .collation(COLLATION)
                .filter(filter);

        configureBatchCursor();
        Flux.from(publisher).blockFirst();
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
    }


    @DisplayName("Should handle error scenarios")
    @Test
    void shouldHandleErrorScenarios() {
        TestOperationExecutor executor = new TestOperationExecutor(asList(new MongoException("Failure"), null));

        // Operation fails
        Publisher<Document> publisher = createDistinctPublisher(null, NAMESPACE, Document.class, Document.class,
                getDefaultCodecRegistry(), ReadPreference.primary(), ReadConcern.DEFAULT, executor, "fieldName", new Document(), true);
        assertThrows(MongoException.class, () -> Flux.from(publisher).blockFirst());

        // Missing Codec
        Publisher<Document> publisherMissingCodec = createDistinctPublisher(null, NAMESPACE, Document.class, Document.class,
                fromProviders(new BsonValueCodecProvider()), ReadPreference.primary(), ReadConcern.DEFAULT, executor,"fieldName",
                new Document(), true);
        assertThrows(CodecConfigurationException.class, () -> Flux.from(publisherMissingCodec).blockFirst());
    }

}
