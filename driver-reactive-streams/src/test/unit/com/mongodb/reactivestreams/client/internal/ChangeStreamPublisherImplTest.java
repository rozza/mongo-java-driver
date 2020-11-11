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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.internal.operation.ChangeStreamOperation;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.List;

import static com.mongodb.reactivestreams.client.MongoClients.getDefaultCodecRegistry;
import static com.mongodb.reactivestreams.client.internal.PublisherCreator.createChangeStreamPublisher;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ChangeStreamPublisherImplTest extends TestHelper {

    private static final MongoNamespace NAMESPACE = new MongoNamespace("db", "coll");
    private static final Collation COLLATION = Collation.builder().locale("en").build();

    @DisplayName("Should build the expected ChangeStreamOperation")
    @Test
    void shouldBuildTheExpectedOperation() {
        configureBatchCursor();
        List<BsonDocument> pipeline = singletonList(BsonDocument.parse("{'$match': 1}"));
        Codec<ChangeStreamDocument<Document>> codec = ChangeStreamDocument.createCodec(Document.class, getDefaultCodecRegistry());

        TestOperationExecutor executor = new TestOperationExecutor(asList(getBatchCursor(), getBatchCursor()));
        ChangeStreamPublisher<Document> publisher = createChangeStreamPublisher(null, NAMESPACE, Document.class,
                                                                                getDefaultCodecRegistry(), ReadPreference.primary(),
                                                                                ReadConcern.DEFAULT, executor, pipeline,
                                                                                ChangeStreamLevel.COLLECTION,
                                                                                true);

        ChangeStreamOperation<ChangeStreamDocument<Document>> expectedOperation = new ChangeStreamOperation<>(NAMESPACE,
                                                                                                              FullDocument.DEFAULT,
                                                                                                              pipeline, codec)
                .retryReads(true);

        // default input should be as expected
        Flux.from(publisher).blockFirst();

        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());

        // Should apply settings
        publisher
                .batchSize(100)
                .collation(COLLATION)
                .maxAwaitTime(20, SECONDS)
                .fullDocument(FullDocument.UPDATE_LOOKUP);

        expectedOperation = new ChangeStreamOperation<>(NAMESPACE,
                                                        FullDocument.UPDATE_LOOKUP, pipeline, codec).retryReads(true);
        expectedOperation
                .batchSize(100)
                .collation(COLLATION)
                .maxAwaitTime(20, SECONDS);

        configureBatchCursor();
        Flux.from(publisher).blockFirst();
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
    }

    @DisplayName("Should build the expected ChangeStreamOperation when setting the document class")
    @Test
    void shouldBuildTheExpectedOperationWhenSettingDocumentClass() {
        configureBatchCursor();
        List<BsonDocument> pipeline = singletonList(BsonDocument.parse("{'$match': 1}"));
        TestOperationExecutor executor = new TestOperationExecutor(singletonList(getBatchCursor()));
        Publisher<BsonDocument> publisher = createChangeStreamPublisher(null, NAMESPACE, Document.class,
                                                                        getDefaultCodecRegistry(), ReadPreference.primary(),
                                                                        ReadConcern.DEFAULT, executor, pipeline,
                                                                        ChangeStreamLevel.COLLECTION, false)
                .withDocumentClass(BsonDocument.class);

        ChangeStreamOperation<BsonDocument> expectedOperation = new ChangeStreamOperation<>(NAMESPACE,
                                                                                            FullDocument.DEFAULT, pipeline,
                                                                                            getDefaultCodecRegistry()
                                                                                                    .get(BsonDocument.class));

        // default input should be as expected
        Flux.from(publisher).blockFirst();

        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
    }

    @DisplayName("Should handle error scenarios")
    @Test
    void shouldHandleErrorScenarios() {
        List<BsonDocument> pipeline = singletonList(BsonDocument.parse("{'$match': 1}"));
        TestOperationExecutor executor = new TestOperationExecutor(asList(new MongoException("Failure"), null, null));

        // Operation fails
        ChangeStreamPublisher<Document> publisher = createChangeStreamPublisher(null, NAMESPACE, Document.class,
                                                                                getDefaultCodecRegistry(), ReadPreference.primary(),
                                                                                ReadConcern.DEFAULT, executor, pipeline,
                                                                                ChangeStreamLevel.COLLECTION, false);
        assertThrows(MongoException.class, () -> Flux.from(publisher).blockFirst());

        // Missing Codec
        assertThrows(CodecConfigurationException.class, () -> createChangeStreamPublisher(null, NAMESPACE, Document.class,
                                                                                          fromProviders(new BsonValueCodecProvider()),
                                                                                          ReadPreference.primary(), ReadConcern.DEFAULT,
                                                                                          executor, pipeline,
                                                                                          ChangeStreamLevel.COLLECTION, false));

        // Pipeline contains null
        ChangeStreamPublisher<Document> publisherPipelineNull = createChangeStreamPublisher(null, NAMESPACE, Document.class,
                                                                                            getDefaultCodecRegistry(),
                                                                                            ReadPreference.primary(), ReadConcern.DEFAULT,
                                                                                            executor, singletonList(null),
                                                                                            ChangeStreamLevel.COLLECTION, false);
        assertThrows(IllegalArgumentException.class, () -> Flux.from(publisherPipelineNull).blockFirst());
    }

}
