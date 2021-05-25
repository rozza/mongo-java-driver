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
import com.mongodb.WriteConcern;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.operation.AggregateOperation;
import com.mongodb.internal.operation.AggregateToCollectionOperation;
import com.mongodb.internal.operation.FindOperation;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.List;

import static com.mongodb.reactivestreams.client.MongoClients.getDefaultCodecRegistry;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"deprecation", "rawtypes"})
public class AggregatePublisherImplTest extends TestHelper {

    @DisplayName("Should build the expected AggregateOperation")
    @Test
    void shouldBuildTheExpectedOperation() {
        List<BsonDocument> pipeline = singletonList(BsonDocument.parse("{'$match': 1}"));

        TestOperationExecutor executor = createOperationExecutor(asList(getBatchCursor(), getBatchCursor()));
        AggregatePublisher<Document> publisher =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.COLLECTION);

        AggregateOperation<Document> expectedOperation = new AggregateOperation<>(CSOT_NO_TIMEOUT, NAMESPACE, pipeline,
                                                                                  getDefaultCodecRegistry().get(Document.class))
                .batchSize(Integer.MAX_VALUE)
                .retryReads(true);

        // default input should be as expected
        Flux.from(publisher).blockFirst();

        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());

        // Should apply settings
        publisher
                .allowDiskUse(true)
                .batchSize(100)
                .bypassDocumentValidation(true) // Ignored
                .collation(COLLATION)
                .comment("my comment")
                .hint(BsonDocument.parse("{a: 1}"))
                .maxTime(99, MILLISECONDS)
                .maxAwaitTime(999, MILLISECONDS);

        expectedOperation = new AggregateOperation<>(CSOT_MAX_TIME_AND_MAX_AWAIT_TIME, NAMESPACE, pipeline,
                getDefaultCodecRegistry().get(Document.class))
                .retryReads(true)
                .allowDiskUse(true)
                .batchSize(100)
                .collation(COLLATION)
                .comment("my comment")
                .hint(BsonDocument.parse("{a: 1}"));

        Flux.from(publisher).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
    }

    @DisplayName("Should build the expected AggregateOperation for $out")
    @Test
    void shouldBuildTheExpectedOperationsForDollarOut() {
        String collectionName = "collectionName";
        List<BsonDocument> pipeline = asList(BsonDocument.parse("{'$match': 1}"),
                                             BsonDocument.parse(format("{'$out': '%s'}", collectionName)));
        MongoNamespace collectionNamespace = new MongoNamespace(NAMESPACE.getDatabaseName(), collectionName);

        TestOperationExecutor executor = createOperationExecutor(asList(getBatchCursor(), getBatchCursor(), getBatchCursor(), null));
        AggregatePublisher<Document> publisher =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.COLLECTION);

        AggregateToCollectionOperation expectedOperation = new AggregateToCollectionOperation(CSOT_NO_TIMEOUT, NAMESPACE, pipeline,
                ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED);

        // default input should be as expected
        Flux.from(publisher).blockFirst();

        WriteOperationThenCursorReadOperation operation = (WriteOperationThenCursorReadOperation) executor.getReadOperation();
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        assertOperationIsTheSameAs(expectedOperation, operation.getAggregateToCollectionOperation());

        // Should apply settings
        publisher
                .allowDiskUse(true)
                .batchSize(100) // Used in Find
                .bypassDocumentValidation(true)
                .collation(COLLATION)
                .comment("my comment")
                .hint(BsonDocument.parse("{a: 1}"))
                .maxAwaitTime(999, MILLISECONDS) // Ignored on $out
                .maxTime(99, MILLISECONDS);

        expectedOperation = new AggregateToCollectionOperation(CSOT_MAX_TIME_AND_MAX_AWAIT_TIME, NAMESPACE, pipeline,
                ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED)
                .allowDiskUse(true)
                .bypassDocumentValidation(true)
                .collation(COLLATION)
                .comment("my comment")
                .hint(BsonDocument.parse("{a: 1}"));

        Flux.from(publisher).blockFirst();
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        operation = (WriteOperationThenCursorReadOperation) executor.getReadOperation();
        assertOperationIsTheSameAs(expectedOperation, operation.getAggregateToCollectionOperation());

        FindOperation<Document> expectedFindOperation =
                new FindOperation<>(CSOT_MAX_TIME_AND_MAX_AWAIT_TIME, collectionNamespace,
                        getDefaultCodecRegistry().get(Document.class))
                        .batchSize(100)
                        .collation(COLLATION)
                        .filter(new BsonDocument())
                        .retryReads(true);

        assertOperationIsTheSameAs(expectedFindOperation, operation.getReadOperation());

        // Should handle database level aggregations
        publisher = new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.DATABASE);

        expectedOperation = new AggregateToCollectionOperation(CSOT_NO_TIMEOUT, NAMESPACE, pipeline, ReadConcern.DEFAULT,
                WriteConcern.ACKNOWLEDGED);

        Flux.from(publisher).blockFirst();
        operation = (WriteOperationThenCursorReadOperation) executor.getReadOperation();
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        assertOperationIsTheSameAs(expectedOperation, operation.getAggregateToCollectionOperation());

        // Should handle toCollection
        publisher = new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.COLLECTION);

        expectedOperation = new AggregateToCollectionOperation(CSOT_NO_TIMEOUT, NAMESPACE, pipeline, ReadConcern.DEFAULT,
                WriteConcern.ACKNOWLEDGED);

        // default input should be as expected
        Flux.from(publisher.toCollection()).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getWriteOperation());
    }

    @DisplayName("Should build the expected AggregateOperation for $out as document")
    @Test
    void shouldBuildTheExpectedOperationsForDollarOutAsDocument() {
        List<BsonDocument> pipeline = asList(BsonDocument.parse("{'$match': 1}"), BsonDocument.parse("{'$out': {s3: true}}"));

        TestOperationExecutor executor = createOperationExecutor(asList(getBatchCursor(), null, null, null));
        AggregatePublisher<Document> publisher =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.COLLECTION);

        // default input should be as expected
        assertThrows(IllegalStateException.class, () -> {
            Flux.from(publisher).blockFirst();
            executor.getReadOperation();
        });

        // Should handle toCollection
        Publisher<Void> toCollectionPublisher =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.COLLECTION)
                        .toCollection();

        AggregateToCollectionOperation expectedOperation = new AggregateToCollectionOperation(CSOT_NO_TIMEOUT, NAMESPACE,
                pipeline, ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED);

        Flux.from(toCollectionPublisher).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getWriteOperation());

        // Should handle database level
        toCollectionPublisher =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.DATABASE)
                        .toCollection();

        Flux.from(toCollectionPublisher).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getWriteOperation());

        // Should handle $out with namespace
        List<BsonDocument> pipelineWithNamespace = asList(BsonDocument.parse("{'$match': 1}"),
                                                          BsonDocument.parse("{'$out': {db: 'db1', coll: 'coll1'}}"));
        toCollectionPublisher = new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipelineWithNamespace,
                                                             AggregationLevel.COLLECTION)
                .toCollection();

        expectedOperation = new AggregateToCollectionOperation(CSOT_NO_TIMEOUT, NAMESPACE, pipelineWithNamespace,
                ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED);

        Flux.from(toCollectionPublisher).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getWriteOperation());
    }

    @DisplayName("Should build the expected AggregateOperation for $merge")
    @Test
    void shouldBuildTheExpectedOperationsForDollarMerge() {
        String collectionName = "collectionName";
        List<BsonDocument> pipeline = asList(BsonDocument.parse("{'$match': 1}"),
                                             BsonDocument.parse(format("{'$merge': {into: '%s'}}", collectionName)));
        MongoNamespace collectionNamespace = new MongoNamespace(NAMESPACE.getDatabaseName(), collectionName);

        TestOperationExecutor executor = createOperationExecutor(asList(getBatchCursor(), getBatchCursor(), getBatchCursor(), null));
        AggregatePublisher<Document> publisher =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.COLLECTION);

        AggregateToCollectionOperation expectedOperation = new AggregateToCollectionOperation(CSOT_NO_TIMEOUT, NAMESPACE,
                pipeline, ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED);

        // default input should be as expected
        Flux.from(publisher).blockFirst();

        WriteOperationThenCursorReadOperation operation = (WriteOperationThenCursorReadOperation) executor.getReadOperation();
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        assertOperationIsTheSameAs(expectedOperation, operation.getAggregateToCollectionOperation());

        // Should apply settings
        publisher
                .allowDiskUse(true)
                .batchSize(100) // Used in Find
                .bypassDocumentValidation(true)
                .collation(COLLATION)
                .comment("my comment")
                .hint(BsonDocument.parse("{a: 1}"))
                .maxAwaitTime(999, MILLISECONDS) // Ignored on $out
                .maxTime(99, MILLISECONDS);

        expectedOperation = new AggregateToCollectionOperation(CSOT_MAX_TIME_AND_MAX_AWAIT_TIME, NAMESPACE,
                pipeline, ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED)
                .allowDiskUse(true)
                .bypassDocumentValidation(true)
                .collation(COLLATION)
                .comment("my comment")
                .hint(BsonDocument.parse("{a: 1}"));

        Flux.from(publisher).blockFirst();
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        operation = (WriteOperationThenCursorReadOperation) executor.getReadOperation();
        assertOperationIsTheSameAs(expectedOperation, operation.getAggregateToCollectionOperation());

        FindOperation<Document> expectedFindOperation =
                new FindOperation<>(CSOT_MAX_TIME_AND_MAX_AWAIT_TIME, collectionNamespace,
                        getDefaultCodecRegistry().get(Document.class))
                        .batchSize(100)
                        .collation(COLLATION)
                        .filter(new BsonDocument())
                        .retryReads(true);

        assertOperationIsTheSameAs(expectedFindOperation, operation.getReadOperation());

        // Should handle database level aggregations
        publisher = new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline,
                AggregationLevel.DATABASE);

        expectedOperation = new AggregateToCollectionOperation(CSOT_NO_TIMEOUT, NAMESPACE, pipeline, ReadConcern.DEFAULT,
                WriteConcern.ACKNOWLEDGED);

        Flux.from(publisher).blockFirst();
        operation = (WriteOperationThenCursorReadOperation) executor.getReadOperation();
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        assertOperationIsTheSameAs(expectedOperation, operation.getAggregateToCollectionOperation());

        // Should handle toCollection
        publisher = new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.COLLECTION);

        expectedOperation = new AggregateToCollectionOperation(CSOT_NO_TIMEOUT, NAMESPACE, pipeline, ReadConcern.DEFAULT,
                WriteConcern.ACKNOWLEDGED);

        // default input should be as expected
        Flux.from(publisher.toCollection()).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getWriteOperation());
    }

    @DisplayName("Should handle error scenarios")
    @Test
    void shouldHandleErrorScenarios() {
        List<BsonDocument> pipeline = singletonList(BsonDocument.parse("{'$match': 1}"));
        TestOperationExecutor errorExecutor = createOperationExecutor(singletonList(new MongoException("Failure")));

        // Operation fails
        Publisher<Document> publisher =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(errorExecutor), pipeline, AggregationLevel.COLLECTION);
        assertThrows(MongoException.class, () -> Flux.from(publisher).blockFirst());

        // Missing Codec
        TestOperationExecutor executor = createOperationExecutor(asList(getBatchCursor(), getBatchCursor()));
        Publisher<Document> publisherMissingCodec =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor)
                        .withCodecRegistry(BSON_CODEC_REGISTRY), pipeline, AggregationLevel.COLLECTION);
        assertThrows(CodecConfigurationException.class, () -> {
            Flux.from(publisherMissingCodec).blockFirst();
            executor.getReadOperation();
        });

        // Pipeline contains null
        Publisher<Document> publisherPipelineNull =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), singletonList(null),
                        AggregationLevel.COLLECTION);
        assertThrows(IllegalArgumentException.class, () -> {
            Flux.from(publisherPipelineNull).blockFirst();
            executor.getReadOperation();
        });
    }


}
