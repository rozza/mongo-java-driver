package com.mongodb.reactivestreams.client;


import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.IntStream;

import static com.mongodb.reactivestreams.client.Fixture.getDefaultDatabase;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class SmokeTest {

    @BeforeEach
    void setUp() {
        Mono.from(getDefaultDatabase().drop()).block();
    }

    @AfterAll
    static void tearDown() {
        Mono.from(getDefaultDatabase().drop()).block();
    }

    @Test
    void testCollection() {
        MongoCollection<Document> collection = getDefaultDatabase().getCollection("test");
        List<Document> documents = IntStream.range(1, 100).boxed().map(i -> Document.parse(format("{_id: %s, value: '%s'}", i, i))).collect(toList());

        Mono.from(collection.insertMany(documents)).block();
        assertEquals(10, Flux.from(collection.find().batchSize(2).limit(10)).collect(toList()).block().size());
    }
}
