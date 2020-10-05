package com.mongodb.reactivestreams.client.internal.reactor.gridfs;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class ReszingByteBufferFluxTest {

    private static final String TEST_STRING = String.join("",
            asList("foobar", "foo", "bar", "baz", "qux", "quux", "quuz", "corge", "grault", "garply",
                    "waldo", "fred", "plugh", "xyzzy", "thud"));

    @TestFactory
    @DisplayName("test that the resizing publisher produces the expected results")
    List<DynamicTest> testResizingByteBufferPublisher() {
        List<DynamicTest> dynamicTests = new ArrayList<>();
        IntStream.range(1, 11).boxed().forEach(i -> {
            int originalChunkSize = 11 - i;
            dynamicTests.add(dynamicTest("Resizing from chunks of: " + originalChunkSize + " to chunks of: " + i, () -> {
                Flux<ByteBuffer> input = Flux.fromIterable(splitStringIntoChunks(TEST_STRING, originalChunkSize))
                        .map(STRING_BYTE_BUFFER_FUNCTION);
                Flux<String> resized = new ResizingByteBufferFlux(input, i).map(BYTE_BUFFER_STRING_FUNCTION);
                assertIterableEquals(splitStringIntoChunks(TEST_STRING, i), resized.toIterable());
            }));
        });
        return dynamicTests;
    }

    @Test
    public void testAndVerifyResizingByteBufferPublisher() {
        List<Long> internalRequests = new ArrayList<>();
        Flux<ByteBuffer> internal = Flux.fromIterable(asList("fo", "ob", "ar", "foo", "bar", "ba", "z"))
                .map(STRING_BYTE_BUFFER_FUNCTION)
                .doOnRequest(internalRequests::add);
        Flux<ByteBuffer> publisher = new ResizingByteBufferFlux(internal , 3);

        Duration waitDuration = Duration.ofMillis(200);
        StepVerifier.create(publisher, 0)
                .expectSubscription()
                .expectNoEvent(waitDuration)
                .thenRequest(1)
                .expectNext(STRING_BYTE_BUFFER_FUNCTION.apply("foo"))
                .expectNoEvent(waitDuration)
                .thenRequest(1)
                .expectNext(STRING_BYTE_BUFFER_FUNCTION.apply("bar"))
                .expectNoEvent(waitDuration)
                .thenRequest(1)
                .expectNext(STRING_BYTE_BUFFER_FUNCTION.apply("foo"))
                .expectNoEvent(waitDuration)
                .thenRequest(1)
                .expectNext(STRING_BYTE_BUFFER_FUNCTION.apply("bar"))
                .expectNoEvent(waitDuration)
                .thenRequest(1)
                .expectNext(STRING_BYTE_BUFFER_FUNCTION.apply("baz"))
                .expectComplete()
                .verify();

        assertIterableEquals(asList(1L, 1L, 1L, 1L, 1L, 1L, 1L), internalRequests);
    }


    private Collection<String> splitStringIntoChunks(final String original, final int chunkSize) {
        AtomicInteger splitCounter = new AtomicInteger(0);
        return original
                .chars()
                .mapToObj(_char -> String.valueOf((char)_char))
                .collect(Collectors.groupingBy(stringChar -> splitCounter.getAndIncrement() / chunkSize,
                        Collectors.joining()))
                .values();
    }

    final static Function<ByteBuffer, String> BYTE_BUFFER_STRING_FUNCTION = bb -> {
        bb.mark();
        byte[] arr = new byte[bb.remaining()];
        bb.get(arr);
        bb.reset();
        return new String(arr);
    };

    final static Function<String, ByteBuffer> STRING_BYTE_BUFFER_FUNCTION = s -> ByteBuffer.wrap(s.getBytes());

}
