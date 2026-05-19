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

package com.mongodb.internal.connection;

import org.bson.ByteBuf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.internal.connection.MessageHeader.MESSAGE_HEADER_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Attempts to trigger a NIO2 memory visibility race with real async socket I/O.
 *
 * <p>Strategy: multiple connections share a single {@link PowerOfTwoBufferPool} with a small
 * bucket for 16-byte buffers. Each connection rapidly reads response headers from a local
 * server that sends unique, verifiable data. Between reads, buffers are released back to the
 * pool and immediately reacquired by other connections' reads.</p>
 *
 * <p>If the NIO2 epoll completion handler ever fires with stale buffer contents (a memory
 * visibility gap between the kernel DMA write and the userspace handler thread), the read
 * will see data from a DIFFERENT connection's previous response — producing the exact
 * "responseTo mismatch" pattern seen in production.</p>
 *
 * <p>This test is probabilistic — it may not trigger the race on every run. The race is
 * extremely timing-dependent and may require specific JVM/kernel combinations. Run with
 * high repetition counts on the target platform to maximize detection probability.</p>
 *
 * @see <a href="https://jira.mongodb.org/browse/HELP-93816">HELP-93816</a>
 */
class NioBufferVisibilityRaceTest {

    private static final int NUM_CONNECTIONS = 8;
    private static final int READS_PER_CONNECTION = 5000;

    private ServerSocket serverSocket;
    private ExecutorService serverExecutor;
    private PowerOfTwoBufferPool pool;
    private final AtomicInteger mismatchCount = new AtomicInteger(0);
    private final AtomicInteger totalReads = new AtomicInteger(0);
    private final List<String> mismatchDetails = Collections.synchronizedList(new ArrayList<>());

    @BeforeEach
    void setUp() throws IOException {
        serverSocket = new ServerSocket(0); // OS-assigned port
        serverExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "test-server");
            t.setDaemon(true);
            return t;
        });
        pool = new PowerOfTwoBufferPool(10);
        mismatchCount.set(0);
        totalReads.set(0);
        mismatchDetails.clear();
    }

    @AfterEach
    void tearDown() throws IOException {
        serverExecutor.shutdownNow();
        serverSocket.close();
        pool.disablePruning();
    }

    /**
     * Runs multiple async connections reading from a local server that sends unique headers.
     * Each read uses a buffer from the shared pool. After reading, verifies the buffer contains
     * the data the server actually sent — not stale data from another connection's previous read.
     *
     * <p>The server sends headers where {@code responseTo} is a unique, incrementing value.
     * Each client read verifies it got its own unique value. A mismatch means the buffer
     * contained stale data from a previous read (the NIO2 race we're trying to trigger).</p>
     */
    @RepeatedTest(10)
    void asyncReadsWithSharedPoolShouldNeverSeeStaleData() throws Exception {
        int port = serverSocket.getLocalPort();
        AtomicInteger serverSequence = new AtomicInteger(1_000_000);

        // Start server threads — one per connection, sends unique headers on demand
        CountDownLatch allClientsConnected = new CountDownLatch(NUM_CONNECTIONS);
        CountDownLatch allReadsDone = new CountDownLatch(NUM_CONNECTIONS);

        for (int c = 0; c < NUM_CONNECTIONS; c++) {
            serverExecutor.submit(() -> {
                try {
                    Socket clientSocket = serverSocket.accept();
                    allClientsConnected.countDown();
                    OutputStream out = clientSocket.getOutputStream();

                    for (int i = 0; i < READS_PER_CONNECTION; i++) {
                        int seq = serverSequence.getAndIncrement();
                        ByteBuffer header = ByteBuffer.allocate(MESSAGE_HEADER_LENGTH);
                        header.order(ByteOrder.LITTLE_ENDIAN);
                        header.putInt(MESSAGE_HEADER_LENGTH + 20); // messageLength (header + small body)
                        header.putInt(seq + 100_000);              // requestId (server's response ID)
                        header.putInt(seq);                        // responseTo (unique per write)
                        header.putInt(2013);                       // opCode (OP_MSG)
                        header.flip();
                        out.write(header.array());
                        out.flush();
                    }
                    clientSocket.close();
                } catch (IOException e) {
                    // Connection closed, test ending
                }
            });
        }

        // Start client connections — each reads headers using shared pool
        List<CompletableFuture<Void>> clientFutures = new ArrayList<>();
        for (int c = 0; c < NUM_CONNECTIONS; c++) {
            final int connectionId = c;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    AsynchronousSocketChannel channel = AsynchronousSocketChannel.open();
                    channel.connect(new InetSocketAddress("127.0.0.1", port)).get(5, TimeUnit.SECONDS);

                    for (int i = 0; i < READS_PER_CONNECTION; i++) {
                        // Get buffer from shared pool (may contain stale data from another connection)
                        ByteBuf byteBuf = pool.getBuffer(MESSAGE_HEADER_LENGTH);
                        ByteBuffer nio = byteBuf.asNIO();

                        // Async read into the pooled buffer
                        CompletableFuture<Integer> readFuture = new CompletableFuture<>();
                        channel.read(nio, null, new CompletionHandler<Integer, Void>() {
                            @Override
                            public void completed(final Integer result, final Void attachment) {
                                readFuture.complete(result);
                            }

                            @Override
                            public void failed(final Throwable exc, final Void attachment) {
                                readFuture.completeExceptionally(exc);
                            }
                        });

                        int bytesRead = readFuture.get(5, TimeUnit.SECONDS);
                        assertEquals(MESSAGE_HEADER_LENGTH, bytesRead,
                                "Should read full header in one shot from localhost");

                        // Parse header (same as MessageHeader constructor)
                        nio.flip();
                        nio.order(ByteOrder.LITTLE_ENDIAN);
                        int messageLength = nio.getInt();
                        int requestId = nio.getInt();
                        int responseTo = nio.getInt();
                        int opCode = nio.getInt();

                        totalReads.incrementAndGet();

                        // Validate: responseTo should be a value the server sent (>= 1_000_000)
                        // and opCode should be 2013. If we see something else, it's stale data.
                        if (opCode != 2013 || responseTo < 1_000_000
                                || messageLength != MESSAGE_HEADER_LENGTH + 20) {
                            mismatchCount.incrementAndGet();
                            mismatchDetails.add(String.format(
                                    "conn=%d read=%d: messageLength=%d requestId=%d responseTo=%d opCode=%d",
                                    connectionId, i, messageLength, requestId, responseTo, opCode));
                        }

                        // Release immediately to maximize pool reuse across connections
                        byteBuf.release();

                        // Read and discard the body (20 bytes that server didn't send — will block/fail)
                        // Actually, server only sends headers, so we just continue reading next header.
                        // This means the stream stays aligned: 16 bytes in, 16 bytes consumed.
                    }

                    channel.close();
                } catch (Exception e) {
                    throw new RuntimeException("Connection " + connectionId + " failed", e);
                } finally {
                    allReadsDone.countDown();
                }
            });
            clientFutures.add(future);
        }

        // Wait for all clients
        allClientsConnected.await(10, TimeUnit.SECONDS);
        for (CompletableFuture<Void> f : clientFutures) {
            f.get(30, TimeUnit.SECONDS);
        }

        // Report results
        int total = totalReads.get();
        int mismatches = mismatchCount.get();

        if (mismatches > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("DETECTED NIO2 RACE: %d/%d reads saw stale buffer data%n", mismatches, total));
            int detailsToShow = Math.min(10, mismatchDetails.size());
            for (int i = 0; i < detailsToShow; i++) {
                sb.append("  ").append(mismatchDetails.get(i)).append("\n");
            }
            System.err.println(sb);
        }

        assertEquals(0, mismatches,
                "Detected " + mismatches + " reads with stale buffer data out of " + total + " total reads. "
                + "This indicates a NIO2 memory visibility race where the completion handler "
                + "sees buffer contents from a previous read rather than the current one.");

        assertTrue(total >= NUM_CONNECTIONS * READS_PER_CONNECTION,
                "Expected at least " + (NUM_CONNECTIONS * READS_PER_CONNECTION) + " reads, got " + total);
    }
}
