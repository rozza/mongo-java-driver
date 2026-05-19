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
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.mongodb.internal.connection.MessageHeader.MESSAGE_HEADER_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Demonstrates the precondition for the intermittent "responseTo does not match requestId" error
 * (HELP-93816).
 *
 * <h2>Background</h2>
 * <p>Users report a rare, non-reproducible error where the driver reads a response from the socket
 * whose {@code responseTo} field doesn't match the {@code requestId} of the request that was sent.
 * The mismatched value is always a valid, recent requestId (typically 4-10 IDs behind), not garbage.</p>
 *
 * <h2>Root cause hypothesis</h2>
 * <p>The {@link PowerOfTwoBufferPool} reuses heap {@link ByteBuffer}s via LIFO semantics.
 * {@link ByteBuffer#clear()} resets position/limit but does NOT zero the underlying memory.
 * This means a freshly-acquired buffer from the pool still contains data from its previous use.</p>
 *
 * <p>In the async read path ({@code AsynchronousChannelStream.readAsync()}), a buffer is obtained
 * from the pool and passed to {@code AsynchronousSocketChannel.read()}. The NIO2 async read is
 * expected to fill the buffer with fresh data from the socket before firing the completion handler.
 * If that invariant is ever violated — due to a JVM/kernel race where the completion fires before
 * the DMA write is visible to the handler thread — then {@link MessageHeader} would parse stale
 * bytes from the buffer's previous occupant: a different response header with a different
 * {@code responseTo} value.</p>
 *
 * <h2>What these tests prove</h2>
 * <p>These tests demonstrate the PRECONDITION — that stale response header data persists in
 * pooled buffers after release and reacquisition. They do NOT reproduce the actual NIO2 race
 * (which is a JVM/kernel timing issue that cannot be reliably triggered from userspace).</p>
 *
 * @see <a href="https://jira.mongodb.org/browse/HELP-93816">HELP-93816</a>
 */
class ResponseToMismatchReproductionTest {

    /**
     * Proves that {@link PowerOfTwoBufferPool} returns the same physical {@link ByteBuffer}
     * after release (LIFO reuse), and that the buffer retains its previous content.
     *
     * <p>This is the fundamental precondition: if NIO2 ever fires a read completion without
     * writing new data, the stale content from the previous use would be parsed as a valid
     * (but wrong) response header.</p>
     */
    @Test
    void pooledBufferRetainsStaleDataAfterReleaseAndReacquisition() {
        PowerOfTwoBufferPool pool = new PowerOfTwoBufferPool(10);
        try {
            // Simulate a previous operation's response header being read into a pooled buffer
            ByteBuf firstBuf = pool.getBuffer(MESSAGE_HEADER_LENGTH);
            ByteBuffer firstNio = firstBuf.asNIO();
            firstNio.order(ByteOrder.LITTLE_ENDIAN);
            firstNio.putInt(100);     // messageLength
            firstNio.putInt(7777);    // requestId (server's ID for this response)
            firstNio.putInt(1163582); // responseTo (echoes the client's requestId for request A)
            firstNio.putInt(2013);    // opCode (OP_MSG)

            // Release back to pool — simulates MessageHeaderCallback.finally { result.release() }
            ByteBuffer underlyingBuffer = firstBuf.asNIO();
            firstBuf.release();

            // Acquire a new buffer from the pool — due to LIFO, we get the same physical buffer
            ByteBuf secondBuf = pool.getBuffer(MESSAGE_HEADER_LENGTH);
            ByteBuffer secondNio = secondBuf.asNIO();

            // Verify it IS the same underlying buffer (LIFO pool semantics)
            assertSame(underlyingBuffer, secondNio,
                    "Pool should return the same physical ByteBuffer (LIFO reuse)");

            // Verify the stale data is still present — clear() only resets position/limit
            secondNio.order(ByteOrder.LITTLE_ENDIAN);
            assertEquals(100, secondNio.getInt(0), "Stale messageLength persists");
            assertEquals(7777, secondNio.getInt(4), "Stale requestId persists");
            assertEquals(1163582, secondNio.getInt(8), "Stale responseTo persists");
            assertEquals(2013, secondNio.getInt(12), "Stale opCode persists");

            secondBuf.release();
        } finally {
            pool.disablePruning();
        }
    }

    /**
     * Demonstrates the exact sequence that would produce the reported error:
     *
     * <ol>
     *   <li>Request A (requestId=1163582) is sent and its response is read into buffer X</li>
     *   <li>Buffer X is released to the pool (header data remains in memory)</li>
     *   <li>Request B (requestId=1163586) is sent on the same connection</li>
     *   <li>A new readAsync for request B's response acquires buffer X from the pool</li>
     *   <li>If NIO2 does NOT overwrite the buffer (race condition), MessageHeader parses
     *       the stale header and finds responseTo=1163582, but expects 1163586 → MISMATCH</li>
     * </ol>
     *
     * <p>This test simulates steps 1-4 and shows that at step 5, the stale responseTo from
     * request A is what MessageHeader would see if NIO2 failed to write new data.</p>
     */
    @Test
    void staleResponseToInReusedBufferMatchesReportedErrorPattern() {
        PowerOfTwoBufferPool pool = new PowerOfTwoBufferPool(10);
        try {
            int requestIdA = 1163582; // The request whose response was previously in this buffer
            int requestIdB = 1163586; // The current request expecting its own response

            // Step 1-2: Request A's response header was read into this buffer, then released
            ByteBuf bufferUsedByRequestA = pool.getBuffer(MESSAGE_HEADER_LENGTH);
            ByteBuffer nio = bufferUsedByRequestA.asNIO();
            nio.order(ByteOrder.LITTLE_ENDIAN);
            nio.putInt(85);          // messageLength (realistic small response)
            nio.putInt(50001);       // requestId (server's response message ID)
            nio.putInt(requestIdA);  // responseTo = 1163582 (reply to request A)
            nio.putInt(2013);        // opCode = OP_MSG
            bufferUsedByRequestA.release();

            // Step 3-4: Request B acquires a buffer for its response read
            ByteBuf bufferForRequestB = pool.getBuffer(MESSAGE_HEADER_LENGTH);
            ByteBuffer nioForB = bufferForRequestB.asNIO();
            nioForB.order(ByteOrder.LITTLE_ENDIAN);

            // Step 5: Parse what's in the buffer WITHOUT NIO2 writing new data
            // (simulates the race where completion fires before DMA write is visible)
            MessageHeader staleHeader = new MessageHeader(bufferForRequestB, Integer.MAX_VALUE);

            // This is exactly the error the user sees:
            // "The responseTo (1163582) does not match the requestId (1163586)"
            assertEquals(requestIdA, staleHeader.getResponseTo(),
                    "Without NIO2 writing new data, MessageHeader sees stale responseTo from request A");

            // In normal operation, NIO2 would have overwritten the buffer with request B's response
            // (responseTo=1163586). The bug occurs when this write is not visible to the parsing thread.

            bufferForRequestB.release();
        } finally {
            pool.disablePruning();
        }
    }

    /**
     * Shows that the buffer size for message headers (16 bytes) maps to a specific pool bucket,
     * making buffer reuse between consecutive header reads highly likely.
     *
     * <p>All message header reads request exactly 16 bytes, which rounds to pool bucket size 16.
     * With LIFO semantics, consecutive header reads on the same or different connections will
     * almost always get the same physical buffer — maximizing exposure to stale data.</p>
     */
    @Test
    void headerBufferSizeAlwaysHitsSamePoolBucket() {
        PowerOfTwoBufferPool pool = new PowerOfTwoBufferPool(10);
        try {
            // All header reads request MESSAGE_HEADER_LENGTH (16 bytes)
            // 16 is already a power of two, so it maps directly to one pool bucket
            ByteBuf buf1 = pool.getBuffer(MESSAGE_HEADER_LENGTH);
            ByteBuffer underlying1 = buf1.asNIO();
            buf1.release();

            ByteBuf buf2 = pool.getBuffer(MESSAGE_HEADER_LENGTH);
            ByteBuffer underlying2 = buf2.asNIO();
            buf2.release();

            ByteBuf buf3 = pool.getBuffer(MESSAGE_HEADER_LENGTH);
            ByteBuffer underlying3 = buf3.asNIO();
            buf3.release();

            // All three acquisitions return the exact same buffer object
            assertSame(underlying1, underlying2, "Second header read reuses same buffer");
            assertSame(underlying2, underlying3, "Third header read reuses same buffer");
        } finally {
            pool.disablePruning();
        }
    }
}
