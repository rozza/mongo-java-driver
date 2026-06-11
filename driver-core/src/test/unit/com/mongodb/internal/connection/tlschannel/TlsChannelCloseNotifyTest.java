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
package com.mongodb.internal.connection.tlschannel;

import com.mongodb.internal.connection.tlschannel.util.Util;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests the read semantics of the vendored TLS channel when application data and a close_notify
 * alert are delivered in the same network segment (separate TLS records), as happens when a KMS
 * server responds and immediately closes the connection (JAVA-5391 / JAVA-5411).
 *
 * <p>Mirrors the test coverage proposed upstream in
 * <a href="https://github.com/marianobarrios/tls-channel/pull/351">tls-channel PR 351</a>.
 */
final class TlsChannelCloseNotifyTest {

    private static final char[] KEYSTORE_PASSWORD = "changeit".toCharArray();

    private TlsChannel clientChannel;
    private TlsChannel serverChannel;
    private Wire serverToClient;

    @ParameterizedTest
    @ValueSource(strings = {"TLSv1.2", "TLSv1.3"})
    void shouldReportDataAndCloseNotifyFromOneSegmentInOneRead(final String protocol) throws Exception {
        setUpChannels(protocol);
        byte[] payload = randomBytes(300);
        serverChannel.write(ByteBuffer.wrap(payload));
        serverChannel.close();

        // all server bytes (data record + close_notify record) are now buffered on the wire,
        // simulating delivery in a single TCP segment; the destination is larger than the
        // payload so buffer size is not under test here
        ByteBuffer dest = ByteBuffer.allocate(1024);
        assertEquals(payload.length, clientChannel.read(dest));
        assertEquals(payload.length, dest.position());
        assertTrue(clientChannel.shutdownReceived(),
                "close_notify buffered behind the data should be detected in the same read");

        assertEquals(-1, clientChannel.read(dest));
        ((Buffer) dest).flip();
        byte[] received = new byte[dest.remaining()];
        dest.get(received);
        assertArrayEquals(payload, received);

        // the shutdown can be completed from the reader side without further reads
        assertTrue(clientChannel.shutdown());
        clientChannel.close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"TLSv1.2", "TLSv1.3"})
    void shouldReportCloseNotifyFromSeparateSegmentOnNextRead(final String protocol) throws Exception {
        setUpChannels(protocol);
        byte[] payload = randomBytes(300);
        serverChannel.write(ByteBuffer.wrap(payload));

        // only the data is buffered; the close arrives later, in its own segment
        ByteBuffer dest = ByteBuffer.allocate(1024);
        assertEquals(payload.length, clientChannel.read(dest));
        assertFalse(clientChannel.shutdownReceived());

        serverChannel.close();
        assertEquals(-1, clientChannel.read(dest));
        assertTrue(clientChannel.shutdownReceived());
        clientChannel.close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"TLSv1.2", "TLSv1.3"})
    void shouldCountAllBytesWhenResponseSpansMultipleRecords(final String protocol) throws Exception {
        setUpChannels(protocol);
        // large enough to require several TLS records (max ~16KB plaintext each)
        byte[] payload = randomBytes(50_000);
        serverChannel.write(ByteBuffer.wrap(payload));
        serverChannel.close();

        // the destination is larger than the payload so it never fills up mid-read; this
        // verifies byte counts are correct across records, not partial delivery
        assertReadsDeliverExactly(payload, ByteBuffer.allocate(64 * 1024));
    }

    @ParameterizedTest
    @ValueSource(strings = {"TLSv1.2", "TLSv1.3"})
    void shouldReturnPartialCountWhenDestinationFillsUpMidResponse(final String protocol) throws Exception {
        setUpChannels(protocol);
        byte[] payload = randomBytes(50_000);
        serverChannel.write(ByteBuffer.wrap(payload));
        serverChannel.close();

        // the destination fits the first TLS record but fills up while further buffered
        // records remain, so a partial count must be returned
        assertReadsDeliverExactly(payload, ByteBuffer.allocate(20_000));
    }

    @ParameterizedTest
    @ValueSource(strings = {"TLSv1.2", "TLSv1.3"})
    void shouldHandleDestinationSmallerThanTlsRecord(final String protocol) throws Exception {
        setUpChannels(protocol);
        byte[] payload = randomBytes(50_000);
        serverChannel.write(ByteBuffer.wrap(payload));
        serverChannel.close();

        // a destination smaller than a single TLS record forces overflow into the channel's
        // internal plaintext buffer
        assertReadsDeliverExactly(payload, ByteBuffer.allocate(1000));
    }

    @ParameterizedTest
    @ValueSource(strings = {"TLSv1.2", "TLSv1.3"})
    void shouldDeliverDataPrecedingCorruptRecord(final String protocol) throws Exception {
        // JDK 8's SSLEngine reports malformed records inconsistently across builds (older builds
        // throw IllegalArgumentException instead of SSLException); the modern semantics this test
        // asserts are only guaranteed on Java 9+
        assumeTrue(Util.getJavaMajorVersion() >= 9, "requires Java 9+ SSLEngine error reporting");
        setUpChannels(protocol);
        byte[] payload = randomBytes(300);
        serverChannel.write(ByteBuffer.wrap(payload));

        // append a corrupt TLS record in the same segment as the data: a complete
        // application-data record whose ciphertext cannot be decrypted
        byte[] garbage = new byte[37];
        new Random(13).nextBytes(garbage);
        ByteBuffer corruptRecord = ByteBuffer.allocate(5 + garbage.length);
        corruptRecord.put((byte) 23); // content type: application data
        corruptRecord.put((byte) 3).put((byte) 3); // legacy version TLS 1.2
        corruptRecord.putShort((short) garbage.length);
        corruptRecord.put(garbage);
        ((Buffer) corruptRecord).flip();
        serverToClient.add(corruptRecord);

        // the data preceding the corrupt record must still be delivered ...
        ByteBuffer dest = ByteBuffer.allocate(1024);
        assertEquals(payload.length, clientChannel.read(dest));
        assertEquals(payload.length, dest.position());

        // ... and the failure surfaces on the next read
        assertThrows(IOException.class, () -> clientChannel.read(dest));
    }

    private void assertReadsDeliverExactly(final byte[] payload, final ByteBuffer dest) throws IOException {
        ByteArrayOutputStream received = new ByteArrayOutputStream();
        while (true) {
            int positionBeforeRead = dest.position();
            int bytesRead = clientChannel.read(dest);
            if (bytesRead == -1) {
                break;
            }
            assertEquals(positionBeforeRead + bytesRead, dest.position(),
                    "read() must report exactly the number of bytes transferred into the destination");
            ((Buffer) dest).flip();
            byte[] chunk = new byte[dest.remaining()];
            dest.get(chunk);
            received.write(chunk, 0, chunk.length);
            ((Buffer) dest).clear();
        }
        assertArrayEquals(payload, received.toByteArray());
        assertTrue(clientChannel.shutdownReceived());
        clientChannel.close();
    }

    private void setUpChannels(final String protocol) throws Exception {
        // older JDK 8 builds do not support TLSv1.3 at all
        assumeTrue(Arrays.asList(SSLContext.getDefault().getSupportedSSLParameters().getProtocols()).contains(protocol),
                protocol + " is not supported by this JVM");
        SSLContext sslContext = createSslContext();
        Wire clientToServer = new Wire();
        serverToClient = new Wire();

        SSLEngine clientEngine = sslContext.createSSLEngine();
        clientEngine.setUseClientMode(true);
        clientEngine.setEnabledProtocols(new String[] {protocol});
        clientChannel = ClientTlsChannel.newBuilder(new WireChannel(serverToClient, clientToServer), clientEngine).build();
        serverChannel = ServerTlsChannel.newBuilder(new WireChannel(clientToServer, serverToClient), sslContext).build();

        pumpHandshake(clientChannel, serverChannel);
    }

    private static void pumpHandshake(final TlsChannel client, final TlsChannel server) throws IOException {
        boolean clientDone = false;
        boolean serverDone = false;
        int rounds = 0;
        while (!(clientDone && serverDone)) {
            assertTrue(rounds++ < 100, "handshake did not complete");
            // only NeedsReadException can occur: WireChannel accepts all writes, and tasks run
            // inline by default, so NeedsWriteException/NeedsTaskException are impossible
            if (!clientDone) {
                try {
                    client.handshake();
                    clientDone = true;
                } catch (NeedsReadException e) {
                    // waiting for server bytes
                }
            }
            if (!serverDone) {
                try {
                    server.handshake();
                    serverDone = true;
                } catch (NeedsReadException e) {
                    // waiting for client bytes
                }
            }
        }
    }

    private static SSLContext createSslContext() throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream inputStream = TlsChannelCloseNotifyTest.class.getResourceAsStream("/tlschannel/test-keystore.p12")) {
            keyStore.load(inputStream, KEYSTORE_PASSWORD);
        }
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, KEYSTORE_PASSWORD);

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("test", (X509Certificate) keyStore.getCertificate("test"));
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);
        return sslContext;
    }

    private static byte[] randomBytes(final int size) {
        byte[] bytes = new byte[size];
        new Random(42).nextBytes(bytes);
        return bytes;
    }

    /**
     * A one-directional, in-memory byte stream. Reads return 0 when no bytes are buffered
     * (non-blocking semantics) and -1 once closed and drained. Because all written bytes are
     * available at once, it deterministically simulates records arriving in a single segment.
     */
    private static final class Wire {
        private final Deque<ByteBuffer> chunks = new ArrayDeque<>();
        private boolean closed;

        void add(final ByteBuffer src) {
            byte[] copy = new byte[src.remaining()];
            src.get(copy);
            chunks.add(ByteBuffer.wrap(copy));
        }

        int drainTo(final ByteBuffer dst) {
            if (chunks.isEmpty()) {
                return closed ? -1 : 0;
            }
            int transferred = 0;
            while (dst.hasRemaining() && !chunks.isEmpty()) {
                ByteBuffer head = chunks.peek();
                int count = Math.min(dst.remaining(), head.remaining());
                int oldLimit = head.limit();
                ((Buffer) head).limit(head.position() + count);
                dst.put(head);
                ((Buffer) head).limit(oldLimit);
                transferred += count;
                if (!head.hasRemaining()) {
                    chunks.poll();
                }
            }
            return transferred;
        }
    }

    private static final class WireChannel implements ByteChannel {
        private final Wire in;
        private final Wire out;

        WireChannel(final Wire in, final Wire out) {
            this.in = in;
            this.out = out;
        }

        @Override
        public int read(final ByteBuffer dst) {
            return in.drainTo(dst);
        }

        @Override
        public int write(final ByteBuffer src) {
            int count = src.remaining();
            out.add(src);
            return count;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {
            out.closed = true;
        }
    }
}
