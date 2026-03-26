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

import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.spi.dns.InetAddressResolver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ClusterFixture.getSslSettings;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class AsyncSocketChannelStreamSpecificationTest {

    @Tag("Slow")
    @Test
    void shouldSuccessfullyConnectWithWorkingIpAddressList() throws java.io.IOException {
        assumeTrue(!getSslSettings().isEnabled());

        SocketSettings socketSettings = SocketSettings.builder().connectTimeout(100, MILLISECONDS).build();
        SslSettings sslSettings = SslSettings.builder().build();

        InetAddressResolver inetAddressResolver = host -> {
            try {
                return Arrays.asList(
                        InetAddress.getByName("192.168.255.255"),
                        InetAddress.getByName("127.0.0.1"));
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        };

        AsynchronousSocketChannelStreamFactoryFactory factoryFactory =
                new AsynchronousSocketChannelStreamFactoryFactory(inetAddressResolver);
        StreamFactory factory = factoryFactory.create(socketSettings, sslSettings);
        Stream stream = factory.create(new ServerAddress("host1"));

        stream.open(OPERATION_CONTEXT);
        assertFalse(stream.isClosed());
    }

    @Tag("Slow")
    @Test
    void shouldFailToConnectWithNonWorkingIpAddressList() {
        assumeTrue(!getSslSettings().isEnabled());

        SocketSettings socketSettings = SocketSettings.builder().connectTimeout(100, MILLISECONDS).build();
        SslSettings sslSettings = SslSettings.builder().build();

        InetAddressResolver inetAddressResolver = host -> {
            try {
                return Arrays.asList(
                        InetAddress.getByName("192.168.255.255"),
                        InetAddress.getByName("1.2.3.4"));
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        };

        AsynchronousSocketChannelStreamFactoryFactory factoryFactory =
                new AsynchronousSocketChannelStreamFactoryFactory(inetAddressResolver);
        StreamFactory factory = factoryFactory.create(socketSettings, sslSettings);
        Stream stream = factory.create(new ServerAddress());

        assertThrows(MongoSocketOpenException.class, () -> stream.open(OPERATION_CONTEXT));
    }

    @Test
    void shouldFailAsyncCompletionHandlerIfNameResolutionFails() throws InterruptedException {
        assumeTrue(!getSslSettings().isEnabled());

        ServerAddress serverAddress = new ServerAddress();
        MongoSocketException exception = new MongoSocketException("Temporary failure in name resolution", serverAddress);

        InetAddressResolver inetAddressResolver = host -> {
            throw exception;
        };

        Stream stream = new AsynchronousSocketChannelStream(serverAddress, inetAddressResolver,
                SocketSettings.builder().connectTimeout(100, MILLISECONDS).build(),
                new PowerOfTwoBufferPool());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        stream.openAsync(OPERATION_CONTEXT, new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void result) {
            }

            @Override
            public void failed(final Throwable t) {
                errorRef.set(t);
                latch.countDown();
            }
        });

        latch.await();
        assertSame(exception, errorRef.get());
    }
}
