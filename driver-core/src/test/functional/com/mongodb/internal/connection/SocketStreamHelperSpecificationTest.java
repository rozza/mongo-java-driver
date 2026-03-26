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

import com.mongodb.ClusterFixture;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import org.junit.jupiter.api.Test;

import javax.net.SocketFactory;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.Socket;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.ClusterFixture.createOperationContext;
import static com.mongodb.ClusterFixture.getPrimary;
import static com.mongodb.internal.connection.ServerAddressHelper.getSocketAddresses;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class SocketStreamHelperSpecificationTest {

    @Test
    void shouldConfigureSocketWithSettings() throws IOException {
        Socket socket = SocketFactory.getDefault().createSocket();
        try {
            SocketSettings socketSettings = SocketSettings.builder()
                    .readTimeout(10, SECONDS)
                    .build();

            OperationContext operationContext = createOperationContext(
                    TIMEOUT_SETTINGS.withReadTimeoutMS(socketSettings.getReadTimeout(MILLISECONDS)));

            SocketStreamHelper.initialize(operationContext, socket,
                    getSocketAddresses(getPrimary(), new DefaultInetAddressResolver()).get(0),
                    socketSettings, SslSettings.builder().build());

            assertTrue(socket.getTcpNoDelay());
            assertTrue(socket.getKeepAlive());
            assertEquals(socketSettings.getReadTimeout(MILLISECONDS), socket.getSoTimeout());
        } finally {
            socket.close();
        }
    }

    @Test
    void shouldThrowMongoOperationTimeoutExceptionDuringInitializationWhenTimeoutMSExpires() throws IOException {
        Socket socket = SocketFactory.getDefault().createSocket();
        try {
            assertThrows(MongoOperationTimeoutException.class, () ->
                    SocketStreamHelper.initialize(
                            OPERATION_CONTEXT.withTimeoutContext(new TimeoutContext(
                                    new TimeoutSettings(1L, 100L, 100L, 1L, 100L))),
                            socket,
                            getSocketAddresses(getPrimary(), new DefaultInetAddressResolver()).get(0),
                            SocketSettings.builder().build(), SslSettings.builder().build()));
        } finally {
            socket.close();
        }
    }

    @Test
    void shouldConnectSocket() throws IOException {
        Socket socket = SocketFactory.getDefault().createSocket();
        try {
            SocketStreamHelper.initialize(OPERATION_CONTEXT, socket,
                    getSocketAddresses(getPrimary(), new DefaultInetAddressResolver()).get(0),
                    SocketSettings.builder().build(), SslSettings.builder().build());

            assertTrue(socket.isConnected());
        } finally {
            socket.close();
        }
    }

    @Test
    void shouldEnableHostNameVerificationIfSocketIsSSLSocket() throws IOException {
        assumeTrue(ClusterFixture.getSslSettings().isEnabled());

        // Test with SSL enabled, invalidHostNameAllowed = false
        SSLSocket socket1 = (SSLSocket) SSLSocketFactory.getDefault().createSocket();
        try {
            SslSettings sslEnabled = SslSettings.builder().enabled(true).build();
            SocketStreamHelper.initialize(OPERATION_CONTEXT, socket1,
                    getSocketAddresses(getPrimary(), new DefaultInetAddressResolver()).get(0),
                    SocketSettings.builder().build(), sslEnabled);
            assertEquals("HTTPS", socket1.getSSLParameters().getEndpointIdentificationAlgorithm());
        } finally {
            socket1.close();
        }

        // Test with SSL enabled, invalidHostNameAllowed = true
        SSLSocket socket2 = (SSLSocket) SSLSocketFactory.getDefault().createSocket();
        try {
            SslSettings sslInvalidHostAllowed = SslSettings.builder().enabled(true).invalidHostNameAllowed(true).build();
            SocketStreamHelper.initialize(OPERATION_CONTEXT, socket2,
                    getSocketAddresses(getPrimary(), new DefaultInetAddressResolver()).get(0),
                    SocketSettings.builder().build(), sslInvalidHostAllowed);
            assertEquals(null, socket2.getSSLParameters().getEndpointIdentificationAlgorithm());
        } finally {
            socket2.close();
        }
    }

    @Test
    void shouldEnableSNIIfSocketIsSSLSocket() throws IOException {
        assumeTrue(ClusterFixture.getSslSettings().isEnabled());

        SSLSocket socket = (SSLSocket) SSLSocketFactory.getDefault().createSocket();
        try {
            SslSettings sslEnabled = SslSettings.builder().enabled(true).build();
            SocketStreamHelper.initialize(OPERATION_CONTEXT, socket,
                    getSocketAddresses(getPrimary(), new DefaultInetAddressResolver()).get(0),
                    SocketSettings.builder().build(), sslEnabled);

            assertEquals(singletonList(new SNIHostName(getPrimary().getHost())),
                    socket.getSSLParameters().getServerNames());
        } finally {
            socket.close();
        }
    }

    @Test
    void shouldThrowMongoInternalExceptionIfSslEnabledAndSocketIsNotSSLSocket() throws IOException {
        Socket socket = SocketFactory.getDefault().createSocket();
        try {
            assertThrows(MongoInternalException.class, () ->
                    SocketStreamHelper.initialize(OPERATION_CONTEXT, socket,
                            getSocketAddresses(getPrimary(), new DefaultInetAddressResolver()).get(0),
                            SocketSettings.builder().build(), SslSettings.builder().enabled(true).build()));
        } finally {
            socket.close();
        }
    }
}
