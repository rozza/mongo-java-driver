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

import com.mongodb.ServerAddress;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AsynchronousSocketChannelStreamFactoryFactoryTest {

    private final SocketSettings socketSettings = SocketSettings.builder().build();
    private final SslSettings sslSettings = SslSettings.builder().build();
    private final ServerAddress serverAddress = new ServerAddress();

    @Test
    void shouldCreateTheExpectedAsynchronousSocketChannelStream() {
        StreamFactory factory = new AsynchronousSocketChannelStreamFactoryFactory(new DefaultInetAddressResolver())
                .create(socketSettings, sslSettings);

        AsynchronousSocketChannelStream stream = (AsynchronousSocketChannelStream) factory.create(serverAddress);

        assertEquals(socketSettings, stream.getSettings());
        assertEquals(serverAddress, stream.getAddress());
    }
}
