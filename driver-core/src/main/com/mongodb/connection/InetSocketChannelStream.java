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

package com.mongodb.connection;

import com.mongodb.MongoSocketOpenException;
import com.mongodb.ServerAddress;

import java.io.IOException;
import java.nio.channels.SocketChannel;

class InetSocketChannelStream extends SocketChannelStream {
    InetSocketChannelStream(final ServerAddress address, final SocketSettings settings, final SslSettings sslSettings,
                            final BufferProvider bufferProvider) {
        super(address, settings, sslSettings, bufferProvider);
    }

    @Override
    public void open() throws IOException {
        try {
            SocketChannel socketChannel = SocketChannel.open();
            SocketStreamHelper.initialize(socketChannel.socket(), getAddress(), getSettings(), getSslSettings());
            setSocketChannel(socketChannel);
        } catch (IOException e) {
            close();
            throw new MongoSocketOpenException("Exception opening socket", getAddress(), e);
        }
    }
}
