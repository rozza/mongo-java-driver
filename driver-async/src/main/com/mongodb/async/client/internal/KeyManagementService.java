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

package com.mongodb.async.client.internal;

import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.StreamFactory;
import com.mongodb.connection.TlsChannelStreamFactoryFactory;
import com.mongodb.crypt.capi.MongoKeyDecryptor;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;

import javax.net.ssl.SSLContext;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

class KeyManagementService {
    private final int port;
    private final StreamFactory streamFactory;

    KeyManagementService(final SSLContext sslContext, final int port, final int timeoutMillis) {
        this.port = port;
        this.streamFactory = new TlsChannelStreamFactoryFactory().create(SocketSettings.builder()
                        .connectTimeout(timeoutMillis, TimeUnit.MILLISECONDS)
                        .readTimeout(timeoutMillis, TimeUnit.MILLISECONDS)
                        .build(),
                SslSettings.builder().enabled(true).context(sslContext).build());
    }

    void decryptKey(final MongoKeyDecryptor keyDecryptor, final SingleResultCallback<Void> callback) {
        streamOpen(keyDecryptor, callback);
    }

    private void streamOpen(final MongoKeyDecryptor keyDecryptor, final SingleResultCallback<Void> callback) {
        final KeyManagementAsynchronousChannelStream stream =
                new KeyManagementAsynchronousChannelStream(streamFactory.create(new ServerAddress(keyDecryptor.getHostName(), port)));
        stream.openAsync(new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void aVoid) {
                streamWrite(stream, keyDecryptor, callback);
            }

            @Override
            public void failed(final Throwable t) {
                stream.close();
                callback.onResult(null, new MongoSocketOpenException("Exception opening connection to Key Management Service",
                        getServerAddress(keyDecryptor), t));
            }
        });
    }

    private void streamWrite(final KeyManagementAsynchronousChannelStream stream, final MongoKeyDecryptor keyDecryptor,
                             final SingleResultCallback<Void> callback) {
        List<ByteBuf> byteBufs = Collections.<ByteBuf>singletonList(new ByteBufNIO(keyDecryptor.getMessage()));
        stream.writeAsync(byteBufs, new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void aVoid) {
                streamRead(stream, keyDecryptor, callback);
            }

            @Override
            public void failed(final Throwable t) {
                stream.close();
                callback.onResult(null, new MongoSocketWriteException("Exception sending message to Key Management Service",
                        getServerAddress(keyDecryptor), t));
            }
        });
    }

    private void streamRead(final KeyManagementAsynchronousChannelStream stream, final MongoKeyDecryptor keyDecryptor,
                            final SingleResultCallback<Void> callback) {
        int bytesNeeded = keyDecryptor.bytesNeeded();
        if (bytesNeeded > 0) {
            stream.readAsync(bytesNeeded, new AsyncCompletionHandler<ByteBuf>() {
                @Override
                public void completed(final ByteBuf byteBuf) {
                    keyDecryptor.feed(byteBuf.asNIO());
                    streamRead(stream, keyDecryptor, callback);
                }

                @Override
                public void failed(final Throwable t) {
                    stream.close();
                    callback.onResult(null, new MongoSocketReadException("Exception receiving message from Key Management Service",
                            getServerAddress(keyDecryptor), t));
                }
            });
        } else {
            stream.close();
            callback.onResult(null, null);
        }
    }

    private ServerAddress getServerAddress(final MongoKeyDecryptor keyDecryptor) {
        return new ServerAddress(keyDecryptor.getHostName(), port);
    }
}
