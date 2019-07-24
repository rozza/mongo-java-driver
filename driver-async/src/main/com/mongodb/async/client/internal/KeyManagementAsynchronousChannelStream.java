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

import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.BufferProvider;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.Stream;
import com.mongodb.internal.connection.AsynchronousChannelStream;
import com.mongodb.internal.connection.ExtendedAsynchronousByteChannel;
import org.bson.ByteBuf;

import java.io.IOException;
import java.nio.channels.CompletionHandler;
import java.nio.channels.InterruptedByTimeoutException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

class KeyManagementAsynchronousChannelStream implements Stream {
    private final AsynchronousChannelStream wrapped;

    KeyManagementAsynchronousChannelStream(final Stream wrapped) {
        this.wrapped = (AsynchronousChannelStream) wrapped;
    }

    public ServerAddress getServerAddress() {
        return wrapped.getServerAddress();
    }

    public SocketSettings getSettings() {
        return wrapped.getSettings();
    }

    public BufferProvider getBufferProvider() {
        return wrapped.getBufferProvider();
    }

    public ExtendedAsynchronousByteChannel getChannel() {
        return wrapped.getChannel();
    }

    @Override
    public void writeAsync(final List<ByteBuf> buffers, final AsyncCompletionHandler<Void> handler) {
        wrapped.writeAsync(buffers, handler);
    }

    @Override
    public void readAsync(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler) {
//        wrapped.readAsync(numBytes, handler);
        ByteBuf buffer = wrapped.getBuffer(numBytes);
        wrapped.getChannel().read(buffer.asNIO(), wrapped.getSettings().getReadTimeout(MILLISECONDS), MILLISECONDS, null,
                new BasicCompletionHandler(buffer, handler));
    }

    @Override
    public void open() throws IOException {
        wrapped.open();
    }

    @Override
    public void write(final List<ByteBuf> buffers) throws IOException {
        wrapped.write(buffers);
    }

    @Override
    public ByteBuf read(final int numBytes) throws IOException {
        return wrapped.read(numBytes);
    }

    @Override
    public ServerAddress getAddress() {
        return wrapped.getAddress();
    }

    @Override
    public void close() {
        wrapped.close();
    }

    @Override
    public boolean isClosed() {
        return wrapped.isClosed();
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return wrapped.getBuffer(size);
    }

    @Override
    public void openAsync(final AsyncCompletionHandler<Void> handler) {
        wrapped.openAsync(handler);
    }

    private final class BasicCompletionHandler implements CompletionHandler<Integer, Void> {
        private final AtomicReference<AsyncCompletionHandler<ByteBuf>> handlerReference;
        private final AtomicReference<ByteBuf> byteBufReference;

        private BasicCompletionHandler(final ByteBuf dst, final AsyncCompletionHandler<ByteBuf> handler) {
            this.handlerReference = new AtomicReference<AsyncCompletionHandler<ByteBuf>>(handler);
            this.byteBufReference = new AtomicReference<ByteBuf>(dst);
        }

        @Override
        public void completed(final Integer result, final Void attachment) {
            AsyncCompletionHandler<ByteBuf> localHandler = getHandlerAndClear();
            ByteBuf localByteBuf = byteBufReference.getAndSet(null);
            if (result == -1 || !localByteBuf.hasRemaining()) {
                localByteBuf.flip();
                localHandler.completed(localByteBuf);
            } else {
                wrapped.getChannel().read(localByteBuf.asNIO(), wrapped.getSettings().getReadTimeout(MILLISECONDS), MILLISECONDS, null,
                        new BasicCompletionHandler(localByteBuf, localHandler));
            }
        }

        @Override
        public void failed(final Throwable t, final Void attachment) {
            AsyncCompletionHandler<ByteBuf> localHandler = getHandlerAndClear();
            ByteBuf localByteBuf = byteBufReference.getAndSet(null);
            localByteBuf.release();
            if (t instanceof InterruptedByTimeoutException) {
                localHandler.failed(new MongoSocketReadTimeoutException("Timeout while receiving message", wrapped.getServerAddress(), t));
            } else {
                localHandler.failed(t);
            }
        }

        AsyncCompletionHandler<ByteBuf> getHandlerAndClear() {
            return handlerReference.getAndSet(null);
        }
    }
}

