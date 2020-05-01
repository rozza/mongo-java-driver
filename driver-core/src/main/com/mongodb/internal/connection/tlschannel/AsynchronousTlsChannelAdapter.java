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

import com.mongodb.internal.connection.ExtendedAsynchronousByteChannel;
import com.mongodb.internal.connection.tlschannel.async.AsynchronousTlsChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class AsynchronousTlsChannelAdapter implements ExtendedAsynchronousByteChannel {
    private final AsynchronousTlsChannel wrapped;

    public static ExtendedAsynchronousByteChannel adapt(final AsynchronousTlsChannel asynchronousTlsChannel) {
        return new AsynchronousTlsChannelAdapter(asynchronousTlsChannel);
    }

    AsynchronousTlsChannelAdapter(AsynchronousTlsChannel wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public <A> void read(final ByteBuffer dst, final A attach, final CompletionHandler<Integer, ? super A> handler) {
        wrapped.read(dst, attach, handler);
    }

    @Override
    public <A> void read(final ByteBuffer dst, final long timeout, final TimeUnit unit, final A attach,
                         final CompletionHandler<Integer, ? super A> handler) {
        wrapped.read(dst, timeout, unit, attach, handler);
    }

    @Override
    public <A> void read(final ByteBuffer[] dsts, final int offset, final int length, final long timeout, final TimeUnit unit,
                         final A attach, final CompletionHandler<Long, ? super A> handler) {
        wrapped.read(dsts, offset, length, timeout, unit, attach, handler);
    }

    @Override
    public Future<Integer> read(final ByteBuffer dst) {
        return wrapped.read(dst);
    }

    @Override
    public <A> void write(final ByteBuffer src, final A attach, final CompletionHandler<Integer, ? super A> handler) {
        wrapped.write(src, attach, handler);
    }

    @Override
    public <A> void write(final ByteBuffer src, final long timeout, final TimeUnit unit, final A attach,
                          final CompletionHandler<Integer, ? super A> handler) {
        wrapped.write(src, timeout, unit, attach, handler);
    }

    @Override
    public <A> void write(final ByteBuffer[] srcs, final int offset, final int length, final long timeout, final TimeUnit unit,
                          final A attach, final CompletionHandler<Long, ? super A> handler) {
        wrapped.write(srcs, offset, length, timeout, unit, attach, handler);
    }

    @Override
    public Future<Integer> write(final ByteBuffer src) {
        return wrapped.write(src);
    }

    @Override
    public boolean isOpen() {
        return wrapped.isOpen();
    }

    @Override
    public void close() throws IOException {
        wrapped.close();
    }
}
