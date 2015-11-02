/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.async.client.gridfs

import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback

import java.nio.ByteBuffer
import java.nio.channels.AsynchronousByteChannel
import java.nio.channels.CompletionHandler
import java.util.concurrent.Future

import static java.util.concurrent.TimeUnit.SECONDS

class GridFSTestHelper {

    static run(operation, ... args) {
        FutureResultCallback futureResultCallback = new FutureResultCallback()
        List opArgs = (args != null) ? args : []
        operation.call(*opArgs + futureResultCallback)
        futureResultCallback.get(60, SECONDS)
    }

    static AsyncInputStream getInputStream(final String content) {
        getInputStream(ByteBuffer.wrap(content.getBytes()))
    }

    static AsyncInputStream getInputStream(final ByteBuffer srcByteBuffer) {
        new AsyncInputStream() {
            @Override
            void read(final ByteBuffer dstByteBuffer, final SingleResultCallback<Integer> callback) {
                int transferAmount = Math.min(dstByteBuffer.remaining(), srcByteBuffer.remaining());
                if (transferAmount == 0 ) {
                    transferAmount = -1
                };
                if (transferAmount > 0) {
                    ByteBuffer temp = srcByteBuffer.duplicate();
                    temp.limit(temp.position() + transferAmount);
                    dstByteBuffer.put(temp);
                    srcByteBuffer.position(srcByteBuffer.position() + transferAmount);
                }

                callback.onResult(transferAmount, null);
            }

            @Override
            void close(final SingleResultCallback<Void> callback) {
                callback.onResult(null, null)
            }
        }
    }

    static AsyncOutputStream getOutputStream(final ByteBuffer dstByteBuffer) {
        new AsyncOutputStream() {
            @Override
            void write(final ByteBuffer srcByteBuffer, final SingleResultCallback<Integer> callback) {
                int amount = srcByteBuffer.remaining()
                dstByteBuffer.put(srcByteBuffer)
                callback.onResult(amount, null)
            }

            @Override
            void close(final SingleResultCallback<Void> callback) {
                callback.onResult(null, null)
            }
        }
    }

    static class TestAsynchronousByteChannel implements AsynchronousByteChannel, Closeable {
        private boolean closed;
        private final readBuffer = ByteBuffer.allocate(1024)
        private final writeBuffer = ByteBuffer.allocate(1024)

        TestAsynchronousByteChannel(final readBuffer) {
            this.readBuffer = readBuffer
        }

        ByteBuffer getReadBuffer() {
            readBuffer.flip()
        }

        ByteBuffer getWriteBuffer() {
            writeBuffer.flip()
        }

        @Override
        <A> void read(final ByteBuffer dst, final A attachment, final CompletionHandler<Integer, ? super A> handler) {
            int transferAmount = Math.min(dst.remaining(), readBuffer.remaining());
            if (transferAmount == 0 ) {
                transferAmount = -1
            }
            if (transferAmount > 0) {
                ByteBuffer temp = readBuffer.duplicate()
                temp.limit(temp.position() + transferAmount)
                dst.put(temp)
                readBuffer.position(readBuffer.position() + transferAmount)
            }
            handler.completed(transferAmount, attachment)
        }

        @Override
        Future<Integer> read(final ByteBuffer dst) {
            throw new UnsupportedOperationException('Not Supported')
        }

        @Override
        <A> void write(final ByteBuffer src, final A attachment, final CompletionHandler<Integer, ? super A> handler) {
            int transferAmount = Math.min(src.remaining(), writeBuffer.remaining());
            if (transferAmount == 0 ) {
                transferAmount = -1
            }
            if (transferAmount > 0) {
                ByteBuffer temp = src.duplicate()
                temp.limit(temp.position() + transferAmount)
                writeBuffer.put(temp)
            }
            handler.completed(transferAmount, attachment)
        }

        @Override
        Future<Integer> write(final ByteBuffer src) {
            throw new UnsupportedOperationException('Not Supported')
        }

        @Override
        void close() throws IOException {
            closed = true
        }

        @Override
        boolean isOpen() {
            !closed;
        }
    }
}
