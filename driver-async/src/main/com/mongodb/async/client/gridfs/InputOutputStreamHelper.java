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

package com.mongodb.async.client.gridfs;

import com.mongodb.MongoGridFSException;
import com.mongodb.async.SingleResultCallback;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.bson.assertions.Assertions.notNull;

/**
 * A helper class to convert to {@link InputStream} or {@link OutputStream} instances into {@link AsyncInputStream}
 * or {@link AsyncOutputStream} instances.
 *
 * @since 3.2
 */
public final class InputOutputStreamHelper {

    /**
     * Converts a {@link InputStream} into a {@link AsyncInputStream}
     *
     * @param inputStream the InputStream
     * @return the AsyncInputStream
     */
    public static AsyncInputStream toAsyncInputStream(final InputStream inputStream) {
        notNull("inputStream", inputStream);
        return new AsyncInputStream() {
            @Override
            public void read(final ByteBuffer dst, final SingleResultCallback<Integer> callback) {
                notNull("dst", dst);
                notNull("callback", callback);
                if (!dst.hasRemaining()) {
                    callback.onResult(-1, null);
                    return;
                }

                int maxAmount = dst.remaining();
                byte[] bytes = new byte[maxAmount];
                int amount;
                try {
                    amount = inputStream.read(bytes);
                } catch (Exception e) {
                    callback.onResult(null, new MongoGridFSException("Error reading from input stream", e));
                    return;
                }

                if (amount > 0) {
                    if (amount < maxAmount) {
                        byte[] data = new byte[amount];
                        System.arraycopy(bytes, 0, data, 0, amount);
                        dst.put(data);
                    } else {
                        dst.put(bytes);
                    }
                }
                callback.onResult(amount, null);
            }

            @Override
            public void close(final SingleResultCallback<Void> callback) {
                try {
                    inputStream.close();
                    callback.onResult(null, null);
                } catch (Exception e) {
                    callback.onResult(null, e);
                }
            }
        };
    }

    /**
     * Converts a {@link OutputStream} into a {@link AsyncOutputStream}
     *
     * @param outputStream the OutputStream
     * @return the AsyncOutputStream
     */
    public static AsyncOutputStream toAsyncOutputStream(final OutputStream outputStream) {
        notNull("outputStream", outputStream);
        return new AsyncOutputStream() {
            @Override
            public void write(final ByteBuffer src, final SingleResultCallback<Integer> callback) {
                notNull("src", src);
                notNull("callback", callback);
                if (!src.hasRemaining()) {
                    callback.onResult(-1, null);
                    return;
                }

                int amount = src.remaining();
                byte[] bytes = new byte[amount];
                try {
                    src.get(bytes);
                    outputStream.write(bytes);
                    callback.onResult(amount, null);
                } catch (Exception e) {
                    callback.onResult(null, new MongoGridFSException("Error reading from input stream", e));
                    return;
                }
            }

            @Override
            public void close(final SingleResultCallback<Void> callback) {
                try {
                    outputStream.close();
                    callback.onResult(null, null);
                } catch (IOException e) {
                    callback.onResult(null, e);
                }
            }
        };
    }

    private InputOutputStreamHelper() {
    }
}
