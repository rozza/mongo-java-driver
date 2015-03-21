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

package com.mongodb.connection;

import com.mongodb.async.SingleResultCallback;
import org.bson.ByteBuf;

import java.util.List;

final class InternalStreamSendMessageAsync {

    private final List<ByteBuf> byteBuffers;
    private final int messageId;
    private final SingleResultCallback<Void> callback;

    InternalStreamSendMessageAsync(final List<ByteBuf> byteBuffers, final int messageId, final SingleResultCallback<Void> callback) {
        this.byteBuffers = byteBuffers;
        this.messageId = messageId;
        this.callback = callback;
    }

    public List<ByteBuf> getByteBuffers() {
        return byteBuffers;
    }

    public int getMessageId() {
        return messageId;
    }

    public SingleResultCallback<Void> getCallback() {
        return callback;
    }
}
