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

import com.mongodb.Block;
import com.mongodb.Function;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

import static java.lang.String.format;

final class InternalStreamReadQueue {
    static final Logger LOGGER = Loggers.getLogger("connection");

    private final ServerId serverId;
    private final ConcurrentHashMap<Integer, SingleResultCallback<ResponseBuffers>> queue =
            new ConcurrentHashMap<Integer, SingleResultCallback<ResponseBuffers>>();
    private final ConcurrentMap<Integer, InternalStreamResponse> responses = new ConcurrentHashMap<Integer, InternalStreamResponse>();

    private final Semaphore readingLock = new Semaphore(1);
    private final Semaphore queueLock = new Semaphore(1);

    private volatile ConnectionDescription description;

    public InternalStreamReadQueue(final ServerId serverId, final ConnectionDescription description) {
        this.serverId = serverId;
        this.description = description;
    }

    public void setDescription(final ConnectionDescription description) {
        this.description = description;
    }

    public boolean isEmpty() {
        try {
            queueLock.acquire();
            boolean empty = queue.isEmpty();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Read queue%s empty ([%s] %s)", empty ? "" : " not", getId(), serverId));
            }
            return empty;
        } catch (InterruptedException e) {
            return true;
        } finally {
            queueLock.release();
        }
    }

    public SingleResultCallback<ResponseBuffers> remove(final int messageId) {
        try {
            queueLock.acquire();
            return queue.remove(messageId);
        } catch (InterruptedException e) {
            return null;
        } finally {
            queueLock.release();
        }
    }

    public void put(final int messageId, final SingleResultCallback<ResponseBuffers> callback) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Queuing read message: %s. ([%s] %s)", messageId, getId(), serverId));
        }
        try {
            queueLock.acquire();
            queue.put(messageId, callback);
        } catch (InterruptedException e) {
            callback.onResult(null, e);
        } finally {
            queueLock.release();
        }
    }

    public void forEach(final Block<Map.Entry<Integer, SingleResultCallback<ResponseBuffers>>> block) {
        try {
            queueLock.acquire();
        } catch (InterruptedException e) {
            // noop
            return;
        }

        try {
            Iterator<Map.Entry<Integer, SingleResultCallback<ResponseBuffers>>> it = queue.entrySet().iterator();
            while (it.hasNext()) {
                block.apply(it.next());
                it.remove();
            }
        } finally {
            queueLock.release();
        }
    }

    public final void acquire(final Block<Throwable> block) {
        try {
            queueLock.acquire();
        } catch (InterruptedException e) {
            block.apply(e);
            return;
        }

        try {
            readingLock.acquire();
        } catch (InterruptedException e) {
            block.apply(e);
            queueLock.release();
        }

        try {
            block.apply(null);
        } finally {
            readingLock.release();
            queueLock.release();
        }
    }

    public boolean tryAcquire() {
        try {
            queueLock.acquire();
            if (!queue.isEmpty()) {
                boolean hasLock = readingLock.tryAcquire();
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(format("Reading lock: %s ([%s] %s)", hasLock ? "acquired" : "unavailable", getId(), serverId));
                }
                return hasLock;
            }
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Reading lock: queue empty ([%s] %s)", getId(), serverId));
            }
            return false;
        } catch (InterruptedException e) {
            return false;
        } finally {
            queueLock.release();
        }
    }

    public void release() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Reading lock: released ([%s] %s)", getId(), serverId));
        }
        readingLock.release();
    }

    public void putResponse(final int messageId, final InternalStreamResponse response) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Message added to pending results: %s. ([%s] %s)", messageId, getId(), serverId));
        }
        responses.put(messageId, response);
    }

    public InternalStreamResponse removeResponse(final int messageId) {
        return responses.remove(messageId);
    }

    public boolean containsKeyResponse(final int messageId) {
        return responses.containsKey(messageId);
    }

    public void forEachResponse(final Function<Map.Entry<Integer, InternalStreamResponse>, Boolean> function) {
        Iterator<Map.Entry<Integer, InternalStreamResponse>> it = responses.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, InternalStreamResponse> entry = it.next();
            int messageId = entry.getKey();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Processing read message: %s. ([%s] %s)", messageId, getId(), serverId));
            }
            if (function.apply(entry)) {
                it.remove();
            }
        }
    }

    private ConnectionId getId() {
        return description.getConnectionId();
    }
}
