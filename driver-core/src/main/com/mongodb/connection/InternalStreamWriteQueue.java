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

import com.mongodb.MongoSocketClosedException;
import com.mongodb.ServerAddress;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;

import java.util.LinkedList;
import java.util.concurrent.Semaphore;

import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static java.lang.String.format;

final class InternalStreamWriteQueue {
    static final Logger LOGGER = Loggers.getLogger("connection");
    private final LinkedList<InternalStreamSendMessageAsync> queue = new LinkedList<InternalStreamSendMessageAsync>();
    private final Semaphore writingLock = new Semaphore(1);
    private final Semaphore queueLock = new Semaphore(1);

    private final ServerId serverId;
    private volatile ConnectionDescription description;

    public InternalStreamWriteQueue(final ServerId serverId, final ConnectionDescription description) {
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
                LOGGER.trace(format("Write queue%s empty ([%s] %s)", empty ? "" : " not", getId(), serverId));
            }
            return empty;
        } catch (InterruptedException e) {
            return true;
        } finally {
            queueLock.release();
        }
    }

    public void closeAll() {
        try {
            queueLock.acquire();
            while (!queue.isEmpty()) {
                informedClosed(queue.poll());
            }
        } catch (InterruptedException e) {
            // noop
        } finally {
            queueLock.release();
        }
    }

    public InternalStreamSendMessageAsync poll() {
        try {
            queueLock.acquire();
            return queue.poll();
        } catch (InterruptedException e) {
            return null;
        } finally {
            queueLock.release();
        }
    }

    public void add(final InternalStreamSendMessageAsync sendMessage) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Queuing send message: %s. ([%s] %s)", sendMessage.getMessageId(), getId(), serverId));
        }
        try {
            queueLock.acquire();
            queue.add(sendMessage);
        } catch (InterruptedException e) {
            //  noop
        } finally {
            queueLock.release();
        }
    }

    public void acquire() throws InterruptedException {
        try {
            queueLock.acquire();
            if (!queue.isEmpty()) {
                writingLock.acquire();
            }
        } finally {
            queueLock.release();
        }
    }

    public boolean tryAcquire() {
        try {
            queueLock.acquire();
            if (!queue.isEmpty()) {
                return writingLock.tryAcquire();
            }
            return false;
        } catch (InterruptedException e) {
            return false;
        } finally {
            queueLock.release();
        }
    }

    public void release() {
        writingLock.release();
    }

    private void informedClosed(final InternalStreamSendMessageAsync message) {
        if (message != null) {
            errorHandlingCallback(message.getCallback(), LOGGER).onResult(null,
                    new MongoSocketClosedException("Cannot write to a closed stream", getServerAddress()));
        }
    }

    private ConnectionId getId() {
        return description.getConnectionId();
    }

    private ServerAddress getServerAddress() {
        return description.getServerAddress();
    }

}

