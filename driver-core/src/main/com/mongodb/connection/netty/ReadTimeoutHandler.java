/*
 * Copyright 2015 MongoDB, Inc.
 * Copyright 2012 The Netty Project
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

package com.mongodb.connection.netty;

import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.ReadTimeoutException;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


/**
 * Raises a {@link ReadTimeoutException} if the time between a read being requested and the read completing is longer than the set timeout.
 */
final class ReadTimeoutHandler extends ChannelInboundHandlerAdapter {

    private static final long MIN_TIMEOUT_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    private final long timeoutNanos;

    private volatile ScheduledFuture<?> timeout;
    private volatile long lastReadTime;

    private enum State {
        Created,
        Active,
        Paused,
        Destroyed
    }

    private volatile State state = State.Created;

    private boolean closed;

    /**
     * Creates a new instance.
     *
     * @param timeout read timeout
     * @param unit the {@link TimeUnit} of {@code timeout}
     */
    public ReadTimeoutHandler(final long timeout, final TimeUnit unit) {
        if (unit == null) {
            throw new NullPointerException("unit");
        }

        if (timeout <= 0) {
            timeoutNanos = 0;
        } else {
            timeoutNanos = Math.max(unit.toNanos(timeout), MIN_TIMEOUT_NANOS);
        }
    }

    @Override
    public void handlerRemoved(final ChannelHandlerContext ctx) throws Exception {
        destroy();
        super.handlerRemoved(ctx);
    }

    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
        // Initialize early if channel is active already.
        if (ctx.channel().isActive()) {
            scheduleTimeout(ctx);
        }
        super.channelRegistered(ctx);
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        scheduleTimeout(ctx);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        destroy();
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        lastReadTime = System.nanoTime();
        scheduleTimeout(ctx);
        ctx.fireChannelRead(msg);
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception {
        removeTimeout();
        ctx.fireChannelReadComplete();
    }

    private void removeTimeout() {
        if (State.Active == state) {
            state = State.Paused;
            if (timeout != null) {
                timeout.cancel(false);
            }
        }
    }

    private void scheduleTimeout(final ChannelHandlerContext ctx) {
        // Avoid the case where destroy() is called before scheduling timeouts.
        // See: https://github.com/netty/netty/issues/143
        switch (state) {
            case Active:
                return;
            case Destroyed:
                return;
            default:
                break;
        }

        state = State.Active;

        lastReadTime = System.nanoTime();
        if (timeoutNanos > 0) {
            timeout = ctx.executor().schedule(new ReadTimeoutTask(ctx), timeoutNanos, TimeUnit.NANOSECONDS);
        }
    }

    private void destroy() {
        state = State.Destroyed;
        if (timeout != null) {
            timeout.cancel(false);
            timeout = null;
        }
    }

    /**
     * Is called when a read timeout was detected.
     */
     void readTimedOut(final ChannelHandlerContext ctx) throws Exception {
        if (!closed) {
            ctx.fireExceptionCaught(ReadTimeoutException.INSTANCE);
            ctx.close();
            closed = true;
        }
    }

    private final class ReadTimeoutTask implements Runnable {

        private final ChannelHandlerContext ctx;

        ReadTimeoutTask(final ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {
            if (!ctx.channel().isOpen()) {
                return;
            }

            long currentTime = System.nanoTime();
            long nextDelay = timeoutNanos - (currentTime - lastReadTime);
            if (nextDelay <= 0) {
                // Read timed out - set a new timeout and notify the callback.
                timeout = ctx.executor().schedule(this, timeoutNanos, TimeUnit.NANOSECONDS);
                try {
                    readTimedOut(ctx);
                } catch (Throwable t) {
                    ctx.fireExceptionCaught(t);
                }
            } else {
                // Read occurred before the timeout - set a new timeout with shorter delay.
                timeout = ctx.executor().schedule(this, nextDelay, TimeUnit.NANOSECONDS);
            }
        }
    }
}
