/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFlushPromiseNotifier.FlushCheckpoint;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.nio.ByteBuffer;

/**
 * The default {@link ChannelPromise} implementation.  It is recommended to use {@link Channel#newPromise()} to create
 * a new {@link ChannelPromise} rather than calling the constructor explicitly.
 */
public class DefaultChannelPromise extends DefaultPromise<Void>
        implements ChannelPromise, FlushCheckpoint, ChannelOutboundBuffer.Entry {

    private final Channel channel;
    private long checkpoint;

    // Fields for ChannelOutboundBuffer.Entry
    private ChannelOutboundBuffer.Entry next;
    private int count;
    private ByteBuffer buffer;
    private ByteBuffer[] buffers;
    private int pendingSize;
    private long total;
    private long progress;
    private Object msg;

    /**
     * Creates a new instance.
     *
     * @param channel
     *        the {@link Channel} associated with this future
     */
    public DefaultChannelPromise(Channel channel) {
        this.channel = channel;
    }

    /**
     * Creates a new instance.
     *
     * @param channel
     *        the {@link Channel} associated with this future
     */
    public DefaultChannelPromise(Channel channel, EventExecutor executor) {
        super(executor);
        this.channel = channel;
    }

    @Override
    protected EventExecutor executor() {
        EventExecutor e = super.executor();
        if (e == null) {
            return channel().eventLoop();
        } else {
            return e;
        }
    }

    @Override
    public Channel channel() {
        return channel;
    }

    @Override
    public ChannelPromise setSuccess() {
        return setSuccess(null);
    }

    @Override
    public ChannelPromise setSuccess(Void result) {
        super.setSuccess(result);
        return this;
    }

    @Override
    public boolean trySuccess() {
        return trySuccess(null);
    }

    @Override
    public ChannelPromise setFailure(Throwable cause) {
        super.setFailure(cause);
        return this;
    }

    @Override
    public ChannelPromise addListener(GenericFutureListener<? extends Future<? super Void>> listener) {
        super.addListener(listener);
        return this;
    }

    @Override
    public ChannelPromise addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
        super.addListeners(listeners);
        return this;
    }

    @Override
    public ChannelPromise removeListener(GenericFutureListener<? extends Future<? super Void>> listener) {
        super.removeListener(listener);
        return this;
    }

    @Override
    public ChannelPromise removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
        super.removeListeners(listeners);
        return this;
    }

    @Override
    public ChannelPromise sync() throws InterruptedException {
        super.sync();
        return this;
    }

    @Override
    public ChannelPromise syncUninterruptibly() {
        super.syncUninterruptibly();
        return this;
    }

    @Override
    public ChannelPromise await() throws InterruptedException {
        super.await();
        return this;
    }

    @Override
    public ChannelPromise awaitUninterruptibly() {
        super.awaitUninterruptibly();
        return this;
    }

    @Override
    public long flushCheckpoint() {
        return checkpoint;
    }

    @Override
    public void flushCheckpoint(long checkpoint) {
        this.checkpoint = checkpoint;
    }

    @Override
    public ChannelPromise promise() {
        return this;
    }

    @Override
    protected void checkDeadLock() {
        if (channel().isRegistered()) {
            super.checkDeadLock();
        }
    }

    @Override
    public int pendingSize() {
        return pendingSize;
    }

    @Override
    public long total() {
        return total;
    }

    @Override
    public long progress(long bytes) {
        progress += bytes;
        return progress;
    }

    @Override
    public void clear() {
        buffers = null;
        buffer = null;
        msg = null;
        progress = 0;
        total = 0;
        pendingSize = 0;
        count = 0;
        next = null;
    }

    @Override
    public void init(Object msg, int pendingSize, long total) {
        this.msg = msg;
        this.pendingSize = pendingSize;
        this.total = total;
        buffer = null;
        buffers = null;
        count = 0;
    }

    @Override
    public void next(ChannelOutboundBuffer.Entry entry) {
        next = entry;
    }

    @Override
    public ChannelOutboundBuffer.Entry next() {
        return next;
    }

    @Override
    public ByteBuffer buffer() {
        initBuffers();
        return buffer;
    }

    @Override
    public ByteBuffer[] buffers() {
        initBuffers();
        return buffers;
    }

    @Override
    public int bufferCount() {
        initBuffers();
        return count;
    }

    @Override
    public Object msg() {
        return msg;
    }

    private void initBuffers() {
        if (count == 0 && msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            count = buf.nioBufferCount();
            if (count == 1) {
                buffer = buf.internalNioBuffer(buf.readerIndex(), buf.readableBytes());
            } else {
                buffers = buf.nioBuffers(buf.readerIndex(), buf.readableBytes());
            }
        }
    }
}
